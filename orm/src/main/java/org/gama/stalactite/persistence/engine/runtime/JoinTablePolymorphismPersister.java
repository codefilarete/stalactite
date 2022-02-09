package org.codefilarete.stalactite.persistence.engine.runtime;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.exception.NotImplementedException;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.stalactite.persistence.engine.DeleteExecutor;
import org.codefilarete.stalactite.persistence.engine.EntityPersister;
import org.codefilarete.stalactite.persistence.engine.ExecutableQuery;
import org.codefilarete.stalactite.persistence.engine.InsertExecutor;
import org.codefilarete.stalactite.persistence.engine.SelectExecutor;
import org.codefilarete.stalactite.persistence.engine.UpdateExecutor;
import org.codefilarete.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.codefilarete.stalactite.persistence.engine.listening.DeleteListener;
import org.codefilarete.stalactite.persistence.engine.listening.InsertListener;
import org.codefilarete.stalactite.persistence.engine.listening.PersisterListener;
import org.codefilarete.stalactite.persistence.engine.listening.PersisterListenerCollection;
import org.codefilarete.stalactite.persistence.engine.listening.SelectListener;
import org.codefilarete.stalactite.persistence.engine.listening.UpdateListener;
import org.codefilarete.stalactite.persistence.engine.runtime.SimpleRelationalEntityPersister.CriteriaProvider;
import org.codefilarete.stalactite.persistence.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.persistence.mapping.ColumnedRow;
import org.codefilarete.stalactite.persistence.mapping.EntityMappingStrategy;
import org.codefilarete.stalactite.persistence.mapping.IdMappingStrategy;
import org.codefilarete.stalactite.persistence.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.persistence.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.persistence.query.RelationalEntityCriteria;
import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.codefilarete.stalactite.query.model.AbstractRelationalOperator;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * Class that wraps some other persisters and transfers its invokations to them.
 * Used for polymorphism to dispatch method calls to sub-entities persisters.
 * 
 * @author Guillaume Mary
 */
public class JoinTablePolymorphismPersister<C, I> implements EntityConfiguredJoinedTablesPersister<C, I>, PolymorphicPersister<C> {
	
	/**
	 * Current storage of entities to be loaded during the 2-Phases load algorithm.
	 * Tracked as a {@link Queue} to solve resource cleaning issue in case of recursive polymorphism. This may be solved by avoiding to have a static field
	 */
	// TODO : try a non-static field to remove Queue usage which impacts code complexity
	private static final ThreadLocal<Queue<Set<RelationIds<Object /* E */, Object /* target */, Object /* target identifier */ >>>> CURRENT_2PHASES_LOAD_CONTEXT = new ThreadLocal<>();
	
	private final Map<Class<? extends C>, EntityConfiguredJoinedTablesPersister<C, I>> subEntitiesPersisters;
	/** The wrapper around sub entities loaders, for 2-phases load */
	private final JoinTablePolymorphismSelectExecutor<C, I, ?> mainSelectExecutor;
	private final Class<C> parentClass;
	private final Map<Class<? extends C>, IdMappingStrategy<C, I>> subclassIdMappingStrategies;
	private final Map<Class<? extends C>, Table> tablePerSubEntityType;
	private final Column<?, I> mainTablePrimaryKey;
	private final EntityCriteriaSupport<C> criteriaSupport;
	private final JoinTablePolymorphismEntitySelectExecutor<C, I, ?> entitySelectExecutor;
	private final EntityConfiguredJoinedTablesPersister<C, I> mainPersister;
	
	public JoinTablePolymorphismPersister(EntityConfiguredJoinedTablesPersister<C, I> mainPersister,
										  Map<Class<? extends C>, EntityConfiguredJoinedTablesPersister<C, I>> subEntitiesPersisters,
										  ConnectionProvider connectionProvider,
										  Dialect dialect) {
		this.mainPersister = mainPersister;
		this.parentClass = this.mainPersister.getClassToPersist();
		this.mainTablePrimaryKey = (Column) Iterables.first(mainPersister.getMappingStrategy().getTargetTable().getPrimaryKey().getColumns());
		
		this.subEntitiesPersisters = subEntitiesPersisters;
		Set<Entry<Class<? extends C>, EntityConfiguredJoinedTablesPersister<C, I>>> subPersisterPerSubEntityType = subEntitiesPersisters.entrySet();
		Map<Class<? extends C>, SelectExecutor<C, I>> subclassSelectExecutors = Iterables.map(subPersisterPerSubEntityType, Entry::getKey,
				Entry::getValue);
		this.subclassIdMappingStrategies = Iterables.map(subPersisterPerSubEntityType, Entry::getKey, e -> e.getValue().getMappingStrategy().getIdMappingStrategy());
		
		// sub entities persisters will be used to select sub entities but at this point they lacks subgraph loading, so we add it (from their parent)
		subEntitiesPersisters.forEach((type, persister) ->
				// NB: we can't use copyRootJoinsTo(..) because persister is composed of one join to parent table (the one of mainPersister),
				// since there's no manner to find it cleanly we have to use get(0), dirty thing ...
				mainPersister.getEntityJoinTree().projectTo(persister.getEntityJoinTree().getRoot().getJoins().get(0))
		);
		
		this.tablePerSubEntityType = Iterables.map(this.subEntitiesPersisters.entrySet(),
				Entry::getKey,
				entry -> entry.getValue().getMappingStrategy().getTargetTable());
		this.mainSelectExecutor = new JoinTablePolymorphismSelectExecutor<>(
				tablePerSubEntityType,
				subclassSelectExecutors,
				mainPersister.getMappingStrategy().getTargetTable(), connectionProvider, dialect);
		
		this.entitySelectExecutor = new JoinTablePolymorphismEntitySelectExecutor(subEntitiesPersisters, subEntitiesPersisters,
				mainPersister.getMappingStrategy().getTargetTable(),
				mainPersister.getEntityJoinTree(), connectionProvider, dialect);
		
		this.criteriaSupport = new EntityCriteriaSupport<>(mainPersister.getMappingStrategy());
	}
	
	@Override
	public Set<Class<? extends C>> getSupportedEntityTypes() {
		Set<Class<? extends C>> result = new HashSet<>();
		this.subEntitiesPersisters.forEach((c, p) -> {
			if (p instanceof PolymorphicPersister) {
				result.addAll((Collection) ((PolymorphicPersister<?>) p).getSupportedEntityTypes());
			} else if (p instanceof PersisterWrapper && ((PersisterWrapper<C, I>) p).getDeepestSurrogate() instanceof PolymorphicPersister) {
				result.addAll(((PolymorphicPersister) ((PersisterWrapper) p).getDeepestSurrogate()).getSupportedEntityTypes());
			} else {
				result.add(c);
			}
		});
		return result;
	}
	
	@Override
	public Collection<Table> giveImpliedTables() {
		// Implied tables are those of sub entities.
		// Note that doing this lately (not in constructor) garanties that it is uptodate because sub entities may have relations which are
		// configured out of constructor by caller
		List<Table> subTables = subEntitiesPersisters.values().stream().flatMap(p -> p.giveImpliedTables().stream()).collect(Collectors.toList());
		return Collections.cat(mainPersister.giveImpliedTables(), subTables);
	}
	
	@Override
	public PersisterListenerCollection<C, I> getPersisterListener() {
		return mainPersister.getPersisterListener();
	}
	
	@Override
	public void insert(Iterable<? extends C> entities) {
		mainPersister.insert(entities);
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister((Iterable) entities);
		entitiesPerType.forEach(InsertExecutor::insert);
	}
	
	@Override
	public void updateById(Iterable<C> entities) {
		mainPersister.updateById(entities);
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(UpdateExecutor::updateById);
	}
	
	@Override
	public void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		mainPersister.update(differencesIterable, allColumnsStatement);
		
		Map<UpdateExecutor<C>, Set<Duo<C, C>>> entitiesPerType = new HashMap<>();
		differencesIterable.forEach(payload ->
				this.subEntitiesPersisters.values().forEach(persister -> {
					C entity = Objects.preventNull(payload.getLeft(), payload.getRight());
					if (persister.getClassToPersist().isInstance(entity)) {
						entitiesPerType.computeIfAbsent(persister, p -> new HashSet<>()).add(payload);
					}
				})
		);
		
		entitiesPerType.forEach((updateExecutor, adhocEntities) -> updateExecutor.update(adhocEntities, allColumnsStatement));
	}
	
	@Override
	public List<C> select(Iterable<I> ids) {
		subEntitiesPersisters.forEach((subclass, subEntityPersister) ->
				subEntityPersister.getPersisterListener().getSelectListener().beforeSelect(ids));
		
		List<C> result = mainSelectExecutor.select(ids);
		
		// Then we call sub entities afterSelect listeners else they are not invoked. Done in particular for relation on sub entities that have
		// an already-assigned identifier which requires to mark entities as persisted (to prevent them from trying to be inserted wherease they 
		// already are)
		Map<Class, Set<C>> entitiesPerType = new HashMap<>();
		for (C entity : result) {
			entitiesPerType.computeIfAbsent(entity.getClass(), cClass -> new HashSet<>()).add(entity);
		}
		// We invoke persisters (not SelectExecutor to trigger event listeners which is necessary for cascade)
		subEntitiesPersisters.forEach((subclass, subEntityPersister) -> {
			Set<C> selectedEntities = entitiesPerType.get(subclass);
			if (selectedEntities != null) {
				subEntityPersister.getPersisterListener().getSelectListener().afterSelect(selectedEntities);
			}
		});
		return result;
	}
	
	@Override
	public void delete(Iterable<C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(DeleteExecutor::delete);
		mainPersister.delete(entities);
	}
	
	@Override
	public void deleteById(Iterable<C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(DeleteExecutor::deleteById);
		mainPersister.deleteById(entities);
	}
	
	@Override
	public void persist(Iterable<? extends C> entities) {
		// This class doesn't need to implement this method because it is better handled by wrapper, especially in triggering event
		throw new NotImplementedException("This class doesn't need to implement this method because it is handled by wrapper");
	}
	
	private Map<EntityPersister<C, I>, Set<C>> computeEntitiesPerPersister(Iterable<C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = new KeepOrderMap<>();
		entities.forEach(entity ->
				this.subEntitiesPersisters.values().forEach(persister -> {
					if (persister.getClassToPersist().isInstance(entity)) {
						entitiesPerType.computeIfAbsent(persister, p -> new HashSet<>()).add(entity);
					}
				})
		);
		return entitiesPerType;
	}
	
	@Override
	public <O> RelationalExecutableEntityQuery<C> selectWhere(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator) {
		EntityCriteriaSupport<C> localCriteriaSupport = newWhere();
		localCriteriaSupport.and(getter, operator);
		return wrapIntoExecutable(localCriteriaSupport);
	}
	
	@Override
	public <O> RelationalExecutableEntityQuery<C> selectWhere(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator) {
		EntityCriteriaSupport<C> localCriteriaSupport = newWhere();
		localCriteriaSupport.and(setter, operator);
		return wrapIntoExecutable(localCriteriaSupport);
	}
	
	private RelationalExecutableEntityQuery<C> wrapIntoExecutable(EntityCriteriaSupport<C> localCriteriaSupport) {
		MethodReferenceDispatcher methodDispatcher = new MethodReferenceDispatcher();
		return methodDispatcher
				.redirect((SerializableFunction<ExecutableQuery, List<C>>) ExecutableQuery::execute,
						() -> entitySelectExecutor.loadGraph(localCriteriaSupport.getCriteria()))
				.redirect(CriteriaProvider::getCriteria, localCriteriaSupport::getCriteria)
				.redirect(RelationalEntityCriteria.class, localCriteriaSupport, true)
				.build((Class<RelationalExecutableEntityQuery<C>>) (Class) RelationalExecutableEntityQuery.class);
	}
	
	private EntityCriteriaSupport<C> newWhere() {
		// we must clone the underlying support, else it would be modified for all subsequent invokations and criteria will aggregate
		return new EntityCriteriaSupport<>(criteriaSupport);
	}
	
	@Override
	public List<C> selectAll() {
		return entitySelectExecutor.loadGraph(newWhere().getCriteria());
	}
	
	@Override
	public boolean isNew(C entity) {
		return mainPersister.isNew(entity);
	}
	
	@Override
	public Class<C> getClassToPersist() {
		return parentClass;
	}
	
	@Override
	public void addInsertListener(InsertListener insertListener) {
		subEntitiesPersisters.values().forEach(p -> p.addInsertListener(insertListener));
	}
	
	@Override
	public void addUpdateListener(UpdateListener updateListener) {
		subEntitiesPersisters.values().forEach(p -> p.addUpdateListener(updateListener));
	}
	
	@Override
	public void addSelectListener(SelectListener selectListener) {
		subEntitiesPersisters.values().forEach(p -> p.addSelectListener(selectListener));
	}
	
	@Override
	public void addDeleteListener(DeleteListener deleteListener) {
		subEntitiesPersisters.values().forEach(p -> p.addDeleteListener(deleteListener));
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener deleteListener) {
		subEntitiesPersisters.values().forEach(p -> p.addDeleteByIdListener(deleteListener));
	}
	
	/**
	 * Implementation that captures {@link EntityMappingStrategy#addTransformerListener(TransformerListener)}
	 * Made to dispatch those methods subclass strategies since their persisters are in charge of managing their entities (not the parent one).
	 * <p>
	 * Design question : one may think that's not a good design to override a getter, caller should invoke an intention-clear method on
	 * ourselves (Persister) but the case is to add a transformer which is not the goal of the Persister to know implementation
	 * detail : they are to manage cascades and coordinate their mapping strategies. {@link EntityMappingStrategy} are in charge of knowing
	 * {@link Column} actions.
	 *
	 * @return an enhanced version of our main persister mapping strategy which dispatches transformer listeners to sub-entities ones
	 */
	@Override
	public <T extends Table> EntityMappingStrategy<C, I, T> getMappingStrategy() {
		return new EntityMappingStrategyWrapper<C, I, T>(mainPersister.getMappingStrategy()) {
			@Override
			public void addTransformerListener(TransformerListener<C> listener) {
				super.addTransformerListener(listener);
				subEntitiesPersisters.values().forEach(persister -> persister.getMappingStrategy().addTransformerListener(listener));
			}
		};
	}
	
	/**
	 * Implementation made for one-to-one use case
	 *
	 * @param <SRC>
	 * @param sourcePersister source that needs this instance joins
	 * @param leftColumn left part of the join, expected to be one of source table
	 * @param rightColumn right part of the join, expected to be one of current instance table
	 * @param rightTableAlias optional alias for right table, if null table name will be used
	 * @param beanRelationFixer setter that fix relation ofthis instance onto source persister instance
	 * @param optional true for optional relation, makes an outer join, else should create a inner join
	 * @return created join name
	 */
	@Override
	public <SRC, T1 extends Table, T2 extends Table, SRCID, JID> String joinAsOne(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																				  Column<T1, JID> leftColumn,
																				  Column<T2, JID> rightColumn,
																				  String rightTableAlias,
																				  BeanRelationFixer<SRC, C> beanRelationFixer,
																				  boolean optional) {
		
		// because subgraph loading is made in 2 phases (load ids, then entities in a second SQL request done by load listener) we add a passive join
		// (we don't need to create bean nor fulfill properties in first phase) 
		// NB: here rightColumn is parent class primary key or reverse column that owns property (depending how one-to-one relation is mapped) 
		String mainTableJoinName = sourcePersister.getEntityJoinTree().addPassiveJoin(EntityJoinTree.ROOT_STRATEGY_NAME,
				leftColumn, rightColumn, optional ? JoinType.OUTER : JoinType.INNER, Arrays.asSet(rightColumn));
		Column primaryKey = (Column) Iterables.first(getMappingStrategy().getTargetTable().getPrimaryKey().getColumns());
		this.subclassIdMappingStrategies.forEach((c, idMappingStrategy) -> {
			Column subclassPrimaryKey = (Column) Iterables.first(this.tablePerSubEntityType.get(c).getPrimaryKey().getColumns());
			sourcePersister.getEntityJoinTree().addMergeJoin(mainTableJoinName,
					new FirstPhaseRelationLoader<C, I, T2>(idMappingStrategy, subclassPrimaryKey, mainSelectExecutor, CURRENT_2PHASES_LOAD_CONTEXT),
					primaryKey,
					subclassPrimaryKey,
					// since we don't know what kind of sub entity is present we must do an OUTER join between common truk and all sub tables
					JoinType.OUTER);
		});
		
		// adding second phase loader
		((PersisterListener) sourcePersister).addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, CURRENT_2PHASES_LOAD_CONTEXT));
		
		return mainTableJoinName;
	}
	
	@Override
	public <SRC, T1 extends Table, T2 extends Table, SRCID, ID> String joinAsMany(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																				  Column<T1, ID> leftColumn,
																				  Column<T2, ID> rightColumn,
																				  BeanRelationFixer<SRC, C> beanRelationFixer,
																				  @Nullable BiFunction<Row, ColumnedRow, ?> duplicateIdentifierProvider,
																				  String joinName,
																				  boolean optional,
																				  Set<Column<T2, ?>> selectableColumns) {
		
		String createdJoinName = sourcePersister.getEntityJoinTree().addPassiveJoin(joinName,
				leftColumn,
				rightColumn,
				JoinType.OUTER,
				selectableColumns);
		
		// Subgraph loading is made in 2 phases (load ids, then entities in a second SQL request done by load listener)
		this.subclassIdMappingStrategies.forEach((c, idMappingStrategy) -> {
			Column subclassPrimaryKey = (Column) Iterables.first(this.tablePerSubEntityType.get(c).getPrimaryKey().getColumns());
			sourcePersister.getEntityJoinTree().addMergeJoin(createdJoinName,
					new FirstPhaseRelationLoader<C, I, T2>(idMappingStrategy, subclassPrimaryKey, mainSelectExecutor, CURRENT_2PHASES_LOAD_CONTEXT),
					mainTablePrimaryKey,
					subclassPrimaryKey,
					// since we don't know what kind of sub entity is present we must do an OUTER join between common truk and all sub tables
					JoinType.OUTER);
		});
		
		// adding second phase loader
		((PersisterListener) sourcePersister).addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, CURRENT_2PHASES_LOAD_CONTEXT));
		
		return createdJoinName;
	}
	
	@Override
	public <E, ID> void copyRootJoinsTo(EntityJoinTree<E, ID> entityJoinTree, String joinName) {
		// nothing to do here, called by one-to-many engines, which actually call joinWithMany()
	}
	
	@Override
	public EntityJoinTree<C, I> getEntityJoinTree() {
		return mainPersister.getEntityJoinTree();
	}
	
}
