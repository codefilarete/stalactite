package org.gama.stalactite.persistence.engine.runtime;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Duo;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.KeepOrderMap;
import org.gama.lang.exception.NotImplementedException;
import org.gama.lang.trace.ModifiableInt;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.stalactite.persistence.engine.ExecutableQuery;
import org.gama.stalactite.persistence.engine.IEntityPersister;
import org.gama.stalactite.persistence.engine.ISelectExecutor;
import org.gama.stalactite.persistence.engine.IUpdateExecutor;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.IPersisterListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.engine.runtime.JoinedTablesPersister.CriteriaProvider;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.mapping.IRowTransformer.TransformerListener;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.query.EntityCriteriaSupport;
import org.gama.stalactite.persistence.query.RelationalEntityCriteria;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.AbstractRelationalOperator;
import org.gama.stalactite.sql.ConnectionProvider;

/**
 * Class that wraps some other persisters and transfers its invokations to them.
 * Used for polymorphism to dispatch method calls to sub-entities persisters.
 * 
 * @author Guillaume Mary
 */
public class JoinedTablesPolymorphicPersister<C, I> implements IEntityConfiguredJoinedTablesPersister<C, I>, PolymorphicPersister<C> {
	
	private static final ThreadLocal<Set<RelationIds<Object /* E */, Object /* target */, Object /* target identifier */ >>> DIFFERED_ENTITY_LOADER = new ThreadLocal<>();
	
	private final Map<Class<? extends C>, IEntityConfiguredJoinedTablesPersister<C, I>> subEntitiesPersisters;
	/** The wrapper around sub entities loaders, for 2-phases load */
	private final JoinedTablesPolymorphismSelectExecutor<C, I, ?> mainSelectExecutor;
	private final Class<C> parentClass;
	private final Map<Class<? extends C>, IdMappingStrategy<C, I>> subclassIdMappingStrategies;
	private final Map<Class<? extends C>, Table> tablePerSubEntityType;
	private final Column<?, I> mainTablePrimaryKey;
	private final EntityCriteriaSupport<C> criteriaSupport;
	private final JoinedTablesPolymorphismEntitySelectExecutor<C, I, ?> entitySelectExecutor;
	private final IEntityConfiguredJoinedTablesPersister<C, I> mainPersister;
	
	public JoinedTablesPolymorphicPersister(IEntityConfiguredJoinedTablesPersister<C, I> mainPersister,
											Map<Class<? extends C>, IEntityConfiguredJoinedTablesPersister<C, I>> subEntitiesPersisters,
											ConnectionProvider connectionProvider,
											Dialect dialect) {
		this.mainPersister = mainPersister;
		this.parentClass = this.mainPersister.getClassToPersist();
		this.mainTablePrimaryKey = (Column) Iterables.first(mainPersister.getMappingStrategy().getTargetTable().getPrimaryKey().getColumns());
		
		this.subEntitiesPersisters = subEntitiesPersisters;
		Set<Entry<Class<? extends C>, IEntityConfiguredJoinedTablesPersister<C, I>>> subPersisterPerSubEntityType = subEntitiesPersisters.entrySet();
		Map<Class<? extends C>, ISelectExecutor<C, I>> subclassSelectExecutors = Iterables.map(subPersisterPerSubEntityType, Entry::getKey,
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
		this.mainSelectExecutor = new JoinedTablesPolymorphismSelectExecutor<>(
				tablePerSubEntityType,
				subclassSelectExecutors,
				mainPersister.getMappingStrategy().getTargetTable(), connectionProvider, dialect);
		
		this.entitySelectExecutor = new JoinedTablesPolymorphismEntitySelectExecutor(subEntitiesPersisters, subEntitiesPersisters,
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
	public PersisterListener<C, I> getPersisterListener() {
		return mainPersister.getPersisterListener();
	}
	
	@Override
	public int insert(Iterable<? extends C> entities) {
		mainPersister.insert(entities);
		
		ModifiableInt insertCount = new ModifiableInt();
		Map<IEntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister((Iterable) entities);
		
		entitiesPerType.forEach((insertExecutor, adhocEntities) -> insertCount.increment(insertExecutor.insert(adhocEntities)));
		
		return insertCount.getValue();
	}
	
	@Override
	public int updateById(Iterable<C> entities) {
		ModifiableInt mainUpdateCount = new ModifiableInt();
		int mainRowCount = mainPersister.updateById(entities);
		mainUpdateCount.increment(mainRowCount);
		
		ModifiableInt subEntitiesUpdateCount = new ModifiableInt();
		Map<IEntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		
		entitiesPerType.forEach((updateExecutor, adhocEntities) -> subEntitiesUpdateCount.increment(updateExecutor.updateById(adhocEntities)));
		
		// RowCount is either 0 or number of rows updated. In the first case result might be number of rows updated by sub entities.
		// In second case we don't need to change it because sub entities updated row count will also be either 0 or total entities count  
		int rowCount;
		if (mainUpdateCount.getValue() != 0) {
			rowCount = mainUpdateCount.getValue();
		} else {
			rowCount = subEntitiesUpdateCount.getValue();
		}
		
		return rowCount;
	}
	
	@Override
	public int update(Iterable<? extends Duo<? extends C, ? extends C>> differencesIterable, boolean allColumnsStatement) {
		ModifiableInt mainUpdateCount = new ModifiableInt();
		int mainRowCount = mainPersister.update(differencesIterable, allColumnsStatement);
		mainUpdateCount.increment(mainRowCount);
		
		ModifiableInt subEntitiesUpdateCount = new ModifiableInt();
		Map<IUpdateExecutor<C>, Set<Duo<? extends C, ? extends C>>> entitiesPerType = new HashMap<>();
		differencesIterable.forEach(payload ->
				this.subEntitiesPersisters.values().forEach(persister -> {
					C entity = Objects.preventNull(payload.getLeft(), payload.getRight());
					if (persister.getClassToPersist().isInstance(entity)) {
						entitiesPerType.computeIfAbsent(persister, p -> new HashSet<>()).add(payload);
					}
				})
		);
		
		entitiesPerType.forEach((updateExecutor, adhocEntities) ->
				subEntitiesUpdateCount.increment(updateExecutor.update(adhocEntities, allColumnsStatement))
		);
		
		// RowCount is either 0 or number of rows updated. In the first case result might be number of rows updated by sub entities.
		// In second case we don't need to change it because sub entities updated row count will also be either 0 or total entities count  
		int rowCount;
		if (mainUpdateCount.getValue() != 0) {
			rowCount = mainUpdateCount.getValue();
		} else {
			rowCount = subEntitiesUpdateCount.getValue();
		}
		
		return rowCount;
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
	public int delete(Iterable<C> entities) {
		ModifiableInt deleteCount = new ModifiableInt();
		Map<IEntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		
		entitiesPerType.forEach((deleteExecutor, adhocEntities) ->
				deleteCount.increment(deleteExecutor.delete(adhocEntities))
		);
		
		return mainPersister.delete(entities);
	}
	
	@Override
	public int deleteById(Iterable<C> entities) {
		ModifiableInt deleteCount = new ModifiableInt();
		Map<IEntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		
		entitiesPerType.forEach((deleteExecutor, adhocEntities) -> 
				deleteCount.increment(deleteExecutor.deleteById(adhocEntities))
		);
		
		return mainPersister.deleteById(entities);
	}
	
	@Override
	public int persist(Iterable<? extends C> entities) {
		// This class doesn't need to implement this method because it is better handled by wrapper, especially in triggering event
		throw new NotImplementedException("This class doesn't need to implement this method because it is handle by wrapper");
	}
	
	private Map<IEntityPersister<C, I>, Set<C>> computeEntitiesPerPersister(Iterable<C> entities) {
		Map<IEntityPersister<C, I>, Set<C>> entitiesPerType = new KeepOrderMap<>();
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
	public <O> ExecutableEntityQuery<C> selectWhere(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator) {
		EntityCriteriaSupport<C> localCriteriaSupport = newWhere();
		localCriteriaSupport.and(setter, operator);
		return wrapIntoExecutable(localCriteriaSupport);
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
	 * Implementation that captures {@link IEntityMappingStrategy#addTransformerListener(TransformerListener)}
	 * Made to dispatch those methods subclass strategies since their persisters are in charge of managing their entities (not the parent one).
	 * <p>
	 * Design question : one may think that's not a good design to override a getter, caller should invoke an intention-clear method on
	 * ourselves (Persister) but the case is to add a transformer which is not the goal of the Persister to know implementation
	 * detail : they are to manage cascades and coordinate their mapping strategies. {@link IEntityMappingStrategy} are in charge of knowing
	 * {@link Column} actions.
	 *
	 * @return an enhanced version of our main persister mapping strategy which dispatches transformer listeners to sub-entities ones
	 */
	@Override
	public <T extends Table> IEntityMappingStrategy<C, I, T> getMappingStrategy() {
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
	public <SRC, T1 extends Table, T2 extends Table, SRCID, JID> String joinAsOne(IJoinedTablesPersister<SRC, SRCID> sourcePersister,
																				  Column<T1, JID> leftColumn,
																				  Column<T2, JID> rightColumn,
																				  String rightTableAlias,
																				  BeanRelationFixer<SRC, C> beanRelationFixer,
																				  boolean optional) {
		
		// because subgraph loading is made in 2 phases (load ids, then entities in a second SQL request done by load listener) we add a passive join
		// (we don't need to create bean nor fulfill properties in first phase) 
		// NB: here rightColumn is parent class primary key or reverse column that owns property (depending how one-to-one relation is mapped) 
		String mainTableJoinName = sourcePersister.getEntityJoinTree().addPassiveJoin(EntityJoinTree.ROOT_STRATEGY_NAME,
				leftColumn, rightColumn, optional ? JoinType.OUTER : JoinType.INNER, (Set<Column<T2, Object>>) (Set) Arrays.asSet(rightColumn));
		Column primaryKey = (Column) Iterables.first(getMappingStrategy().getTargetTable().getPrimaryKey().getColumns());
		this.subclassIdMappingStrategies.forEach((c, idMappingStrategy) -> {
			Column subclassPrimaryKey = (Column) Iterables.first(this.tablePerSubEntityType.get(c).getPrimaryKey().getColumns());
			sourcePersister.getEntityJoinTree().addMergeJoin(mainTableJoinName,
					new FirstPhaseRelationLoader<C, I, T2>(idMappingStrategy, subclassPrimaryKey, mainSelectExecutor,
							DIFFERED_ENTITY_LOADER),
					primaryKey,
					subclassPrimaryKey,
					// since we don't know what kind of sub entity is present we must do an OUTER join between common truk and all sub tables
					JoinType.OUTER);
		});
		
		// adding second phase loader
		((IPersisterListener) sourcePersister).addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, DIFFERED_ENTITY_LOADER));
		
		return mainTableJoinName;
	}
	
	@Override
	public <SRC, T1 extends Table, T2 extends Table, SRCID> void joinAsMany(IJoinedTablesPersister<SRC, SRCID> sourcePersister,
																			Column<T1, ?> leftColumn,
																			Column<T2, ?> rightColumn,
																			BeanRelationFixer<SRC, C> beanRelationFixer,
																			String joinName,
																			boolean optional) {
		
		String createdJoinName = sourcePersister.getEntityJoinTree().addPassiveJoin(joinName,
				(Column) leftColumn,
				(Column) rightColumn,
				JoinType.OUTER,
				(Set) java.util.Collections.emptySet());
		
		// Subgraph loading is made in 2 phases (load ids, then entities in a second SQL request done by load listener)
		this.subclassIdMappingStrategies.forEach((c, idMappingStrategy) -> {
			Column subclassPrimaryKey = (Column) Iterables.first(this.tablePerSubEntityType.get(c).getPrimaryKey().getColumns());
			sourcePersister.getEntityJoinTree().addMergeJoin(createdJoinName,
					new FirstPhaseRelationLoader<C, I, T2>(idMappingStrategy, subclassPrimaryKey, mainSelectExecutor,
							DIFFERED_ENTITY_LOADER),
					mainTablePrimaryKey,
					subclassPrimaryKey,
					// since we don't know what kind of sub entity is present we must do an OUTER join between common truk and all sub tables
					JoinType.OUTER);
		});
		
		// adding second phase loader
		((IPersisterListener) sourcePersister).addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, DIFFERED_ENTITY_LOADER));
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
