package org.codefilarete.stalactite.persistence.engine.runtime;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
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
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.function.Functions;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.stalactite.persistence.engine.DeleteExecutor;
import org.codefilarete.stalactite.persistence.engine.EntityPersister;
import org.codefilarete.stalactite.persistence.engine.ExecutableQuery;
import org.codefilarete.stalactite.persistence.engine.InsertExecutor;
import org.codefilarete.stalactite.persistence.engine.SelectExecutor;
import org.codefilarete.stalactite.persistence.engine.UpdateExecutor;
import org.codefilarete.stalactite.persistence.engine.configurer.CascadeManyConfigurer;
import org.codefilarete.stalactite.persistence.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.persistence.engine.listener.DeleteListener;
import org.codefilarete.stalactite.persistence.engine.listener.InsertListener;
import org.codefilarete.stalactite.persistence.engine.listener.PersisterListener;
import org.codefilarete.stalactite.persistence.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.persistence.engine.listener.SelectListener;
import org.codefilarete.stalactite.persistence.engine.listener.UpdateListener;
import org.codefilarete.stalactite.persistence.engine.runtime.SimpleRelationalEntityPersister.CriteriaProvider;
import org.codefilarete.stalactite.persistence.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.persistence.engine.runtime.load.EntityJoinTree.EntityInflater.EntityMappingStrategyAdapter;
import org.codefilarete.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.persistence.mapping.ColumnedRow;
import org.codefilarete.stalactite.persistence.mapping.EntityMappingStrategy;
import org.codefilarete.stalactite.persistence.mapping.MappingStrategy.ShadowColumnValueProvider;
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
 * @author Guillaume Mary
 */
public class TablePerClassPolymorphismPersister<C, I, T extends Table<T>> implements EntityConfiguredJoinedTablesPersister<C, I>, PolymorphicPersister<C> {
	
	@SuppressWarnings("java:S5164" /* remove() is called by SecondPhaseRelationLoader.afterSelect() */)
	private static final ThreadLocal<Queue<Set<RelationIds<Object /* E */, Object /* target */, Object /* target identifier */ >>>> DIFFERED_ENTITY_LOADER = new ThreadLocal<>();
	
	private final EntityCriteriaSupport<C> criteriaSupport;
	private final TablePerClassPolymorphicEntitySelectExecutor<C, I, T> entitySelectExecutor;
	private final TablePerClassPolymorphicSelectExecutor<C, I, T> selectExecutor;
	private final Map<Class<? extends C>, UpdateExecutor<C>> subclassUpdateExecutors;
	private final EntityConfiguredJoinedTablesPersister<C, I> mainPersister;
	private final Map<Class<? extends C>, ? extends EntityConfiguredJoinedTablesPersister<C, I>> subEntitiesPersisters;
	
	public TablePerClassPolymorphismPersister(EntityConfiguredJoinedTablesPersister<C, I> mainPersister,
											  Map<Class<? extends C>, SimpleRelationalEntityPersister<C, I, T>> subEntitiesPersisters,
											  ConnectionProvider connectionProvider,
											  Dialect dialect) {
		this.mainPersister = mainPersister;
		Set<Entry<Class<? extends C>, SimpleRelationalEntityPersister<C, I, T>>> entries = subEntitiesPersisters.entrySet();
		this.subclassUpdateExecutors = Iterables.map(entries, Entry::getKey, e -> e.getValue().getUpdateExecutor(), KeepOrderMap::new);
		
		Map<Class, Table> tablePerSubEntity = Iterables.map((Set) entries,
				Entry::getKey,
				Functions.<Entry<Class, SimpleRelationalEntityPersister>, SimpleRelationalEntityPersister, Table>chain(Entry::getValue, SimpleRelationalEntityPersister::getMainTable));
		
		
		this.subEntitiesPersisters = subEntitiesPersisters;
		this.subEntitiesPersisters.forEach((type, persister) ->
				mainPersister.getEntityJoinTree().projectTo(persister.getEntityJoinTree(), EntityJoinTree.ROOT_STRATEGY_NAME)
		);
		
		Map<Class<? extends C>, SelectExecutor<C, I>> subEntitiesSelectors = Iterables.map(subEntitiesPersisters.entrySet(),
				Entry::getKey,
				Functions.chain(Entry::getValue, SimpleRelationalEntityPersister::getSelectExecutor));
		this.selectExecutor = new TablePerClassPolymorphicSelectExecutor<>(
				tablePerSubEntity,
				subEntitiesSelectors,
				(T) mainPersister.getMappingStrategy().getTargetTable(), connectionProvider, dialect.getColumnBinderRegistry());
		
		this.entitySelectExecutor = new TablePerClassPolymorphicEntitySelectExecutor<>(tablePerSubEntity, subEntitiesPersisters,
				(T) mainPersister.getMappingStrategy().getTargetTable(), connectionProvider, dialect.getColumnBinderRegistry());
		
		this.criteriaSupport = new EntityCriteriaSupport<>(mainPersister.getMappingStrategy());
	}
	
	@Override
	public Set<Class<? extends C>> getSupportedEntityTypes() {
		// Note that this implementation can't be tested yet because we don't yet support table-per-class polymorphism
		// combined with other polymoprhism types. It has only been implemented in order to not forget this behavior for the day of the polymorphism
		// combination has come
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
		// in table-per-class main persister table does not participate in database schema : only sub entities persisters do
		return this.subEntitiesPersisters.values().stream().map(ConfiguredPersister::getMappingStrategy).map(EntityMappingStrategy::getTargetTable).collect(Collectors.toList());
	}
	
	@Override
	public PersisterListenerCollection<C, I> getPersisterListener() {
		return mainPersister.getPersisterListener();
	}
	
	@Override
	public void insert(Iterable<? extends C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(InsertExecutor::insert);
	}
	
	@Override
	public void updateById(Iterable<C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(UpdateExecutor::updateById);
	}
	
	@Override
	public void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		Map<Class, Set<Duo<C, C>>> entitiesPerType = new HashMap<>();
		differencesIterable.forEach(payload -> {
			C entity = Objects.preventNull(payload.getLeft(), payload.getRight());
			entitiesPerType.computeIfAbsent(entity.getClass(), k -> new HashSet<>()).add(payload);
		});
		subclassUpdateExecutors.forEach((subclass, updateExecutor) -> {
			Set<Duo<C, C>> entitiesToUpdate = entitiesPerType.get(subclass);
			if (entitiesToUpdate != null) {
				updateExecutor.update(entitiesToUpdate, allColumnsStatement);
			}
		});
	}
	
	@Override
	public List<C> select(Iterable<I> ids) {
		return selectExecutor.select(ids);
	}
	
	@Override
	public void delete(Iterable<C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(DeleteExecutor::delete);
	}
	
	@Override
	public void deleteById(Iterable<C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(DeleteExecutor::deleteById);
	}
	
	@Override
	public void persist(Iterable<? extends C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(EntityPersister::persist);
	}
	
	private Map<EntityPersister<C, I>, Set<C>> computeEntitiesPerPersister(Iterable<? extends C> entities) {
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
		return mainPersister.getClassToPersist();
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
	 * Overriden to capture {@link EntityMappingStrategy#addShadowColumnInsert(ShadowColumnValueProvider)} and
	 * {@link EntityMappingStrategy#addShadowColumnUpdate(ShadowColumnValueProvider)} (see {@link CascadeManyConfigurer})
	 * Made to dispatch those methods subclass strategies since their persisters are in charge of managing their entities (not the parent one).
	 *
	 * Design question : one may think that's not a good design to override a getter, caller should invoke an intention-clear method on
	 * ourselves (Persister) but the case is to add a silent Column insert/update which is not the goal of the Persister to know implementation
	 * detail : they are to manage cascades and coordinate their mapping strategies. {@link EntityMappingStrategy} are in charge of knowing
	 * {@link Column} actions.
	 *
	 * @return an enhanced version of our main persister mapping strategy which dispatches silent column insert/update to sub-entities ones
	 */
	@Override
	public EntityMappingStrategy<C, I, T> getMappingStrategy() {
		return new EntityMappingStrategyWrapper<C, I, T>(mainPersister.getMappingStrategy()) {
			@Override
			public void addTransformerListener(TransformerListener<C> listener) {
				subEntitiesPersisters.values().forEach(persister -> persister.getMappingStrategy().addTransformerListener(listener));
			}
			
			@Override
			public <O> void addShadowColumnInsert(ShadowColumnValueProvider<C, O, T> provider) {
				subEntitiesPersisters.values().forEach(p -> {
					Column<T, O> c = provider.getColumn();
					Column projectedColumn = p.getMappingStrategy().getTargetTable().addColumn(c.getName(), c.getJavaType(), c.getSize());
					p.getMappingStrategy().addShadowColumnInsert(new ShadowColumnValueProvider<>(projectedColumn, provider.getValueProvider()));
				});
			}
			
			@Override
			public <O> void addShadowColumnUpdate(ShadowColumnValueProvider<C, O, T> provider) {
				subEntitiesPersisters.values().forEach(p -> {
					Column<T, O> c = provider.getColumn();
					Column projectedColumn = p.getMappingStrategy().getTargetTable().addColumn(c.getName(), c.getJavaType(), c.getSize());
					p.getMappingStrategy().addShadowColumnUpdate(new ShadowColumnValueProvider<>(projectedColumn, provider.getValueProvider()));
				});
			}
		};
	}
	
	@Override
	public <SRC, T1 extends Table, T2 extends Table, SRCID, JID> String joinAsOne(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																				  Column<T1, JID> leftColumn,
																				  Column<T2, JID> rightColumn,
																				  String rightTableAlias,
																				  BeanRelationFixer<SRC, C> beanRelationFixer,
																				  boolean optional) {
		String createdJoinNodeName = sourcePersister.getEntityJoinTree().addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME,
				new EntityMappingStrategyAdapter<>((EntityMappingStrategy) this.getMappingStrategy()),
				leftColumn,
				rightColumn,
				null,
				optional ? JoinType.OUTER : JoinType.INNER,
				beanRelationFixer, Collections.emptySet());
		
		copyRootJoinsTo(sourcePersister.getEntityJoinTree(), createdJoinNodeName);
		
		return createdJoinNodeName;
	}
	
	@Override
	public <SRC, T1 extends Table, T2 extends Table, SRCID, ID> String joinAsMany(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																				  Column<T1, ID> leftColumn,
																				  Column<T2, ID> rightColumn,
																				  BeanRelationFixer<SRC, C> beanRelationFixer,
																				  @Nullable BiFunction<Row, ColumnedRow, ?> duplicateIdentifierProvider, String joinName,
																				  boolean optional,
																				  Set<Column<T2, ?>> selectableColumns) {
		// TODO: simplify query : it joins on target table as many as subentities which can be reduced to one join if FirstPhaseRelationLoader
		//  can compute disciminatorValue 
		Column<T, Object> mainTablePK = Iterables.first(((T) mainPersister.getMappingStrategy().getTargetTable()).getPrimaryKey().getColumns());
		Map<EntityConfiguredJoinedTablesPersister, Column> joinColumnPerSubPersister = new HashMap<>();
		if (rightColumn.equals(mainTablePK)) {
			// join is made on primary key => case is association table
			subEntitiesPersisters.forEach((c, subPersister) -> {
				Column<T, Object> column = Iterables.first((Set<Column>) subPersister.getMappingStrategy().getTargetTable().getPrimaryKey().getColumns());
				joinColumnPerSubPersister.put(subPersister, column);
			});
		} else {
			// join is made on a foreign key => case of relation owned by reverse side
			subEntitiesPersisters.forEach((c, subPersister) -> {
				Column<T, ?> column = subPersister.getMappingStrategy().getTargetTable().addColumn(rightColumn.getName(), rightColumn.getJavaType());
				joinColumnPerSubPersister.put(subPersister, column);
			});
		}
		
		subEntitiesPersisters.forEach((c, subPersister) -> {
			Column subclassPrimaryKey = Iterables.first((Set<Column>) subPersister.getMappingStrategy().getTargetTable().getPrimaryKey().getColumns());
			sourcePersister.getEntityJoinTree().addMergeJoin(joinName,
					new FirstPhaseRelationLoader<>(subPersister.getMappingStrategy().getIdMappingStrategy(), subclassPrimaryKey, selectExecutor,
							DIFFERED_ENTITY_LOADER),
					leftColumn, joinColumnPerSubPersister.get(subPersister), JoinType.OUTER);
		});
		
		// adding second phase loader
		((PersisterListener) sourcePersister).addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, DIFFERED_ENTITY_LOADER));
		
		// FIXME : we shouldn't return null here but a created join node name: which one since we have several table to join ? see joinAsOne(..) maybe ?
		return null;
	}
	
	@Override
	public EntityJoinTree<C, I> getEntityJoinTree() {
		return mainPersister.getEntityJoinTree();
	}
	
	@Override
	public <E, ID> void copyRootJoinsTo(EntityJoinTree<E, ID> entityJoinTree, String joinName) {
		getEntityJoinTree().projectTo(getEntityJoinTree(), joinName);
	}
	
}
