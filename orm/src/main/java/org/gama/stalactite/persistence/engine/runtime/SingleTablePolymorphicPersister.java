package org.gama.stalactite.persistence.engine.runtime;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Duo;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.KeepOrderMap;
import org.gama.lang.exception.NotImplementedException;
import org.gama.lang.trace.ModifiableInt;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.stalactite.persistence.engine.ExecutableQuery;
import org.gama.stalactite.persistence.engine.IEntityPersister;
import org.gama.stalactite.persistence.engine.IUpdateExecutor;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.gama.stalactite.persistence.engine.configurer.CascadeManyConfigurer;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.IPersisterListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.engine.runtime.EntityMappingStrategyTreeJoinPoint.JoinType;
import org.gama.stalactite.persistence.engine.runtime.JoinedTablesPersister.CriteriaProvider;
import org.gama.stalactite.persistence.mapping.AbstractTransformer.TransformerListener;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.mapping.IMappingStrategy.ShadowColumnValueProvider;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.query.EntityCriteriaSupport;
import org.gama.stalactite.persistence.query.RelationalEntityCriteria;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.AbstractRelationalOperator;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.result.Row;

/**
 * @author Guillaume Mary
 */
public class SingleTablePolymorphicPersister<C, I, T extends Table<T>, D> implements IEntityConfiguredJoinedTablesPersister<C, I>, PolymorphicPersister<C> {
	
	private static final ThreadLocal<Set<RelationIds<Object /* E */, Object /* target */, Object /* target identifier */ >>> DIFFERED_ENTITY_LOADER = new ThreadLocal<>();
	
	private final SingleTablePolymorphismSelectExecutor<C, I, T, D> selectExecutor;
	private final Map<Class<? extends C>, IEntityConfiguredJoinedTablesPersister<C, I>> subEntitiesPersisters;
	private final IEntityConfiguredJoinedTablesPersister<C, I> mainPersister;
	private final Column<T, D> discriminatorColumn;
	private final SingleTablePolymorphism<C, D> polymorphismPolicy;
	private final SingleTablePolymorphismEntitySelectExecutor<C, I, T, D> entitySelectExecutor;
	private final EntityCriteriaSupport<C> criteriaSupport;
	
	public SingleTablePolymorphicPersister(IEntityConfiguredJoinedTablesPersister<C, I> mainPersister,
										   Map<Class<? extends C>, IEntityConfiguredJoinedTablesPersister<C, I>> subEntitiesPersisters,
										   ConnectionProvider connectionProvider,
										   Dialect dialect,
										   Column<T, D> discriminatorColumn,
										   SingleTablePolymorphism<C, D> polymorphismPolicy) {
		this.mainPersister = mainPersister;
		this.discriminatorColumn = discriminatorColumn;
		this.polymorphismPolicy = polymorphismPolicy;
		
		this.subEntitiesPersisters = subEntitiesPersisters;
		ShadowColumnValueProvider<C, D, T> discriminatorValueProvider = new ShadowColumnValueProvider<>(discriminatorColumn,
				c -> polymorphismPolicy.getDiscriminatorValue((Class<? extends C>) c.getClass()));
		this.subEntitiesPersisters.values().forEach(subclassPersister -> ((IEntityMappingStrategy) subclassPersister.getMappingStrategy())
				.addShadowColumnInsert(discriminatorValueProvider));
		
		subEntitiesPersisters.forEach((type, persister) ->
				mainPersister.copyJoinsRootTo(persister.getEntityMappingStrategyTreeSelectBuilder(), EntityMappingStrategyTreeSelectBuilder.ROOT_STRATEGY_NAME)
		);
		
		this.selectExecutor = new SingleTablePolymorphismSelectExecutor<>(
				subEntitiesPersisters,
				discriminatorColumn,
				polymorphismPolicy,
				(T) mainPersister.getMappingStrategy().getTargetTable(),
				connectionProvider,
				dialect);
		
		this.entitySelectExecutor = new SingleTablePolymorphismEntitySelectExecutor<>(
				subEntitiesPersisters,
				discriminatorColumn,
				polymorphismPolicy,
				mainPersister.getEntityMappingStrategyTreeSelectBuilder(),
				connectionProvider,
				dialect);
		
		this.criteriaSupport = new EntityCriteriaSupport<>(mainPersister.getMappingStrategy());
	}
	
	@Override
	public Set<Class<? extends C>> getSupportedEntityTypes() {
		return this.subEntitiesPersisters.keySet();
	}
	
	@Override
	public Collection<Table> giveImpliedTables() {
		// Implied tables are those of sub entities.
		// Note that doing this lately (not in constructor) garanties that it is uptodate because sub entities may have relations which are configured
		// out of constructor by caller
		Set<Table> subTables = subEntitiesPersisters.values().stream().flatMap(p -> p.giveImpliedTables().stream()).collect(Collectors.toSet());
		return org.gama.lang.collection.Collections.cat(mainPersister.giveImpliedTables(), subTables);
	}
	
	@Override
	public PersisterListener<C, I> getPersisterListener() {
		return mainPersister.getPersisterListener();
	}
	
	@Override
	public int insert(Iterable<? extends C> entities) {
		Map<IEntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		
		ModifiableInt insertCount = new ModifiableInt();
		entitiesPerType.forEach((insertExecutor, cs) -> insertCount.increment(insertExecutor.insert(cs)));
		
		return insertCount.getValue();
	}
	
	@Override
	public int updateById(Iterable<C> entities) {
		Map<IEntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		
		ModifiableInt insertCount = new ModifiableInt();
		entitiesPerType.forEach((updateExecutor, cs) -> insertCount.increment(updateExecutor.updateById(cs)));
		
		return insertCount.getValue();
	}
	
	@Override
	public int update(Iterable<? extends Duo<? extends C, ? extends C>> differencesIterable, boolean allColumnsStatement) {
		Map<IUpdateExecutor<C>, Set<Duo<? extends C, ? extends C>>> entitiesPerType = new HashMap<>();
		differencesIterable.forEach(payload ->
				this.subEntitiesPersisters.values().forEach(persister -> {
					C entity = Objects.preventNull(payload.getLeft(), payload.getRight());
					if (persister.getClassToPersist().isInstance(entity)) {
						entitiesPerType.computeIfAbsent(persister, p -> new HashSet<>()).add(payload);
					}
				})
		);
		
		ModifiableInt updateCount = new ModifiableInt();
		entitiesPerType.forEach((updateExecutor, adhocEntities) ->
				updateCount.increment(updateExecutor.update(adhocEntities, allColumnsStatement))
		);
		
		return updateCount.getValue();
	}
	
	@Override
	public List<C> select(Iterable<I> ids) {
		return selectExecutor.select(ids);
	}
	
	@Override
	public int delete(Iterable<C> entities) {
		Map<IEntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		
		ModifiableInt deleteCount = new ModifiableInt();
		// We could think that deleting throught main persister would be suffiscient because we are in a single-table hence only table is touched
		// but that's wrong if subpersisters are more complex (themselves polymorphic or with relations) so we delegate it to subpersisters
		// Please note that SQL orders are fully not optimized because each subpersisters will create (batched) statements for its entities on main table
		// whereas we could have this statements grouped
		entitiesPerType.forEach((deleteExecutor, adhocEntities) -> deleteCount.increment(deleteExecutor.delete(adhocEntities)));
		
		return deleteCount.getValue();
	}
	
	@Override
	public int deleteById(Iterable<C> entities) {
		Map<IEntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		
		ModifiableInt deleteCount = new ModifiableInt();
		// We could think that deleting throught main persister would be suffiscient because we are in a single-table hence only table is touched
		// but that's wrong if subpersisters are more complex (themselves polymorphic or with relations) so we delegate it to subpersisters
		// Please note that SQL orders are fully not optimized because each subpersisters will create (batched) statements for its entities on main table
		// whereas we could have this statements grouped
		entitiesPerType.forEach((deleteExecutor, adhocEntities) -> deleteCount.increment(deleteExecutor.deleteById(adhocEntities)));
		
		return deleteCount.getValue();
	}
	
	private Map<IEntityPersister<C, I>, Set<C>> computeEntitiesPerPersister(Iterable<? extends C> entities) {
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
	public int persist(Iterable<? extends C> entities) {
		// This class doesn't need to implement this method because it is better handled by wrapper, especially in triggering event
		throw new NotImplementedException("This class doesn't need to implement this method because it is handle by wrapper");
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
	 * Overriden to capture {@link IEntityMappingStrategy#addShadowColumnInsert(ShadowColumnValueProvider)} and
	 * {@link IEntityMappingStrategy#addShadowColumnUpdate(ShadowColumnValueProvider)} (see {@link CascadeManyConfigurer})
	 * Made to dispatch those methods subclass strategies since their persisters are in charge of managing their entities (not the parent one).
	 *
	 * Design question : one may think that's not a good design to override a getter, caller should invoke an intention-clear method on
	 * ourselves (Persister) but the case is to add a silent Column insert/update which is not the goal of the Persister to know implementation
	 * detail : they are to manage cascades and coordinate their mapping strategies. {@link IEntityMappingStrategy} are in charge of knowing
	 * {@link Column} actions.
	 *
	 * @return an enhanced version of our main persister mapping strategy which dispatches silent column insert/update to sub-entities ones
	 */
	@Override
	public IEntityMappingStrategy<C, I, T> getMappingStrategy() {
		// TODO: This is not the cleanest implementation because we use MethodReferenceDispatcher which is kind of overkill : use a dispatching
		//  interface
		MethodReferenceDispatcher methodReferenceDispatcher = new MethodReferenceDispatcher();
		IEntityMappingStrategy<C, I, T> result = methodReferenceDispatcher
				.redirect((SerializableBiConsumer<IEntityMappingStrategy<C, I, T>, ShadowColumnValueProvider<C, Object, T>>)
								IEntityMappingStrategy::addShadowColumnInsert,
						provider -> subEntitiesPersisters.values().forEach(p -> ((IEntityMappingStrategy) p.getMappingStrategy()).addShadowColumnInsert(provider)))
				.redirect((SerializableBiConsumer<IEntityMappingStrategy<C, I, T>, ShadowColumnValueProvider<C, Object, T>>)
								IEntityMappingStrategy::addShadowColumnUpdate,
						provider -> subEntitiesPersisters.values().forEach(p -> ((IEntityMappingStrategy) p.getMappingStrategy()).addShadowColumnUpdate(provider)))
				.fallbackOn(mainPersister.getMappingStrategy())
				.build((Class<IEntityMappingStrategy<C, I, T>>) (Class) IEntityMappingStrategy.class);
		return result;
	}
	
	@Override
	public <SRC, T1 extends Table, T2 extends Table> void joinAsOne(IJoinedTablesPersister<SRC, I> sourcePersister,
																	Column<T1, I> leftColumn,
																	Column<T2, I> rightColumn,
																	BeanRelationFixer<SRC, C> beanRelationFixer,
																	boolean optional) {
		
		// TODO: simplify query : it joins on target table as many as subentities which can be reduced to one join if FirstPhaseOneToOneLoader
		//  can compute disciminatorValue 
		subEntitiesPersisters.forEach((subEntityType, subPersister) -> {
			Column subclassPrimaryKey = (Column) Iterables.first(subPersister.getMappingStrategy().getTargetTable().getPrimaryKey().getColumns());
			sourcePersister.getEntityMappingStrategyTreeSelectBuilder().addMergeJoin(EntityMappingStrategyTreeSelectBuilder.ROOT_STRATEGY_NAME,
					new SingleTableFirstPhaseOneToOneLoader(subPersister.getMappingStrategy().getIdMappingStrategy(),
							subclassPrimaryKey, selectExecutor, mainPersister.getClassToPersist(), DIFFERED_ENTITY_LOADER,
							subEntityType, discriminatorColumn),
					(Set) Arrays.asHashSet(subclassPrimaryKey, leftColumn, rightColumn, discriminatorColumn),
					leftColumn, rightColumn, optional ? JoinType.OUTER : JoinType.INNER);
		});
		
		
		// adding second phase loader
		((IPersisterListener) sourcePersister).addSelectListener(new SecondPhaseOneToOneLoader<>(beanRelationFixer, DIFFERED_ENTITY_LOADER));
	}
	
	@Override
	public <SRC, T1 extends Table, T2 extends Table, J> void joinAsMany(IJoinedTablesPersister<SRC, J> sourcePersister,
																		Column<T1, J> leftColumn, Column<T2, J> rightColumn,
																		BeanRelationFixer<SRC, C> beanRelationFixer, String joinName,
																		boolean optional) {
		
		sourcePersister.getEntityMappingStrategyTreeSelectBuilder().addPassiveJoin(joinName,
				leftColumn,
				rightColumn,
				JoinType.OUTER,
				(Set) Collections.emptySet());
		
		// TODO: simplify query : it joins on target table as many as subentities which can be reduced to one join if FirstPhaseOneToOneLoader
		//  can compute disciminatorValue 
		subEntitiesPersisters.forEach((subEntityType, subPersister) -> {
			Column subclassPrimaryKey = (Column) Iterables.first(subPersister.getMappingStrategy().getTargetTable().getPrimaryKey().getColumns());
			sourcePersister.getEntityMappingStrategyTreeSelectBuilder().addMergeJoin(EntityMappingStrategyTreeSelectBuilder.ROOT_STRATEGY_NAME,
					new SingleTableFirstPhaseOneToOneLoader(subPersister.getMappingStrategy().getIdMappingStrategy(),
							subclassPrimaryKey, selectExecutor, mainPersister.getClassToPersist(), DIFFERED_ENTITY_LOADER,
							subEntityType, discriminatorColumn),
					(Set) Arrays.asHashSet(rightColumn, subclassPrimaryKey, discriminatorColumn),
					leftColumn, subclassPrimaryKey, JoinType.OUTER);
		});
		
		
		// adding second phase loader
		((IPersisterListener) sourcePersister).addSelectListener(new SecondPhaseOneToOneLoader<>(beanRelationFixer, DIFFERED_ENTITY_LOADER));	
	}
	
	@Override
	public EntityMappingStrategyTreeSelectBuilder<C, I, ?> getEntityMappingStrategyTreeSelectBuilder() {
		return mainPersister.getEntityMappingStrategyTreeSelectBuilder();
	}
	
	@Override
	public <E, ID, TT extends Table> void copyJoinsRootTo(EntityMappingStrategyTreeSelectBuilder<E, ID, TT> entityMappingStrategyTreeSelectBuilder, String joinName) {
		throw new UnsupportedOperationException();
	}
	
	private class SingleTableFirstPhaseOneToOneLoader extends FirstPhaseOneToOneLoader {
		private final Column<T, D> discriminatorColumn;
		private final Class<? extends C> subEntityType;
		
		private SingleTableFirstPhaseOneToOneLoader(IdMappingStrategy<C, I> subEntityIdMappingStrategy,
													Column primaryKey,
													SingleTablePolymorphismSelectExecutor<C, I, T, D> selectExecutor,
													Class<C> mainType,
													ThreadLocal<Set<RelationIds<Object, Object, Object>>> relationIdsHolder,
													Class<? extends C> subEntityType,
													Column<T, D> discriminatorColumn) {
			super(subEntityIdMappingStrategy, primaryKey, selectExecutor, mainType, relationIdsHolder);
			this.discriminatorColumn = discriminatorColumn;
			this.subEntityType = subEntityType;
		}
		
		@Override
		protected void fillCurrentRelationIds(Row row, Object bean, ColumnedRow columnedRow) {
			D dtype = columnedRow.getValue(discriminatorColumn, row);
			if (polymorphismPolicy.getDiscriminatorValue(subEntityType).equals(dtype)) {
				super.fillCurrentRelationIds(row, bean, columnedRow);
			}
		}
	}
}
