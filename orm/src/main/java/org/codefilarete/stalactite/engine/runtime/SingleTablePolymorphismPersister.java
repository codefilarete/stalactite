package org.codefilarete.stalactite.engine.runtime;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.query.RelationalEntityCriteria;
import org.codefilarete.tool.collection.Collections;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.exception.NotImplementedException;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.stalactite.engine.DeleteExecutor;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.ExecutableQuery;
import org.codefilarete.stalactite.engine.InsertExecutor;
import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.UpdateExecutor;
import org.codefilarete.stalactite.engine.configurer.CascadeManyConfigurer;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersisterListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister.CriteriaProvider;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.Mapping.ShadowColumnValueProvider;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.query.model.AbstractRelationalOperator;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * @author Guillaume Mary
 */
public class SingleTablePolymorphismPersister<C, I, T extends Table<T>, DTYPE> implements EntityConfiguredJoinedTablesPersister<C, I>, PolymorphicPersister<C> {
	
	@SuppressWarnings("java:S5164" /* remove() is called by SecondPhaseRelationLoader.afterSelect() */)
	private static final ThreadLocal<Queue<Set<RelationIds<Object /* E */, Object /* target */, Object /* target identifier */ >>>> DIFFERED_ENTITY_LOADER = new ThreadLocal<>();
	
	private final SingleTablePolymorphismSelectExecutor<C, I, T, DTYPE> selectExecutor;
	private final Map<Class<? extends C>, EntityConfiguredJoinedTablesPersister<? extends C, I>> subEntitiesPersisters;
	private final EntityConfiguredJoinedTablesPersister<C, I> mainPersister;
	private final Column<T, DTYPE> discriminatorColumn;
	private final SingleTablePolymorphism<C, DTYPE> polymorphismPolicy;
	private final SingleTablePolymorphismEntitySelectExecutor<C, I, T, DTYPE> entitySelectExecutor;
	private final EntityCriteriaSupport<C> criteriaSupport;
	
	public SingleTablePolymorphismPersister(EntityConfiguredJoinedTablesPersister<C, I> mainPersister,
											Map<Class<? extends C>, EntityConfiguredJoinedTablesPersister<? extends C, I>> subEntitiesPersisters,
											ConnectionProvider connectionProvider,
											Dialect dialect,
											Column<T, DTYPE> discriminatorColumn,
											SingleTablePolymorphism<C, DTYPE> polymorphismPolicy) {
		this.mainPersister = mainPersister;
		this.discriminatorColumn = discriminatorColumn;
		this.polymorphismPolicy = polymorphismPolicy;
		
		this.subEntitiesPersisters = subEntitiesPersisters;
		ShadowColumnValueProvider<C, DTYPE, T> discriminatorValueProvider = new ShadowColumnValueProvider<>(discriminatorColumn,
				c -> polymorphismPolicy.getDiscriminatorValue((Class<? extends C>) c.getClass()));
		this.subEntitiesPersisters.values().forEach(subclassPersister -> ((EntityMapping) subclassPersister.getMapping())
				.addShadowColumnInsert(discriminatorValueProvider));
		
		subEntitiesPersisters.forEach((type, persister) ->
				mainPersister.copyRootJoinsTo(persister.getEntityJoinTree(), EntityJoinTree.ROOT_STRATEGY_NAME)
		);
		
		this.selectExecutor = new SingleTablePolymorphismSelectExecutor<>(
				subEntitiesPersisters,
				discriminatorColumn,
				polymorphismPolicy,
				(T) mainPersister.getMapping().getTargetTable(),
				connectionProvider,
				dialect);
		
		this.entitySelectExecutor = new SingleTablePolymorphismEntitySelectExecutor<>(
				subEntitiesPersisters,
				discriminatorColumn,
				polymorphismPolicy,
				mainPersister.getEntityJoinTree(),
				connectionProvider,
				dialect);
		
		this.criteriaSupport = new EntityCriteriaSupport<>(mainPersister.getMapping());
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
		// Note that doing this lately (not in constructor) garanties that it is uptodate because sub entities may have relations which are configured
		// out of constructor by caller
		Set<Table> subTables = subEntitiesPersisters.values().stream().flatMap(p -> p.giveImpliedTables().stream()).collect(Collectors.toSet());
		return Collections.cat(mainPersister.giveImpliedTables(), subTables);
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
		entitiesPerType.forEach((updateExecutor, cs) -> updateExecutor.updateById(cs));
	}
	
	@Override
	public void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		Map<UpdateExecutor<C>, Set<Duo<C, C>>> entitiesPerType = new HashMap<>();
		differencesIterable.forEach(payload ->
				this.subEntitiesPersisters.values().forEach(persister -> {
					C entity = Objects.preventNull(payload.getLeft(), payload.getRight());
					if (persister.getClassToPersist().isInstance(entity)) {
						entitiesPerType.computeIfAbsent((UpdateExecutor<C>) persister, p -> new HashSet<>()).add(payload);
					}
				})
		);
		
		entitiesPerType.forEach((updateExecutor, adhocEntities) -> updateExecutor.update(adhocEntities, allColumnsStatement));
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
	
	private Map<EntityPersister<C, I>, Set<C>> computeEntitiesPerPersister(Iterable<? extends C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = new KeepOrderMap<>();
		entities.forEach(entity ->
				this.subEntitiesPersisters.values().forEach(persister -> {
					if (persister.getClassToPersist().isInstance(entity)) {
						entitiesPerType.computeIfAbsent((EntityPersister<C, I>) persister, p -> new HashSet<>()).add(entity);
					}
				})
		);
		return entitiesPerType;
	}
	
	@Override
	public void persist(Iterable<? extends C> entities) {
		// This class doesn't need to implement this method because it is better handled by wrapper, especially in triggering event
		throw new NotImplementedException("This class doesn't need to implement this method because it is handled by wrapper");
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
	 * Overriden to capture {@link EntityMapping#addShadowColumnInsert(ShadowColumnValueProvider)} and
	 * {@link EntityMapping#addShadowColumnUpdate(ShadowColumnValueProvider)} (see {@link CascadeManyConfigurer})
	 * Made to dispatch those methods subclass strategies since their persisters are in charge of managing their entities (not the parent one).
	 *
	 * Design question : one may think that's not a good design to override a getter, caller should invoke an intention-clear method on
	 * ourselves (Persister) but the case is to add a silent Column insert/update which is not the goal of the Persister to know implementation
	 * detail : they are to manage cascades and coordinate their mapping strategies. {@link EntityMapping} are in charge of knowing
	 * {@link Column} actions.
	 *
	 * @return an enhanced version of our main persister mapping strategy which dispatches silent column insert/update to sub-entities ones
	 */
	@Override
	public EntityMapping<C, I, T> getMapping() {
		return new EntityMappingWrapper<C, I, T>(mainPersister.getMapping()) {
			@Override
			public void addTransformerListener(TransformerListener<C> listener) {
				subEntitiesPersisters.values().forEach(p -> ((EntityMapping) p.getMapping()).addTransformerListener(listener));
			}
			
			@Override
			public <O> void addShadowColumnInsert(ShadowColumnValueProvider<C, O, T> provider) {
				subEntitiesPersisters.values().forEach(p -> ((EntityMapping) p.getMapping()).addShadowColumnInsert(provider));
			}
			
			@Override
			public <O> void addShadowColumnUpdate(ShadowColumnValueProvider<C, O, T> provider) {
				subEntitiesPersisters.values().forEach(p -> ((EntityMapping) p.getMapping()).addShadowColumnUpdate(provider));
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
		
		Column subclassPrimaryKey = (Column) Iterables.first(mainPersister.getMapping().getTargetTable().getPrimaryKey().getColumns());
		String createdJoinNodeName = sourcePersister.getEntityJoinTree().addMergeJoin(EntityJoinTree.ROOT_STRATEGY_NAME,
				new SingleTableFirstPhaseRelationLoader(mainPersister.getMapping().getIdMapping(),
						subclassPrimaryKey, selectExecutor,
						DIFFERED_ENTITY_LOADER,
						discriminatorColumn, subEntitiesPersisters::get),
				leftColumn, rightColumn, optional ? JoinType.OUTER : JoinType.INNER);
		
		
		// adding second phase loader
		((PersisterListener) sourcePersister).addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, DIFFERED_ENTITY_LOADER));
		
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
		
		// Subgraph loading is made in 2 phases (load ids, then entities in a second SQL request done by load listener)
		Column subclassPrimaryKey = (Column) Iterables.first(mainPersister.getMapping().getTargetTable().getPrimaryKey().getColumns());
		String createdJoinNodeName = sourcePersister.getEntityJoinTree().addMergeJoin(joinName,
				new SingleTableFirstPhaseRelationLoader(mainPersister.getMapping().getIdMapping(),
						subclassPrimaryKey, selectExecutor,
						DIFFERED_ENTITY_LOADER,
						discriminatorColumn, subEntitiesPersisters::get),
				(Column<T1, I>) leftColumn, (Column<T2, I>) rightColumn, JoinType.OUTER);
		
		
		// adding second phase loader
		((PersisterListener) sourcePersister).addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, DIFFERED_ENTITY_LOADER));
		
		return createdJoinNodeName;
	}
	
	@Override
	public EntityJoinTree<C, I> getEntityJoinTree() {
		return mainPersister.getEntityJoinTree();
	}
	
	@Override
	public <E, ID> void copyRootJoinsTo(EntityJoinTree<E, ID> entityJoinTree, String joinName) {
		throw new UnsupportedOperationException();
	}
	
	private class SingleTableFirstPhaseRelationLoader extends FirstPhaseRelationLoader {
		private final Column<T, DTYPE> discriminatorColumn;
		private final Function<Class, SelectExecutor> subtypeSelectors;
		private final Set<DTYPE> discriminatorValues;
		
		private SingleTableFirstPhaseRelationLoader(IdMapping<C, I> subEntityIdMapping,
													Column primaryKey,
													SingleTablePolymorphismSelectExecutor<C, I, T, DTYPE> selectExecutor,
													ThreadLocal<Queue<Set<RelationIds<Object, Object, Object>>>> relationIdsHolder,
													Column<T, DTYPE> discriminatorColumn, Function<Class, SelectExecutor> subtypeSelectors) {
			// Note that selectExecutor won't be used because we dynamically lookup for it in fillCurrentRelationIds
			super(subEntityIdMapping, primaryKey, selectExecutor, relationIdsHolder);
			this.discriminatorColumn = discriminatorColumn;
			this.subtypeSelectors = subtypeSelectors;
			this.discriminatorValues = Iterables.collect(polymorphismPolicy.getSubClasses(), conf -> polymorphismPolicy.getDiscriminatorValue(conf.getEntityType()), HashSet::new);
		}
		
		@Override
		protected void fillCurrentRelationIds(Row row, Object bean, ColumnedRow columnedRow) {
			DTYPE discriminator = columnedRow.getValue(discriminatorColumn, row);
			// we avoid NPE on polymorphismPolicy.getClass(discriminator) caused by null discriminator in case of empty relation
			// by only treating known discriminator values (prefered way to check against null because type can be primitive one)
			if (discriminatorValues.contains(discriminator)) {
				Set<RelationIds<Object, C, I>> relationIds = ((Queue<Set<RelationIds<Object, C, I>>>) relationIdsHolder.get()).peek();
				relationIds.add(new RelationIds(giveSelector(discriminator),
												idMapping.getIdAccessor()::getId, bean, (I) columnedRow.getValue(primaryKey, row)));
			}
		}
		
		@Override
		public Set<Column> getSelectableColumns() {
			return Arrays.asSet(primaryKey, discriminatorColumn);
		}
		
		private SelectExecutor giveSelector(DTYPE discriminator) {
			return subtypeSelectors.apply(polymorphismPolicy.getClass(discriminator));
		}
	}
}