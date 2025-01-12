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

import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.DeleteExecutor;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.InsertExecutor;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.UpdateExecutor;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelationConfigurer;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersistListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateByIdListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.PolymorphicEntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.PolymorphicMergeJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.load.SingleTablePolymorphicRelationJoinNode;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.Mapping.ShadowColumnValueProvider;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_STRATEGY_NAME;

/**
 * @author Guillaume Mary
 */
public class SingleTablePolymorphismPersister<C, I, T extends Table<T>, DTYPE> extends AbstractPolymorphismPersister<C, I> {
	
	@SuppressWarnings("java:S5164" /* remove() is called by SecondPhaseRelationLoader.afterSelect() */)
	private static final ThreadLocal<Queue<Set<RelationIds<Object /* E */, Object /* target */, Object /* target identifier */ >>>> DIFFERED_ENTITY_LOADER = new ThreadLocal<>();
	
	private final ConfiguredRelationalPersister<C, I> mainPersister;
	private final Column<T, DTYPE> discriminatorColumn;
	private final SingleTablePolymorphism<C, DTYPE> polymorphismPolicy;
	private final SingleTablePolymorphismSelectExecutor<C, I, T, DTYPE> selectExecutor;
	
	public SingleTablePolymorphismPersister(ConfiguredRelationalPersister<C, I> mainPersister,
											Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> subEntitiesPersisters,
											ConnectionProvider connectionProvider,
											Dialect dialect,
											Column<T, DTYPE> discriminatorColumn,
											SingleTablePolymorphism<C, DTYPE> polymorphismPolicy) {
		super(mainPersister,
				subEntitiesPersisters,
				new SingleTablePolymorphismEntitySelector<>(mainPersister,
						subEntitiesPersisters,
						discriminatorColumn,
						polymorphismPolicy,
						connectionProvider,
						dialect));
		this.mainPersister = mainPersister;
		this.discriminatorColumn = discriminatorColumn;
		this.polymorphismPolicy = polymorphismPolicy;
		
		ShadowColumnValueProvider<C, T> discriminatorValueProvider = new ShadowColumnValueProvider<C, T>() {
			
			@Override
			public Set<Column<T, ?>> getColumns() {
				return Arrays.asHashSet(discriminatorColumn);
			}
			
			@Override
			public Map<Column<T, ?>, Object> giveValue(C bean) {
				Map<Column<T, ?>, Object> result = new HashMap<>();
				result.put(discriminatorColumn, polymorphismPolicy.getDiscriminatorValue((Class<? extends C>) bean.getClass()));
				return result;
			}
		};
		this.subEntitiesPersisters.values().forEach(subclassPersister -> ((EntityMapping) subclassPersister.getMapping())
				.addShadowColumnInsert(discriminatorValueProvider));
		
		subEntitiesPersisters.forEach((type, persister) ->
				mainPersister.copyRootJoinsTo(persister.getEntityJoinTree(), ROOT_STRATEGY_NAME)
		);
		
		this.selectExecutor = new SingleTablePolymorphismSelectExecutor<>(
				mainPersister,
				subEntitiesPersisters,
				discriminatorColumn,
				polymorphismPolicy,
				connectionProvider,
				dialect);
	}
	
	@Override
	public void registerRelation(ValueAccessPoint<C> relation, ConfiguredRelationalPersister<?, ?> persister) {
		criteriaSupport.registerRelation(relation, persister);
	}
	
	@Override
	public Column getColumn(List<? extends ValueAccessPoint<?>> accessorChain) {
		return criteriaSupport.getRootConfiguration().giveColumn(accessorChain);
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
		// Note that doing this lately (not in constructor) guaranties that it is uptodate because sub entities may have relations which are configured
		// out of constructor by caller
		Set<Table> subTables = subEntitiesPersisters.values().stream().flatMap(p -> p.giveImpliedTables().stream()).collect(Collectors.toSet());
		return Collections.cat(mainPersister.giveImpliedTables(), subTables);
	}
	
	@Override
	public PersisterListenerCollection<C, I> getPersisterListener() {
		return mainPersister.getPersisterListener();
	}
	
	@Override
	public I getId(C entity) {
		return this.mainPersister.getId(entity);
	}
	
	@Override
	public void insert(Iterable<? extends C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(InsertExecutor::insert);
	}
	
	@Override
	public void updateById(Iterable<? extends C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(UpdateExecutor::updateById);
	}
	
	@Override
	public void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		// Below we keep order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
		// impacts but very difficult to measure
		Map<UpdateExecutor<C>, Set<Duo<C, C>>> entitiesPerType = new KeepOrderMap<>();
		differencesIterable.forEach(payload ->
				this.subEntitiesPersisters.values().forEach(persister -> {
					C entity = Objects.preventNull(payload.getLeft(), payload.getRight());
					if (persister.getClassToPersist().isInstance(entity)) {
						entitiesPerType.computeIfAbsent(persister, p -> new KeepOrderSet<>()).add(payload);
					}
				})
		);
		
		entitiesPerType.forEach((updateExecutor, adhocEntities) -> updateExecutor.update(adhocEntities, allColumnsStatement));
	}
	
	@Override
	public Set<C> select(Iterable<I> ids) {
		return selectExecutor.select(ids);
	}
	
	@Override
	public void delete(Iterable<? extends C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(DeleteExecutor::delete);
	}
	
	@Override
	public void deleteById(Iterable<? extends C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(DeleteExecutor::deleteById);
	}
	
	private Map<EntityPersister<C, I>, Set<C>> computeEntitiesPerPersister(Iterable<? extends C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = new KeepOrderMap<>();
		entities.forEach(entity ->
				this.subEntitiesPersisters.values().forEach(persister -> {
					if (persister.getClassToPersist().isInstance(entity)) {
						entitiesPerType.computeIfAbsent(persister, p -> new KeepOrderSet<>()).add(entity);
					}
				})
		);
		return entitiesPerType;
	}
	
	@Override
	public <E, ID> void copyRootJoinsTo(EntityJoinTree<E, ID> entityJoinTree, String joinName) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public void addPersistListener(PersistListener<? extends C> persistListener) {
		subEntitiesPersisters.values().forEach(p -> p.addPersistListener(persistListener));
	}
	
	@Override
	public void addInsertListener(InsertListener<? extends C> insertListener) {
		subEntitiesPersisters.values().forEach(p -> p.addInsertListener(insertListener));
	}
	
	@Override
	public void addUpdateListener(UpdateListener<? extends C> updateListener) {
		subEntitiesPersisters.values().forEach(p -> p.addUpdateListener(updateListener));
	}
	
	@Override
	public void addUpdateByIdListener(UpdateByIdListener<? extends C> updateByIdListener) {
		subEntitiesPersisters.values().forEach(p -> p.addUpdateByIdListener(updateByIdListener));
	}
	
	@Override
	public void addSelectListener(SelectListener<? extends C, I> selectListener) {
		subEntitiesPersisters.values().forEach(p -> p.addSelectListener(selectListener));
	}
	
	@Override
	public void addDeleteListener(DeleteListener<? extends C> deleteListener) {
		subEntitiesPersisters.values().forEach(p -> p.addDeleteListener(deleteListener));
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener<? extends C> deleteListener) {
		subEntitiesPersisters.values().forEach(p -> p.addDeleteByIdListener(deleteListener));
	}
	
	/**
	 * Overridden to capture {@link EntityMapping#addShadowColumnInsert(ShadowColumnValueProvider)} and
	 * {@link EntityMapping#addShadowColumnUpdate(ShadowColumnValueProvider)} (see {@link OneToManyRelationConfigurer})
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
			public void addShadowColumnInsert(ShadowColumnValueProvider<C, T> provider) {
				subEntitiesPersisters.values().forEach(p -> ((EntityMapping) p.getMapping()).addShadowColumnInsert(provider));
			}
			
			@Override
			public void addShadowColumnUpdate(ShadowColumnValueProvider<C, T> provider) {
				subEntitiesPersisters.values().forEach(p -> ((EntityMapping) p.getMapping()).addShadowColumnUpdate(provider));
			}
		};
	}
	
	@Override
	public <SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsOne(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																					 Key<T1, JOINID> leftColumn,
																					 Key<T2, JOINID> rightColumn,
																					 String rightTableAlias,
																					 BeanRelationFixer<SRC, C> beanRelationFixer,
																					 boolean optional,
																					 boolean loadSeparately) {
		
		if (loadSeparately) {
			SingleTableFirstPhaseRelationLoader singleTableFirstPhaseRelationLoader = new SingleTableFirstPhaseRelationLoader(mainPersister.getMapping().getIdMapping(),
					selectExecutor,
					(ThreadLocal<Queue<Set<RelationIds<Object, C, I>>>>) (ThreadLocal) DIFFERED_ENTITY_LOADER,
					discriminatorColumn, subEntitiesPersisters::get);
			String createdJoinNodeName = sourcePersister.getEntityJoinTree().addMergeJoin(ROOT_STRATEGY_NAME,
					singleTableFirstPhaseRelationLoader,
					leftColumn, rightColumn, optional ? JoinType.OUTER : JoinType.INNER);
			
			
			// adding second phase loader
			sourcePersister.addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, DIFFERED_ENTITY_LOADER));
			
			return createdJoinNodeName;
		} else {
			return join(
					sourcePersister.getEntityJoinTree(),
					ROOT_STRATEGY_NAME,
					mainPersister,
					leftColumn,
					rightColumn,
					new HashSet<>(this.subEntitiesPersisters.values()),
					beanRelationFixer,
					polymorphismPolicy,
					(Column<T2, DTYPE>) discriminatorColumn);
		}
	}
	
	@Override
	public <SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsMany(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																							  Key<T1, JOINID> leftColumn,
																							  Key<T2, JOINID> rightColumn,
																							  BeanRelationFixer<SRC, C> beanRelationFixer,
																							  @Nullable BiFunction<Row, ColumnedRow, Object> duplicateIdentifierProvider,
																							  String joinName,
																							  Set<? extends Column<T2, Object>> selectableColumns, boolean optional,
																							  boolean loadSeparately) {
		
		if (loadSeparately) {
			// Subgraph loading is made in 2 phases (load ids, then entities in a second SQL request done by load listener)
			SingleTableFirstPhaseRelationLoader singleTableFirstPhaseRelationLoader = new SingleTableFirstPhaseRelationLoader(mainPersister.getMapping().getIdMapping(),
					selectExecutor,
					(ThreadLocal<Queue<Set<RelationIds<Object, C, I>>>>) (ThreadLocal) DIFFERED_ENTITY_LOADER,
					discriminatorColumn, subEntitiesPersisters::get);
			String createdJoinNodeName = sourcePersister.getEntityJoinTree().addMergeJoin(joinName,
					singleTableFirstPhaseRelationLoader,
					leftColumn, rightColumn, JoinType.OUTER);
			
			// adding second phase loader
			sourcePersister.addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, DIFFERED_ENTITY_LOADER));
			
			return createdJoinNodeName;
		} else {
			return join(
					sourcePersister.getEntityJoinTree(),
					joinName,
					mainPersister,
					leftColumn,
					rightColumn,
					new HashSet<>(this.subEntitiesPersisters.values()),
					beanRelationFixer,
					polymorphismPolicy,
					(Column<T2, DTYPE>) discriminatorColumn);
		}
	}
	
	private <MAINTABLE extends Table<MAINTABLE>, SUBTABLE extends Table<SUBTABLE>, JOINID> KeyBuilder<SUBTABLE, Object>
	projectPrimaryKey(Key<MAINTABLE, JOINID> rightColumn, ConfiguredRelationalPersister<? extends C, I> subPersister) {
		EntityMapping<? extends C, I, SUBTABLE> subTypeMapping = subPersister.getMapping();
		KeyBuilder<SUBTABLE, Object> reverseKey = Key.from(subTypeMapping.getTargetTable());
		rightColumn.getColumns().forEach(col -> {
			Column<SUBTABLE, ?> column = subTypeMapping.getTargetTable().addColumn(col.getExpression(), col.getJavaType());
			subTypeMapping.addShadowColumnSelect(column);
			reverseKey.addColumn(column);
		});
		return reverseKey;
	}
	
	private <SRC, SRCID, U extends C, T1 extends Table<T1>, T2 extends Table<T2>, ID, JOINCOLTYPE> String join(
			EntityJoinTree<SRC, SRCID> entityJoinTree,
			String leftStrategyName,
			ConfiguredRelationalPersister<U, ID> mainPersister,
			Key<T1, JOINCOLTYPE> leftJoinColumn,
			Key<T2, JOINCOLTYPE> rightJoinColumn,
			Set<ConfiguredRelationalPersister<? extends U, ID>> subPersisters,
			BeanRelationFixer<SRC, U> beanRelationFixer,
			SingleTablePolymorphism<U, DTYPE> polymorphismPolicy,
			Column<T2, DTYPE> discriminatorColumn) {
		
		Holder<SingleTablePolymorphicRelationJoinNode<U, T1, T2, JOINCOLTYPE, ID, DTYPE>> createdJoinHolder = new Holder<>();
		String relationJoinName = entityJoinTree.addJoin(leftStrategyName, parent -> {
			SingleTablePolymorphicRelationJoinNode<U, T1, T2, JOINCOLTYPE, ID, DTYPE> polymorphicRelationJoinNode = new SingleTablePolymorphicRelationJoinNode<U, T1, T2, JOINCOLTYPE, ID, DTYPE>(
					(JoinNode<T1>) (JoinNode) parent,
					leftJoinColumn,
					rightJoinColumn,
					JoinType.OUTER,
					mainPersister.getMainTable().getColumns(),
					null,
					new EntityMappingAdapter<>(mainPersister.<T>getMapping()),
					(BeanRelationFixer<Object, U>) beanRelationFixer,
					discriminatorColumn) {
				
				@Override
				public SingleTablePolymorphicRelationJoinNode<U, T1, T2, JOINCOLTYPE, ID, DTYPE>.SingleTablePolymorphicRelationJoinRowConsumer toConsumer(ColumnedRow columnedRow) {
					SingleTablePolymorphicRelationJoinNode<U, T1, T2, JOINCOLTYPE, ID, DTYPE>.SingleTablePolymorphicRelationJoinRowConsumer joinTablePolymorphicRelationJoinRowConsumer = super.toConsumer(columnedRow);
					addSingleTableSubPersistersJoin(mainPersister, this, subPersisters, columnedRow, polymorphismPolicy);
					return joinTablePolymorphicRelationJoinRowConsumer;
				}
			};
			createdJoinHolder.set(polymorphicRelationJoinNode);
			return polymorphicRelationJoinNode;
		});
		
		return relationJoinName;
	}
	
	private <U, V extends U, T1 extends Table<T1>, T2 extends Table<T2>, JOINCOLTYPE, ID> void addSingleTableSubPersistersJoin(
			ConfiguredRelationalPersister<U, ID> mainPersister,
			SingleTablePolymorphicRelationJoinNode<U, T1, T2, JOINCOLTYPE, ID, DTYPE> mainPersisterJoin,
			Set<ConfiguredRelationalPersister<? extends U, ID>> subPersisters,
			ColumnedRow columnedRow,
			SingleTablePolymorphism<U, DTYPE> polymorphismPolicy) {
		
		subPersisters.forEach(subPersister -> {
			ConfiguredRelationalPersister<V, ID> localSubPersister = (ConfiguredRelationalPersister<V, ID>) subPersister;
			PolymorphicMergeJoinRowConsumer<U, V, ID> joinRowConsumer = new PolymorphicMergeJoinRowConsumer<U, V, ID>(
					new PolymorphicEntityInflater<>(mainPersister, localSubPersister), columnedRow);
			mainPersisterJoin.addSubPersisterJoin(localSubPersister, joinRowConsumer, polymorphismPolicy.getDiscriminatorValue(localSubPersister.getClassToPersist()));
		});
	}
	
	private class SingleTableFirstPhaseRelationLoader extends FirstPhaseRelationLoader<C, I> {
		private final Column<T, DTYPE> discriminatorColumn;
		private final Function<Class, SelectExecutor> subtypeSelectors;
		private final Set<DTYPE> discriminatorValues;
		
		private SingleTableFirstPhaseRelationLoader(IdMapping<C, I> subEntityIdMapping,
													SingleTablePolymorphismSelectExecutor<C, I, T, DTYPE> selectExecutor,
													ThreadLocal<Queue<Set<RelationIds<Object, C, I>>>> relationIdsHolder,
													Column<T, DTYPE> discriminatorColumn, Function<Class, SelectExecutor> subtypeSelectors) {
			// Note that selectExecutor won't be used because we dynamically lookup for it in fillCurrentRelationIds
			super(subEntityIdMapping, selectExecutor, relationIdsHolder);
			this.discriminatorColumn = discriminatorColumn;
			this.subtypeSelectors = subtypeSelectors;
			this.discriminatorValues = Iterables.collect(polymorphismPolicy.getSubClasses(), conf -> polymorphismPolicy.getDiscriminatorValue(conf.getEntityType()), HashSet::new);
		}
		
		@Override
		protected void fillCurrentRelationIds(Row row, Object bean, ColumnedRow columnedRow) {
			DTYPE discriminator = columnedRow.getValue(discriminatorColumn, row);
			// we avoid NPE on polymorphismPolicy.getClass(discriminator) caused by null discriminator in case of empty relation
			// by only treating known discriminator values (preferred way to check against null because type can be primitive one)
			if (discriminatorValues.contains(discriminator)) {
				Set<RelationIds<Object, C, I>> relationIds = relationIdsHolder.get().peek();
				I id = idMapping.getIdentifierAssembler().assemble(row, columnedRow);
				relationIds.add(new RelationIds<>(giveSelector(discriminator), idMapping.getIdAccessor()::getId, bean, id));
			}
		}
		
		@Override
		public Set<Selectable<?>> getSelectableColumns() {
			Set<Selectable<?>> result = new HashSet<>(idMapping.getIdentifierAssembler().getColumns());
			result.add((Selectable) discriminatorColumn);
			return result;
		}
		
		private SelectExecutor giveSelector(DTYPE discriminator) {
			return subtypeSelectors.apply(polymorphismPolicy.getClass(discriminator));
		}
	}
}
