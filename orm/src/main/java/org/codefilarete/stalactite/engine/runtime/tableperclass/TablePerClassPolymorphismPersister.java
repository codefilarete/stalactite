package org.codefilarete.stalactite.engine.runtime.tableperclass;

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

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.DeleteExecutor;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.InsertExecutor;
import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.UpdateExecutor;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelationConfigurer;
import org.codefilarete.stalactite.engine.runtime.AbstractPolymorphismPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.EntityMappingWrapper;
import org.codefilarete.stalactite.engine.runtime.EntityQueryCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.FirstPhaseRelationLoader;
import org.codefilarete.stalactite.engine.runtime.PersisterWrapper;
import org.codefilarete.stalactite.engine.runtime.PolymorphicPersister;
import org.codefilarete.stalactite.engine.runtime.RelationIds;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.SecondPhaseRelationLoader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.PolymorphicMergeJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.load.TablePerClassPolymorphicRelationJoinNode;
import org.codefilarete.stalactite.mapping.AccessorWrapperIdAccessor;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.Mapping.ShadowColumnValueProvider;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryStatement.PseudoColumn;
import org.codefilarete.stalactite.query.model.QueryStatement.PseudoTable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Selectable.SelectableString;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.query.model.operator.TupleIn;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.trace.MutableInt;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_STRATEGY_NAME;
import static org.codefilarete.stalactite.query.model.Operators.cast;

/**
 * @author Guillaume Mary
 */
public class TablePerClassPolymorphismPersister<C, I, T extends Table<T>> extends AbstractPolymorphismPersister<C, I> {
	
	@SuppressWarnings("java:S5164" /* remove() is called by SecondPhaseRelationLoader.afterSelect() */)
	private static final ThreadLocal<Queue<Set<RelationIds<Object /* E */, Object /* target */, Object /* target identifier */>>>> DIFFERED_ENTITY_LOADER = new ThreadLocal<>();
	
	private final Map<Class<? extends C>, UpdateExecutor<? extends C>> subclassUpdateExecutors;
	
	public TablePerClassPolymorphismPersister(ConfiguredRelationalPersister<C, I> mainPersister,
											  Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> subEntitiesPersisters,
											  ConnectionProvider connectionProvider,
											  Dialect dialect) {
		super(mainPersister,
				subEntitiesPersisters,
				new TablePerClassPolymorphismEntityFinder<C, I, T>(
						mainPersister,
						subEntitiesPersisters,
						connectionProvider,
						dialect));
		// Below we keep order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
		// impacts but very difficult to measure
		this.subclassUpdateExecutors = Iterables.map(subEntitiesPersisters.entrySet(), Entry::getKey, Entry::getValue, KeepOrderMap::new);
		
		this.subEntitiesPersisters.forEach((type, persister) ->
				mainPersister.getEntityJoinTree().projectTo(persister.getEntityJoinTree(), ROOT_STRATEGY_NAME)
		);
		
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
		// Note that this implementation can't be tested yet because we don't yet support table-per-class polymorphism
		// combined with other polymorphism types. It has only been implemented in order to not forget this behavior for the day of the polymorphism
		// combination has come
		Set<Class<? extends C>> result = new HashSet<>();
		this.subEntitiesPersisters.forEach((c, p) -> {
			if (p instanceof PolymorphicPersister) {
				result.addAll((Collection) ((PolymorphicPersister<?>) p).getSupportedEntityTypes());
			} else if (p instanceof PersisterWrapper && ((PersisterWrapper<C, I>) p).getDeepestDelegate() instanceof PolymorphicPersister) {
				result.addAll(((PolymorphicPersister) ((PersisterWrapper) p).getDeepestDelegate()).getSupportedEntityTypes());
			} else {
				result.add(c);
			}
		});
		return result;
	}
	
	@Override
	public Collection<Table<?>> giveImpliedTables() {
		// in table-per-class main persister table does not participate in database schema : only sub entities persisters do
		return subEntitiesPersisters.values().stream().flatMap(p -> p.giveImpliedTables().stream()).collect(Collectors.toList());
	}
	
	@Override
	public void doInsert(Iterable<? extends C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(InsertExecutor::insert);
	}
	
	@Override
	public void doUpdateById(Iterable<? extends C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(UpdateExecutor::updateById);
	}
	
	@Override
	public void doUpdate(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
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
		
//		updateSubEntities(differencesIterable, allColumnsStatement);
	}
	
	/* Extracted from update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement)
	 * to deal with generics "? extends C" of subclassUpdateExecutors (partly) 
	 */
	private <D extends C> void updateSubEntities(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		// Below we keep order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
		// impacts but very difficult to measure
		Map<Class<D>, Set<Duo<D, D>>> entitiesPerType = new KeepOrderMap<>();
		differencesIterable.forEach(payload -> {
			C entity = Objects.preventNull(payload.getLeft(), payload.getRight());
			entitiesPerType.computeIfAbsent((Class<D>) entity.getClass(), k -> new KeepOrderSet<>()).add((Duo<D, D>) payload);
		});
		subclassUpdateExecutors.forEach((subclass, updateExecutor) -> {
			Set<? extends Duo<D, D>> entitiesToUpdate = entitiesPerType.get(subclass);
			if (entitiesToUpdate != null) {
				((UpdateExecutor<D>) updateExecutor).update(entitiesToUpdate, allColumnsStatement);
			}
		});
	}
	
	@Override
	public void doDelete(Iterable<? extends C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(DeleteExecutor::delete);
	}
	
	@Override
	public void doDeleteById(Iterable<? extends C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(DeleteExecutor::deleteById);
	}
	
	private <D extends C> Map<EntityPersister<D, I>, Set<D>> computeEntitiesPerPersister(Iterable<? extends C> entities) {
		// Below we keep order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
		// impacts but very difficult to measure
		Map<EntityPersister<D, I>, Set<D>> entitiesPerType = new KeepOrderMap<>();
		entities.forEach(entity ->
				this.subEntitiesPersisters.values().forEach(persister -> {
					if (persister.getClassToPersist().isInstance(entity)) {
						entitiesPerType.computeIfAbsent((EntityPersister<D, I>) persister, p -> new KeepOrderSet<>()).add((D) entity);
					}
				})
		);
		return entitiesPerType;
	}
	
	@Override
	public <E, ID> void copyRootJoinsTo(EntityJoinTree<E, ID> entityJoinTree, String joinName) {
		getEntityJoinTree().projectTo(getEntityJoinTree(), joinName);
	}
	
	/**
	 * Overridden to capture {@link EntityMapping#addShadowColumnInsert(ShadowColumnValueProvider)} and
	 * {@link EntityMapping#addShadowColumnUpdate(ShadowColumnValueProvider)} (see {@link OneToManyRelationConfigurer})
	 * Made to dispatch those methods subclass strategies since their persisters are in charge of managing their entities (not the parent one).
	 * <p>
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
				Collection<? extends ConfiguredRelationalPersister<C, I>> subPersisters = subEntitiesPersisters.values();
				subPersisters.forEach(persister -> persister.getMapping().addTransformerListener(listener));
			}
			
			@Override
			public void addShadowColumnInsert(ShadowColumnValueProvider<C, T> provider) {
				subEntitiesPersisters.values().forEach(p -> {
					p.<Table>getMapping().addShadowColumnInsert(projectShadowColumnProvider(provider, p));
				});
			}
			
			@Override
			public void addShadowColumnUpdate(ShadowColumnValueProvider<C, T> provider) {
				subEntitiesPersisters.values().forEach(p -> {
					p.<Table>getMapping().addShadowColumnUpdate(projectShadowColumnProvider(provider, p));
				});
			}
			
			private <D extends C, SUBENTITYTABLE extends Table<SUBENTITYTABLE>>
			ShadowColumnValueProvider<D, SUBENTITYTABLE>
			projectShadowColumnProvider(ShadowColumnValueProvider<C, T> provider, ConfiguredRelationalPersister<D, I> subEntityPersister) {
				
				Map<Column<T, ?>, Column<SUBENTITYTABLE, ?>> projectedColumnMap = new HashMap<>(provider.getColumns().size());
				provider.getColumns().forEach(c -> {
					Column<SUBENTITYTABLE, ?> projectedColumn = subEntityPersister.<SUBENTITYTABLE>getMapping().getTargetTable().addColumn(c.getName(), c.getJavaType(), c.getSize());
					projectedColumn.nullable(c.isNullable());
					projectedColumnMap.put(c, projectedColumn);
				});
				return new ShadowColumnValueProvider<D, SUBENTITYTABLE>() {
					
					private final Set<Column<SUBENTITYTABLE, ?>> values = new HashSet<>(projectedColumnMap.values());
					
					@Override
					public Set<Column<SUBENTITYTABLE, ?>> getColumns() {
						return values;
					}
					
					@Override
					public Map<Column<SUBENTITYTABLE, ?>, ?> giveValue(D bean) {
						Map<Column<T, ?>, ?> columnObjectMap = provider.giveValue(bean);
						return Maps.innerJoin(projectedColumnMap, columnObjectMap);
					}
				};
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
			String createdJoinNodeName = this.joinAsOneWithSeparateLoading(sourcePersister.getEntityJoinTree(), ROOT_STRATEGY_NAME,
					leftColumn,
					rightColumn,
					(Set<? extends ConfiguredRelationalPersister<C, I>>) (Set) new HashSet<>(this.subEntitiesPersisters.values()));

			sourcePersister.addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, DIFFERED_ENTITY_LOADER));
			
			return createdJoinNodeName;
		} else {
			return join(
					sourcePersister.getEntityJoinTree(),
					ROOT_STRATEGY_NAME,
					leftColumn,
					rightColumn,
					beanRelationFixer);
		}
	}
	
	@Override
	public <SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsMany(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																							  Key<T1, JOINID> leftColumn,
																							  Key<T2, JOINID> rightColumn,
																							  BeanRelationFixer<SRC, C> beanRelationFixer,
																							  @Nullable BiFunction<Row, ColumnedRow, Object> duplicateIdentifierProvider,
																							  String joinName,
																							  Set<? extends Column<T2, ?>> selectableColumns,
																							  boolean optional,
																							  boolean loadSeparately) {
		
		PrimaryKey<T, ?> mainTablePK = mainPersister.<T>getMapping().getTargetTable().getPrimaryKey();
		Map<ConfiguredRelationalPersister, Key> joinColumnPerSubPersister = new HashMap<>();
		if (rightColumn.equals(mainTablePK)) {
			// join is made on primary key => case is association table
			subEntitiesPersisters.forEach((c, subPersister) -> {
				joinColumnPerSubPersister.put(subPersister, subPersister.getMapping().getTargetTable().getPrimaryKey());
			});
		} else {
			// join is made on a foreign key => case of relation owned by reverse side
			subEntitiesPersisters.forEach((c, subPersister) -> {
				KeyBuilder<?, Object> reverseKey = projectPrimaryKey(rightColumn, subPersister);
				joinColumnPerSubPersister.put(subPersister, reverseKey.build());
			});
		}
		
		if (loadSeparately) {
			// TODO: simplify query : it joins on target table as many as subentities which can be reduced to one join if FirstPhaseRelationLoader
			//  can compute discriminatorValue 
			subEntitiesPersisters.forEach((c, subPersister) -> {
				sourcePersister.getEntityJoinTree().addMergeJoin(
						joinName,
						// need to be cast to C instead of "? extends C" because selectExecutor is C-typed
						new FirstPhaseRelationLoader<>(subPersister.getMapping().getIdMapping(), this, (ThreadLocal) DIFFERED_ENTITY_LOADER),
						leftColumn,
						joinColumnPerSubPersister.get(subPersister),
						JoinType.OUTER);
			});
			
			// adding second phase loader
			sourcePersister.addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, DIFFERED_ENTITY_LOADER));
			
			// FIXME : we shouldn't return null here but a created join node name: which one since we have several table to join ? see joinAsOne(..) maybe ?
			return null;
		} else {
			return join(
					sourcePersister.getEntityJoinTree(),
					joinName,
					leftColumn,
					rightColumn,
					beanRelationFixer);
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
	
	private <SRC, SRCID, T1 extends Table<T1>, T2 extends Table<T2>, JOINCOLTYPE> String join(
			EntityJoinTree<SRC, SRCID> entityJoinTree,
			String leftStrategyName,
			Key<T1, JOINCOLTYPE> leftJoinColumn,
			Key<T2, JOINCOLTYPE> rightJoinColumn,
			BeanRelationFixer<SRC, C> beanRelationFixer) {
		
		
		// we build a union of all sub queries that will be joined in the main query
		// To build the union we need columns that are common to all persisters
		Set<JoinLink<?, ?>> commonColumns = new KeepOrderSet<>();
		commonColumns.addAll(mainPersister.getMapping().getSelectableColumns());
		// TODO : right column is not in selected columns of class mapping : understand why (and if that's normal)
		commonColumns.addAll(rightJoinColumn.getColumns());
		
		Set<String> commonColumnsNames = commonColumns.stream().map(JoinLink::getExpression).collect(Collectors.toSet());
		
		Set<ConfiguredRelationalPersister<? extends C, I>> subPersisters = new HashSet<>(this.subEntitiesPersisters.values());
		
		KeepOrderSet<Column<?, ?>> nonCommonColumns = new KeepOrderSet<>();
		subPersisters.forEach(subPersister -> {
			nonCommonColumns.addAll(subPersister.getMainTable().getColumns());
		});
		nonCommonColumns.removeIf(c -> commonColumnsNames.contains(c.getName()));
		
		Union subPersistersUnion = new Union();
		String entityTypeDiscriminatorName = "clazz_";
		PseudoColumn<Integer> discriminatorPseudoColumn = subPersistersUnion.registerColumn(entityTypeDiscriminatorName, Integer.class);
		MutableInt discriminatorComputer = new MutableInt();
		
		subPersisters.forEach(subPersister -> {
			Query subEntityQuery = new Query(subPersister.getMapping().getTargetTable());
			subEntityQuery.select(String.valueOf(discriminatorComputer.increment()), Integer.class).as(entityTypeDiscriminatorName);
			subPersistersUnion.unionAll(subEntityQuery);
			
			commonColumns.forEach(column -> {
				subEntityQuery.select(column.getExpression(), column.getJavaType());
				subPersistersUnion.registerColumn(column.getExpression(), column.getJavaType());
			});
			
			nonCommonColumns.forEach(column -> {
				Selectable<?> expression;
				if (subPersister.getMapping().getSelectableColumns().contains(column)) {
					expression = new SelectableString<>(column.getName(), column.getJavaType());
				} else {
					expression = cast((String) null, column.getJavaType());
				}
				// we put an alias else cast(..) as no name which makes it doesn't match official-column name, and then
				// may cause an error since SQL in kind of invalid 
				subEntityQuery.select(expression, column.getName());
				subPersistersUnion.registerColumn(column.getName(), column.getJavaType());
			});
		});
		
		Holder<TablePerClassPolymorphicRelationJoinNode<C, T1, JOINCOLTYPE, I>> createdJoinHolder = new Holder<>();
		String relationJoinName = entityJoinTree.addJoin(leftStrategyName, parent -> {
			TablePerClassPolymorphicRelationJoinNode<C, T1, JOINCOLTYPE, I> relationJoinNode = new TablePerClassPolymorphicRelationJoinNode<>(
					(JoinNode<T1>) (JoinNode) parent,
					subPersistersUnion,
					leftJoinColumn,
					rightJoinColumn,
					JoinType.OUTER,
					subPersistersUnion.getColumns(),
					mainPersister.getClassToPersist().getSimpleName(),
					new EntityMappingAdapter<>(mainPersister.<T1>getMapping()),
					(BeanRelationFixer<Object, C>) beanRelationFixer,
					discriminatorPseudoColumn);
			createdJoinHolder.set(relationJoinNode);
			return relationJoinNode;
		});
		
		this.addTablePerClassPolymorphicSubPersistersJoins(entityJoinTree, relationJoinName, createdJoinHolder.get(), subPersisters);
		
		return relationJoinName;
	}
	
	private <SRC, SRCID, V extends C, T1 extends Table<T1>, T2 extends Table<T2>> void addTablePerClassPolymorphicSubPersistersJoins(
			EntityJoinTree<SRC, SRCID> entityJoinTree,
			String mainPolymorphicJoinNodeName,
			TablePerClassPolymorphicRelationJoinNode<C, T1, ?, I> mainPersisterJoin,
			Set<ConfiguredRelationalPersister<? extends C, I>> subPersisters) {
		
		MutableInt discriminatorComputer = new MutableInt();
		subPersisters.forEach(subPersister -> {
			ConfiguredRelationalPersister<V, I> localSubPersister = (ConfiguredRelationalPersister<V, I>) subPersister;
			entityJoinTree.<V, T1, T2, I>addMergeJoin(mainPolymorphicJoinNodeName,
					new EntityMergerAdapter<>(localSubPersister.<T2>getMapping()),
					mainPersister.<T1>getMainTable().getPrimaryKey(),
					subPersister.<T2>getMainTable().getPrimaryKey(),
					JoinType.OUTER,
					columnedRow -> {
						PolymorphicMergeJoinRowConsumer<V, I> joinRowConsumer = new PolymorphicMergeJoinRowConsumer<>(
								localSubPersister.<T2>getMapping(), columnedRow);
						mainPersisterJoin.addSubPersisterJoin(joinRowConsumer, discriminatorComputer.increment());
						return joinRowConsumer;
					}); 
		});
	}
	
	/**
	 * Makes a join between source entity tree and a lite union of all sub-persisters table in order to get target
	 * entities ids in a first query. Ids will be kept in a {@link ThreadLocal} to fully load target entities in
	 * a separate query (see caller).
	 */
	private <SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsOneWithSeparateLoading(
			EntityJoinTree<SRC, SRCID> entityJoinTree,
			String leftStrategyName,
			Key<T1, JOINID> leftJoinColumn,
			Key<T2, JOINID> rightJoinColumn,
			Set<? extends ConfiguredRelationalPersister<C, I>> subPersisters) {
		
		Union subPersistersUnion = new Union();
		// Union will contain only 3 columns :
		// - discriminator
		// - entity primary key
		// - join column
		String entityTypeDiscriminatorName = "clazz_";
		PseudoColumn<Integer> discriminatorPseudoColumn = subPersistersUnion.registerColumn(entityTypeDiscriminatorName, Integer.class);
		
		PrimaryKey<T, I> primaryKey = mainPersister.<T>getMainTable().getPrimaryKey();
		primaryKey.getColumns().forEach(col -> {
			PseudoColumn<I> primaryKeyPseudoColumn = subPersistersUnion.registerColumn(col.getName(), mainPersister.getMapping().getIdMapping().getIdentifierInsertionManager().getIdentifierType());
		});
		
		MutableInt discriminatorComputer = new MutableInt();
		Map<Integer, SelectExecutor<? extends C, I>> subtypeSelectorPerDiscriminatorValue = new HashMap<>();
		
		subPersisters.forEach(subPersister -> {
			Query subEntityQuery = new Query(subPersister.getMapping().getTargetTable());
			int discriminatorValue = discriminatorComputer.increment();
			subtypeSelectorPerDiscriminatorValue.put(discriminatorValue, subPersister);
			subEntityQuery.select(String.valueOf(discriminatorValue), Integer.class).as(entityTypeDiscriminatorName);
			subPersistersUnion.unionAll(subEntityQuery);
			
			rightJoinColumn.getColumns().forEach(column -> {
				subEntityQuery.select(column.getExpression(), column.getJavaType());
			});
			
			mainPersister.getMapping().getTargetTable().getPrimaryKey().getColumns().forEach(column -> {
				subEntityQuery.select(column.getName(), column.getJavaType());
			});
		});
		
		IdMapping<C, I> idMapping = mainPersister.getMapping().getIdMapping();
		TablePerClassFirstPhaseRelationLoader tablePerClassFirstPhaseRelationLoader = new TablePerClassFirstPhaseRelationLoader(
				idMapping,
				this,
				(ThreadLocal<Queue<Set<RelationIds<Object, C, I>>>>) (ThreadLocal) DIFFERED_ENTITY_LOADER,
				subtypeSelectorPerDiscriminatorValue,
				discriminatorPseudoColumn
		);
		
		// We take right join column from "union all" query : actually it must be taken from union made as a pseudo table
		// because taking it directly from Union is incorrect since a Union doesn't implement Fromable (because it hasn't
		// any alias)
		PseudoTable pseudoTable = subPersistersUnion.asPseudoTable("unioned_" + mainPersister.getClassToPersist().getSimpleName());
		KeyBuilder<Fromable, JOINID> keyBuilder = Key.from(pseudoTable);
		rightJoinColumn.getColumns().forEach(column -> {
			keyBuilder.addColumn(subPersistersUnion.registerColumn(column.getExpression(), column.getJavaType()));
		});
		
		Key<Fromable, JOINID> rightJoinLink = keyBuilder.build();
		
		return entityJoinTree.addMergeJoin(leftStrategyName,
				tablePerClassFirstPhaseRelationLoader,
				leftJoinColumn,
				rightJoinLink,
				JoinType.OUTER);
	}
	
	private class TablePerClassFirstPhaseRelationLoader extends FirstPhaseRelationLoader<C, I> {
		
		private final PseudoColumn<Integer> discriminatorColumn;
		private final Map<Integer, SelectExecutor<? extends C, I>> subtypeSelectorPerDiscriminatorValue;
		
		private TablePerClassFirstPhaseRelationLoader(IdMapping<C, I> subEntityIdMapping,
													  SelectExecutor<C, I> selectExecutor,
													  ThreadLocal<Queue<Set<RelationIds<Object, C, I>>>> relationIdsHolder,
													  Map<Integer, SelectExecutor<? extends C, I>> subtypeSelectorPerDiscriminatorValue,
													  PseudoColumn<Integer> discriminatorColumn) {
			// Note that selectExecutor won't be used because we dynamically lookup for it in fillCurrentRelationIds
			super(subEntityIdMapping, selectExecutor, relationIdsHolder);
			this.discriminatorColumn = discriminatorColumn;
			this.subtypeSelectorPerDiscriminatorValue = subtypeSelectorPerDiscriminatorValue;
		}
		
		@Override
		protected void fillCurrentRelationIds(Row row, C bean, ColumnedRow columnedRow) {
			Integer discriminator = columnedRow.getValue(discriminatorColumn, row);
			// we avoid NPE on polymorphismPolicy.getClass(discriminator) caused by null discriminator in case of empty relation
			// by only treating known discriminator values (preferred way to check against null because type can be primitive)
			SelectExecutor<? extends C, I> subSelectExecutor = subtypeSelectorPerDiscriminatorValue.get(discriminator);
			if (subSelectExecutor != null) {
				I id = idMapping.getIdentifierAssembler().assemble(row, columnedRow);
				addToCurrentIdsHolder(bean, subSelectExecutor, id);
			}
		}
		
		private <D extends C> void addToCurrentIdsHolder(C bean, SelectExecutor<D, I> subSelectExecutor, I id) {
			Set<RelationIds<Object, C, I>> relationIds = relationIdsHolder.get().peek();
			RelationIds<C, D, I> e = new RelationIds<>(subSelectExecutor, idMapping.getIdAccessor()::getId, bean, id);
			relationIds.add((RelationIds<Object, C, I>) e);
		}
		
		@Override
		public Set<Selectable<?>> getSelectableColumns() {
			Set<Selectable<?>> result = new HashSet<>(idMapping.getIdentifierAssembler().getColumns());
			result.add(discriminatorColumn);
			return result;
		}
	}
}
