package org.codefilarete.stalactite.engine.runtime;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.stalactite.engine.DeleteExecutor;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.ExecutableQuery;
import org.codefilarete.stalactite.engine.InsertExecutor;
import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.UpdateExecutor;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelationConfigurer;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersistListener;
import org.codefilarete.stalactite.engine.listener.PersisterListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateByIdListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister.CriteriaProvider;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.PolymorphicEntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.PolymorphicMergeJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.load.TablePerClassPolymorphicRelationJoinNode;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.Mapping.ShadowColumnValueProvider;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.query.RelationalEntityCriteria;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Selectable.SelectableString;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.query.model.Union.PseudoColumn;
import org.codefilarete.stalactite.query.model.Union.UnionInFrom;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.function.Functions;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.trace.ModifiableInt;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_STRATEGY_NAME;
import static org.codefilarete.stalactite.query.model.Operators.cast;

/**
 * @author Guillaume Mary
 */
public class TablePerClassPolymorphismPersister<C, I, T extends Table<T>> implements ConfiguredRelationalPersister<C, I>, PolymorphicPersister<C> {
	
	@SuppressWarnings("java:S5164" /* remove() is called by SecondPhaseRelationLoader.afterSelect() */)
	private static final ThreadLocal<Queue<Set<RelationIds<Object /* E */, Object /* target */, Object /* target identifier */>>>> DIFFERED_ENTITY_LOADER = new ThreadLocal<>();
	
	private final ConfiguredRelationalPersister<C, I> mainPersister;
	private final Map<Class<? extends C>, UpdateExecutor<? extends C>> subclassUpdateExecutors;
	private final Map<Class<? extends C>, ? extends ConfiguredRelationalPersister<C, I>> subEntitiesPersisters;
	private final TablePerClassPolymorphicSelectExecutor<C, I, ?> selectExecutor;
	private final TablePerClassPolymorphismEntitySelectExecutor<C, I, ?> entitySelectExecutor;
	private final EntityCriteriaSupport<C> criteriaSupport;
	
	public TablePerClassPolymorphismPersister(ConfiguredRelationalPersister<C, I> mainPersister,
											  Map<Class<? extends C>, SimpleRelationalEntityPersister<? extends C, I, ?>> subEntitiesPersisters,
											  ConnectionProvider connectionProvider,
											  Dialect dialect) {
		this.mainPersister = mainPersister;
		Set<Entry<Class<? extends C>, SimpleRelationalEntityPersister<? extends C, I, ?>>> entries = subEntitiesPersisters.entrySet();
		this.subclassUpdateExecutors = Iterables.map(entries, Entry::getKey, Entry::getValue, KeepOrderMap::new);
		
		Map<Class<? extends C>, Table> tablePerSubEntity = new HashMap<>(Iterables.map(entries,
				Entry::getKey,
				Functions.<Entry<Class<? extends C>, SimpleRelationalEntityPersister<? extends C, I, ?>>, SimpleRelationalEntityPersister<? extends C, I, ?>, Table>
						chain(Entry::getValue, SimpleRelationalEntityPersister::getMainTable)));
		
		this.subEntitiesPersisters = (Map) subEntitiesPersisters;
		this.subEntitiesPersisters.forEach((type, persister) ->
				mainPersister.getEntityJoinTree().projectTo(persister.getEntityJoinTree(), ROOT_STRATEGY_NAME)
		);
		
		this.selectExecutor = new TablePerClassPolymorphicSelectExecutor<>(
				mainPersister,
				tablePerSubEntity,
				subEntitiesPersisters,
				connectionProvider,
				dialect);
		
		this.entitySelectExecutor = new TablePerClassPolymorphismEntitySelectExecutor<>(
				mainPersister.getMapping().getIdMapping().getIdentifierAssembler(),
				tablePerSubEntity,
				subEntitiesPersisters,
				mainPersister.getMapping().getTargetTable(),
				connectionProvider,
				dialect);
		
		this.criteriaSupport = new EntityCriteriaSupport<>(mainPersister.getMapping());
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
		return subEntitiesPersisters.values().stream().flatMap(p -> p.giveImpliedTables().stream()).collect(Collectors.toList());
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
		updateSubEntities(differencesIterable, allColumnsStatement);
	}
	
	/* Extracted from update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement)
	 * to deal with generics "? extends C" of subclassUpdateExecutors (partly) 
	 */
	private <D extends C> void updateSubEntities(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		Map<Class<D>, Set<Duo<D, D>>> entitiesPerType = new HashMap<>();
		differencesIterable.forEach(payload -> {
			C entity = Objects.preventNull(payload.getLeft(), payload.getRight());
			entitiesPerType.computeIfAbsent((Class<D>) entity.getClass(), k -> new HashSet<>()).add((Duo<D, D>) payload);
		});
		subclassUpdateExecutors.forEach((subclass, updateExecutor) -> {
			Set<? extends Duo<D, D>> entitiesToUpdate = entitiesPerType.get(subclass);
			if (entitiesToUpdate != null) {
				((UpdateExecutor<D>) updateExecutor).update(entitiesToUpdate, allColumnsStatement);
			}
		});
	}
	
	@Override
	public Set<C> select(Iterable<I> ids) {
		// Note that executor emits select listener events
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
	
	@Override
	public void persist(Iterable<? extends C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(EntityPersister::persist);
	}
	
	private <D extends C> Map<EntityPersister<D, I>, Set<D>> computeEntitiesPerPersister(Iterable<? extends C> entities) {
		Map<EntityPersister<D, I>, Set<D>> entitiesPerType = new KeepOrderMap<>();
		entities.forEach(entity ->
				this.subEntitiesPersisters.values().forEach(persister -> {
					if (persister.getClassToPersist().isInstance(entity)) {
						entitiesPerType.computeIfAbsent((EntityPersister<D, I>) persister, p -> new HashSet<>()).add((D) entity);
					}
				})
		);
		return entitiesPerType;
	}
	
	@Override
	public <O> RelationalExecutableEntityQuery<C> selectWhere(SerializableFunction<C, O> getter, ConditionalOperator<O> operator) {
		EntityCriteriaSupport<C> localCriteriaSupport = newWhere();
		localCriteriaSupport.and(getter, operator);
		return wrapIntoExecutable(localCriteriaSupport);
	}
	
	@Override
	public <O> RelationalExecutableEntityQuery<C> selectWhere(SerializableBiConsumer<C, O> setter, ConditionalOperator<O> operator) {
		EntityCriteriaSupport<C> localCriteriaSupport = newWhere();
		localCriteriaSupport.and(setter, operator);
		return wrapIntoExecutable(localCriteriaSupport);
	}
	
	@Override
	public <O> ExecutableEntityQuery<C> selectWhere(AccessorChain<C, O> accessorChain, ConditionalOperator<O> operator) {
		EntityCriteriaSupport<C> localCriteriaSupport = newWhere();
		localCriteriaSupport.and(accessorChain, operator);
		return wrapIntoExecutable(localCriteriaSupport);
	}
	
	private RelationalExecutableEntityQuery<C> wrapIntoExecutable(EntityCriteriaSupport<C> localCriteriaSupport) {
		MethodReferenceDispatcher methodDispatcher = new MethodReferenceDispatcher();
		return methodDispatcher
				.redirect((SerializableBiFunction<ExecutableQuery<C>, Accumulator<C, ?, Set<C>>, Set<C>>) ExecutableQuery::execute,
						(Accumulator<C, ?, Set<C>> accumulator) -> entitySelectExecutor.loadGraph(localCriteriaSupport.getCriteria()))
				.redirect(CriteriaProvider::getCriteria, localCriteriaSupport::getCriteria)
				.redirect(RelationalEntityCriteria.class, localCriteriaSupport, true)
				.build((Class<RelationalExecutableEntityQuery<C>>) (Class) RelationalExecutableEntityQuery.class);
	}
	
	private EntityCriteriaSupport<C> newWhere() {
		// we must clone the underlying support, else it would be modified for all subsequent invocations and criteria will aggregate
		return new EntityCriteriaSupport<>(criteriaSupport);
	}
	
	@Override
	public Set<C> selectAll() {
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
	public EntityJoinTree<C, I> getEntityJoinTree() {
		return mainPersister.getEntityJoinTree();
	}
	
	@Override
	public <E, ID> void copyRootJoinsTo(EntityJoinTree<E, ID> entityJoinTree, String joinName) {
		getEntityJoinTree().projectTo(getEntityJoinTree(), joinName);
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
				
				Map<Column<T, Object>, Column<SUBENTITYTABLE, Object>> projectedColumnMap = new HashMap<>(provider.getColumns().size());
				provider.getColumns().forEach(c -> {
					Column<SUBENTITYTABLE, Object> projectedColumn = subEntityPersister.<SUBENTITYTABLE>getMapping().getTargetTable().addColumn(c.getName(), c.getJavaType(), c.getSize());
					projectedColumn.nullable(c.isNullable());
					projectedColumnMap.put(c, projectedColumn);
				});
				return new ShadowColumnValueProvider<D, SUBENTITYTABLE>() {
					
					private final Set<Column<SUBENTITYTABLE, Object>> values = new HashSet<>(projectedColumnMap.values());
					
					@Override
					public Set<Column<SUBENTITYTABLE, Object>> getColumns() {
						return values;
					}
					
					@Override
					public Map<Column<SUBENTITYTABLE, Object>, Object> giveValue(D bean) {
						Map<Column<T, Object>, Object> columnObjectMap = provider.giveValue(bean);
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

			((PersisterListener) sourcePersister).addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, DIFFERED_ENTITY_LOADER));
			
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
																							  Set<? extends Column<T2, Object>> selectableColumns, boolean optional,
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
						new FirstPhaseRelationLoader<>(subPersister.getMapping().getIdMapping(), selectExecutor, (ThreadLocal) DIFFERED_ENTITY_LOADER),
						leftColumn,
						joinColumnPerSubPersister.get(subPersister),
						JoinType.OUTER);
			});
			
			// adding second phase loader
			((PersisterListener) sourcePersister).addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, DIFFERED_ENTITY_LOADER));
			
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
			Column<SUBTABLE, Object> column = subTypeMapping.getTargetTable().addColumn(col.getExpression(), col.getJavaType());
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
		ModifiableInt discriminatorComputer = new ModifiableInt();
		
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
					expression = cast(null, column.getJavaType());
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
		
		ModifiableInt discriminatorComputer = new ModifiableInt();
		subPersisters.forEach(subPersister -> {
			ConfiguredRelationalPersister<V, I> localSubPersister = (ConfiguredRelationalPersister<V, I>) subPersister;
			entityJoinTree.<V, T1, T2, I>addMergeJoin(mainPolymorphicJoinNodeName,
					new EntityMergerAdapter<>(localSubPersister.<T2>getMapping()),
					mainPersister.getMainTable().getPrimaryKey(),
					subPersister.getMainTable().getPrimaryKey(),
					JoinType.OUTER,
					columnedRow -> {
						PolymorphicEntityInflater<C, V, I, T> editPolymorphicEntityInflater = new PolymorphicEntityInflater<>(mainPersister, localSubPersister);
						PolymorphicMergeJoinRowConsumer<C, V, I> joinRowConsumer = new PolymorphicMergeJoinRowConsumer<>(
								editPolymorphicEntityInflater, columnedRow);
						mainPersisterJoin.addSubPersisterJoin(localSubPersister, joinRowConsumer, discriminatorComputer.increment());
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
		
		PrimaryKey<T, I> primaryKey = mainPersister.getMainTable().getPrimaryKey();
		primaryKey.getColumns().forEach(col -> {
			PseudoColumn<I> primaryKeyPseudoColumn = subPersistersUnion.registerColumn(col.getName(), mainPersister.getMapping().getIdMapping().getIdentifierInsertionManager().getIdentifierType());
		});
		
		ModifiableInt discriminatorComputer = new ModifiableInt();
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
				selectExecutor,
				(ThreadLocal<Queue<Set<RelationIds<Object, C, I>>>>) (ThreadLocal) DIFFERED_ENTITY_LOADER,
				subtypeSelectorPerDiscriminatorValue,
				discriminatorPseudoColumn
		);
		
		// We take right join column from "union all" query : actually it must be taken from union made as a pseudo table
		// because taking it directly from Union is incorrect since a Union doesn't implement Fromable (because it hasn't
		// any alias)
		UnionInFrom unionInFrom = subPersistersUnion.asPseudoTable("unioned_" + mainPersister.getClassToPersist().getSimpleName());
		KeyBuilder<UnionInFrom, JOINID> keyBuilder = Key.from(unionInFrom);
		rightJoinColumn.getColumns().forEach(column -> {
			keyBuilder.addColumn(subPersistersUnion.registerColumn(column.getExpression(), column.getJavaType()));
		});
		
		Key<UnionInFrom, JOINID> rightJoinLink = keyBuilder.build();
		
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
													  TablePerClassPolymorphicSelectExecutor<C, I, ?> selectExecutor,
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
			// by only treating known discriminator values (preferred way to check against null because type can be primitive one)
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
		public Set<Selectable<Object>> getSelectableColumns() {
			Set<Selectable<Object>> result = new HashSet<>(idMapping.getIdentifierAssembler().getColumns());
			result.add((Selectable) discriminatorColumn);
			return result;
		}
	}
}
