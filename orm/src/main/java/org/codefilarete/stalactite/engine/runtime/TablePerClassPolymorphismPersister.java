package org.codefilarete.stalactite.engine.runtime;

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

import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.stalactite.engine.DeleteExecutor;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.ExecutableQuery;
import org.codefilarete.stalactite.engine.InsertExecutor;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.TablePerClassPolymorphism;
import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.UpdateExecutor;
import org.codefilarete.stalactite.engine.configurer.OneToManyRelationConfigurer;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersisterListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister.CriteriaProvider;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.PolymorphicEntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.PolymorphicMergeJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
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
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Functions;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.trace.ModifiableInt;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_STRATEGY_NAME;
import static org.codefilarete.stalactite.query.model.Operators.cast;
import static org.codefilarete.tool.collection.Iterables.first;

/**
 * @author Guillaume Mary
 */
public class TablePerClassPolymorphismPersister<C, I, T extends Table<T>> implements EntityConfiguredJoinedTablesPersister<C, I>, PolymorphicPersister<C> {
	
	@SuppressWarnings("java:S5164" /* remove() is called by SecondPhaseRelationLoader.afterSelect() */)
	private static final ThreadLocal<Queue<Set<RelationIds<Object /* E */, Object /* target */, Object /* target identifier */>>>> DIFFERED_ENTITY_LOADER = new ThreadLocal<>();
	
	private final EntityConfiguredJoinedTablesPersister<C, I> mainPersister;
	private final Map<Class<? extends C>, UpdateExecutor<C>> subclassUpdateExecutors;
	private final TablePerClassPolymorphism<C> polymorphismPolicy;
	private final Map<Class<? extends C>, ? extends EntityConfiguredJoinedTablesPersister<C, I>> subEntitiesPersisters;
	private final TablePerClassPolymorphicSelectExecutor<C, I, T> selectExecutor;
	private final TablePerClassPolymorphicEntitySelectExecutor<C, I, T> entitySelectExecutor;
	private final EntityCriteriaSupport<C> criteriaSupport;
	
	public TablePerClassPolymorphismPersister(EntityConfiguredJoinedTablesPersister<C, I> mainPersister,
											  Map<Class<? extends C>, SimpleRelationalEntityPersister<C, I, T>> subEntitiesPersisters,
											  ConnectionProvider connectionProvider,
											  Dialect dialect,
											  TablePerClassPolymorphism<C> polymorphismPolicy) {
		this.mainPersister = mainPersister;
		Set<Entry<Class<? extends C>, SimpleRelationalEntityPersister<C, I, T>>> entries = subEntitiesPersisters.entrySet();
		this.subclassUpdateExecutors = Iterables.map(entries, Entry::getKey, e -> e.getValue().getUpdateExecutor(), KeepOrderMap::new);
		this.polymorphismPolicy = polymorphismPolicy;
		
		Map<Class, Table> tablePerSubEntity = Iterables.map((Set) entries,
				Entry::getKey,
				Functions.<Entry<Class, SimpleRelationalEntityPersister>, SimpleRelationalEntityPersister, Table>chain(Entry::getValue, SimpleRelationalEntityPersister::getMainTable));
		
		
		this.subEntitiesPersisters = subEntitiesPersisters;
		this.subEntitiesPersisters.forEach((type, persister) ->
				mainPersister.getEntityJoinTree().projectTo(persister.getEntityJoinTree(), ROOT_STRATEGY_NAME)
		);
		
		Map<Class<? extends C>, SelectExecutor<C, I>> subEntitiesSelectors = Iterables.map(subEntitiesPersisters.entrySet(),
				Entry::getKey,
				Functions.chain(Entry::getValue, SimpleRelationalEntityPersister::getSelectExecutor));
		this.selectExecutor = new TablePerClassPolymorphicSelectExecutor<>(
				tablePerSubEntity,
				subEntitiesSelectors,
				(T) mainPersister.getMapping().getTargetTable(), connectionProvider, dialect);
		
		this.entitySelectExecutor = new TablePerClassPolymorphicEntitySelectExecutor<>(
				tablePerSubEntity,
				subEntitiesPersisters,
				(T) mainPersister.getMapping().getTargetTable(),
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
		return this.subEntitiesPersisters.values().stream().map(ConfiguredPersister::getMapping).map(EntityMapping::getTargetTable).collect(Collectors.toList());
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
		// we must clone the underlying support, else it would be modified for all subsequent invocations and criteria will aggregate
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
	public EntityJoinTree<C, I> getEntityJoinTree() {
		return mainPersister.getEntityJoinTree();
	}
	
	@Override
	public <E, ID> void copyRootJoinsTo(EntityJoinTree<E, ID> entityJoinTree, String joinName) {
		getEntityJoinTree().projectTo(getEntityJoinTree(), joinName);
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
				subEntitiesPersisters.values().forEach(persister -> persister.getMapping().addTransformerListener(listener));
			}
			
			@Override
			public <O> void addShadowColumnInsert(ShadowColumnValueProvider<C, O, T> provider) {
				subEntitiesPersisters.values().forEach(p -> {
					Column<T, O> c = provider.getColumn();
					Column projectedColumn = p.getMapping().getTargetTable().addColumn(c.getName(), c.getJavaType(), c.getSize());
					p.getMapping().addShadowColumnInsert(new ShadowColumnValueProvider<>(projectedColumn, provider.getValueProvider()));
				});
			}
			
			@Override
			public <O> void addShadowColumnUpdate(ShadowColumnValueProvider<C, O, T> provider) {
				subEntitiesPersisters.values().forEach(p -> {
					Column<T, O> c = provider.getColumn();
					Column projectedColumn = p.getMapping().getTargetTable().addColumn(c.getName(), c.getJavaType(), c.getSize());
					p.getMapping().addShadowColumnUpdate(new ShadowColumnValueProvider<>(projectedColumn, provider.getValueProvider()));
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
																				  boolean optional,
																				  boolean loadSeparately) {
		if (loadSeparately) {
			String createdJoinNodeName = joinAsOneWithSeparateLoading(sourcePersister.getEntityJoinTree(), ROOT_STRATEGY_NAME,
					mainPersister,
					leftColumn,
					rightColumn,
					new HashSet<>(this.subEntitiesPersisters.values()));

			((PersisterListener) sourcePersister).addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, DIFFERED_ENTITY_LOADER));
			
			return createdJoinNodeName;
		} else {
			return join(
					sourcePersister.getEntityJoinTree(),
					ROOT_STRATEGY_NAME,
					mainPersister,
					leftColumn,
					rightColumn,
					new HashSet<>(this.subEntitiesPersisters.values()),
					beanRelationFixer);
		}
	}
	
	@Override
	public <SRC, T1 extends Table, T2 extends Table, SRCID, ID> String joinAsMany(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																				  Column<T1, ID> leftColumn,
																				  Column<T2, ID> rightColumn,
																				  BeanRelationFixer<SRC, C> beanRelationFixer,
																				  @Nullable BiFunction<Row, ColumnedRow, ?> duplicateIdentifierProvider,
																				  String joinName,
																				  Set<Column<T2, ?>> selectableColumns, boolean optional,
																				  boolean loadSeparately) {
		
		Column<T, Object> mainTablePK = first(((T) mainPersister.getMapping().getTargetTable()).getPrimaryKey().getColumns());
		Map<EntityConfiguredJoinedTablesPersister, Column> joinColumnPerSubPersister = new HashMap<>();
		if (rightColumn.equals(mainTablePK)) {
			// join is made on primary key => case is association table
			subEntitiesPersisters.forEach((c, subPersister) -> {
				Column<T, Object> column = first((Set<Column>) subPersister.getMapping().getTargetTable().getPrimaryKey().getColumns());
				joinColumnPerSubPersister.put(subPersister, column);
			});
		} else {
			// join is made on a foreign key => case of relation owned by reverse side
			subEntitiesPersisters.forEach((c, subPersister) -> {
				Column<T, ?> column = subPersister.getMapping().getTargetTable().addColumn(rightColumn.getName(), rightColumn.getJavaType());
				subPersister.getMapping().addShadowColumnSelect((Column) column);
				joinColumnPerSubPersister.put(subPersister, column);
			});
		}
		
		if (loadSeparately) {
			// TODO: simplify query : it joins on target table as many as subentities which can be reduced to one join if FirstPhaseRelationLoader
			//  can compute discriminatorValue 
			subEntitiesPersisters.forEach((c, subPersister) -> {
				Column subclassPrimaryKey = first((Set<Column>) subPersister.getMapping().getTargetTable().getPrimaryKey().getColumns());
				sourcePersister.getEntityJoinTree().addMergeJoin(joinName,
						new FirstPhaseRelationLoader<>(subPersister.getMapping().getIdMapping(), subclassPrimaryKey, selectExecutor,
								(ThreadLocal) DIFFERED_ENTITY_LOADER),
						leftColumn, joinColumnPerSubPersister.get(subPersister), JoinType.OUTER);
			});
			
			// adding second phase loader
			((PersisterListener) sourcePersister).addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, DIFFERED_ENTITY_LOADER));
			
			// FIXME : we shouldn't return null here but a created join node name: which one since we have several table to join ? see joinAsOne(..) maybe ?
			return null;
		} else {
			return join(
					sourcePersister.getEntityJoinTree(),
					joinName,
					mainPersister,
					leftColumn,
					rightColumn,
					new HashSet<>(this.subEntitiesPersisters.values()),
					beanRelationFixer);
		}
	}

	private <SRC, SRCID, U, T1 extends Table, T2 extends Table, ID, JOINCOLTYPE> String join(
			EntityJoinTree<SRC, SRCID> entityJoinTree,
			String leftStrategyName,
			EntityConfiguredJoinedTablesPersister<U, ID> mainPersister,
			Column<T1, JOINCOLTYPE> leftJoinColumn,
			Column<T2, JOINCOLTYPE> rightJoinColumn,
			Set<EntityConfiguredJoinedTablesPersister<? extends U, ID>> subPersisters,
			BeanRelationFixer<SRC, U> beanRelationFixer) {
		
		
		// we build a union of all sub queries that will be joined in the main query
		// To build the union we need columns that are common to all persisters
		Set<Column<?, ?>> commonColumns = new KeepOrderSet<>();
		commonColumns.addAll(mainPersister.getMapping().getSelectableColumns());
		// TODO : right column is not in selected columns of class mapping : understand why (and if that's normal)
		commonColumns.add(rightJoinColumn);
		
		Set<String> commonColumnsNames = commonColumns.stream().map(Column::getName).collect(Collectors.toSet());
		
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
				subEntityQuery.select(column.getName(), column.getJavaType());
				subPersistersUnion.registerColumn(column.getName(), column.getJavaType());
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
		
		Holder<TablePerClassPolymorphicRelationJoinNode<U, T1, JOINCOLTYPE, ID>> createdJoinHolder = new Holder<>();
		String relationJoinName = entityJoinTree.<T1>addJoin(leftStrategyName, parent -> {
			TablePerClassPolymorphicRelationJoinNode<U, T1, JOINCOLTYPE, ID> relationJoinNode = new TablePerClassPolymorphicRelationJoinNode<>(parent,
					subPersistersUnion,
					leftJoinColumn,
					rightJoinColumn,
					JoinType.OUTER,
					subPersistersUnion.getColumns(),
					mainPersister.getClassToPersist().getSimpleName(),
					new EntityMappingAdapter<>(mainPersister.getMapping()),
					(BeanRelationFixer<Object, U>) beanRelationFixer,
					discriminatorPseudoColumn);
			createdJoinHolder.set(relationJoinNode);
			return relationJoinNode;
		});
		
		this.addTablePerClassPolymorphicSubPersistersJoins(entityJoinTree, relationJoinName, mainPersister, createdJoinHolder.get(), subPersisters);
		
		return relationJoinName;
	}
	
	private <SRC, SRCID, U, V extends U, T1 extends Table, T2 extends Table, ID> void addTablePerClassPolymorphicSubPersistersJoins(
			EntityJoinTree<SRC, SRCID> entityJoinTree,
			String mainPolymorphicJoinNodeName,
			EntityConfiguredJoinedTablesPersister<U, ID> mainPersister,
			TablePerClassPolymorphicRelationJoinNode<U, T1, ?, ID> mainPersisterJoin,
			Set<EntityConfiguredJoinedTablesPersister<? extends U, ID>> subPersisters) {
		
		ModifiableInt discriminatorComputer = new ModifiableInt();
		subPersisters.forEach(subPersister -> {
			EntityConfiguredJoinedTablesPersister<V, ID> localSubPersister = (EntityConfiguredJoinedTablesPersister<V, ID>) subPersister;
			entityJoinTree.<V, T1, T2, ID>addMergeJoin(mainPolymorphicJoinNodeName,
					new EntityMergerAdapter<>(localSubPersister.getMapping()),
					(Column) first(mainPersister.getMainTable().getPrimaryKey().getColumns()),
					(Column) first(subPersister.getMainTable().getPrimaryKey().getColumns()),
					JoinType.OUTER,
					columnedRow -> {
						PolymorphicMergeJoinRowConsumer<U, V, ID> joinRowConsumer = new PolymorphicMergeJoinRowConsumer<>(
								new PolymorphicEntityInflater<>(mainPersister, localSubPersister), columnedRow);
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
	private <SRC, U extends C, T1 extends Table, T2 extends Table, SRCID, ID> String joinAsOneWithSeparateLoading(
			EntityJoinTree<SRC, SRCID> entityJoinTree,
			String leftStrategyName,
			EntityConfiguredJoinedTablesPersister<U, I> mainPersister,
			Column<T1, ID> leftJoinColumn,
			Column<T2, ID> rightJoinColumn,
			Set<EntityConfiguredJoinedTablesPersister<C, I>> subPersisters) {
		
		Union subPersistersUnion = new Union();
		// Union will contain only 3 columns :
		// - discriminator
		// - entity primary key
		// - join column
		String entityTypeDiscriminatorName = "clazz_";
		PseudoColumn<Integer> discriminatorPseudoColumn = subPersistersUnion.registerColumn(entityTypeDiscriminatorName, Integer.class);
		
		Column<?, I> primaryKey = first(((Set<Column>) mainPersister.getMainTable().getPrimaryKey().getColumns()));
		PseudoColumn<I> primaryKeyPseudoColumn = subPersistersUnion.registerColumn(primaryKey.getName(), mainPersister.getMapping().getIdMapping().getIdentifierInsertionManager().getIdentifierType());
		
		PseudoColumn<ID> rightJoinPseudoColumn = subPersistersUnion.registerColumn(rightJoinColumn.getName(), rightJoinColumn.getJavaType());
		
		ModifiableInt discriminatorComputer = new ModifiableInt();
		Map<Integer, SelectExecutor<C, I>> subtypeSelectorPerDiscriminatorValue = new HashMap<>();
		
		subPersisters.forEach(subPersister -> {
			Query subEntityQuery = new Query(subPersister.getMapping().getTargetTable());
			int discriminatorValue = discriminatorComputer.increment();
			subtypeSelectorPerDiscriminatorValue.put(discriminatorValue, subPersister);
			subEntityQuery.select(String.valueOf(discriminatorValue), Integer.class).as(entityTypeDiscriminatorName);
			subPersistersUnion.unionAll(subEntityQuery);
			
			subEntityQuery.select(rightJoinColumn.getName(), rightJoinColumn.getJavaType());
			
			((Set<Column>) mainPersister.getMapping().getTargetTable().getPrimaryKey().getColumns()).forEach(column -> {
				subEntityQuery.select(column.getName(), column.getJavaType());
			});
		});
		
		IdMapping<U, I> idMapping = mainPersister.getMapping().getIdMapping();
		TablePerClassFirstPhaseRelationLoader tablePerClassFirstPhaseRelationLoader = new TablePerClassFirstPhaseRelationLoader(
				(IdMapping<C, I>) idMapping,
				primaryKeyPseudoColumn,
				(TablePerClassPolymorphicSelectExecutor) selectExecutor,
				(ThreadLocal<Queue<Set<RelationIds<Object, C, I>>>>) (ThreadLocal) DIFFERED_ENTITY_LOADER,
				subtypeSelectorPerDiscriminatorValue,
				discriminatorPseudoColumn
		);
		
		// We take right join column from "union all" query : actually it must be took from union made as a pseudo table
		// because taking it directly from Union is incorrect since a Union doesn't implement Fromable (because it hasn't
		// any alias)
		UnionInFrom unionInFrom = subPersistersUnion.asPseudoTable("unioned_" + mainPersister.getClassToPersist().getSimpleName());
		JoinLink rightJoinLink = (JoinLink) unionInFrom.mapColumnsOnName().get(rightJoinPseudoColumn.getExpression());
		
		return entityJoinTree.addMergeJoin(leftStrategyName,
				tablePerClassFirstPhaseRelationLoader,
				leftJoinColumn,
				rightJoinLink,
				JoinType.OUTER);
	}
	
	private class TablePerClassFirstPhaseRelationLoader extends FirstPhaseRelationLoader<C, I, Table> {
		
		private final PseudoColumn<Integer> discriminatorColumn;
		private final Map<Integer, SelectExecutor<C, I>> subtypeSelectorPerDiscriminatorValue;
		
		private TablePerClassFirstPhaseRelationLoader(IdMapping<C, I> subEntityIdMapping,
													  PseudoColumn<I> primaryKey,
													  TablePerClassPolymorphicSelectExecutor<C, I, Table> selectExecutor,
													  ThreadLocal<Queue<Set<RelationIds<Object, C, I>>>> relationIdsHolder,
													  Map<Integer, SelectExecutor<C, I>> subtypeSelectorPerDiscriminatorValue,
													  PseudoColumn<Integer> discriminatorColumn) {
			// Note that selectExecutor won't be used because we dynamically lookup for it in fillCurrentRelationIds
			super(subEntityIdMapping, primaryKey, selectExecutor, relationIdsHolder);
			this.discriminatorColumn = discriminatorColumn;
			this.subtypeSelectorPerDiscriminatorValue = subtypeSelectorPerDiscriminatorValue;
		}
		
		@Override
		protected void fillCurrentRelationIds(Row row, Object bean, ColumnedRow columnedRow) {
			Integer discriminator = columnedRow.getValue(discriminatorColumn, row);
			// we avoid NPE on polymorphismPolicy.getClass(discriminator) caused by null discriminator in case of empty relation
			// by only treating known discriminator values (preferred way to check against null because type can be primitive one)
			if (subtypeSelectorPerDiscriminatorValue.containsKey(discriminator)) {
				Set<RelationIds<Object, C, I>> relationIds = relationIdsHolder.get().peek();
				relationIds.add(new RelationIds<>(giveSelector(discriminator),
						idMapping.getIdAccessor()::getId, bean, (I) columnedRow.getValue(primaryKey, row)));
			}
		}
		
		@Override
		public Set<Selectable<Object>> getSelectableColumns() {
			return (Set) Arrays.asSet(primaryKey, discriminatorColumn);
		}
		
		private SelectExecutor<C, I> giveSelector(Integer discriminator) {
			return subtypeSelectorPerDiscriminatorValue.get(discriminator);
		}
	}
}
