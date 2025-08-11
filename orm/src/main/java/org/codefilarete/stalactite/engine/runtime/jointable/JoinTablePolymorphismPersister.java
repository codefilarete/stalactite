package org.codefilarete.stalactite.engine.runtime.jointable;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.DeleteExecutor;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.InsertExecutor;
import org.codefilarete.stalactite.engine.UpdateExecutor;
import org.codefilarete.stalactite.engine.runtime.AbstractPolymorphismPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.EntityMappingWrapper;
import org.codefilarete.stalactite.engine.runtime.FirstPhaseRelationLoader;
import org.codefilarete.stalactite.engine.runtime.PersisterWrapper;
import org.codefilarete.stalactite.engine.runtime.PolymorphicPersister;
import org.codefilarete.stalactite.engine.runtime.RelationIds;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.SecondPhaseRelationLoader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.load.JoinTablePolymorphicRelationJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.PolymorphicMergeJoinRowConsumer;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

/**
 * Class that wraps some other persisters and transfers its invocations to them.
 * Used for polymorphism to dispatch method calls to sub-entities persisters.
 * 
 * @author Guillaume Mary
 */
public class JoinTablePolymorphismPersister<C, I> extends AbstractPolymorphismPersister<C, I> {
	
	/**
	 * Current storage of entities to be loaded during the 2-Phases load algorithm.
	 * Tracked as a {@link Queue} to solve resource cleaning issue in case of recursive polymorphism. This may be solved by avoiding to have a static field
	 */
	// TODO : try a non-static field to remove Queue usage which impacts code complexity
	@SuppressWarnings("java:S5164" /* remove() is called by SecondPhaseRelationLoader.afterSelect() */)
	private static final ThreadLocal<Queue<Set<RelationIds<Object /* E */, Object /* target */, Object /* target identifier */ >>>> CURRENT_2PHASES_LOAD_CONTEXT = new ThreadLocal<>();
	
	private final Map<Class<? extends C>, IdMapping<C, I>> subclassIdMappingStrategies;
	private final Map<Class<? extends C>, Table> tablePerSubEntityType;
	private final PrimaryKey<?, I> mainTablePrimaryKey;
	
	public JoinTablePolymorphismPersister(ConfiguredRelationalPersister<C, I> mainPersister,
										  Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> subEntitiesPersisters,
										  ConnectionProvider connectionProvider,
										  Dialect dialect) {
		super(mainPersister,
				subEntitiesPersisters,
				new JoinTablePolymorphismEntityFinder<>(
						mainPersister,
						subEntitiesPersisters,
						connectionProvider,
						dialect));
		Table<?> mainTable = mainPersister.<Table>getMapping().getTargetTable();
		this.mainTablePrimaryKey = mainTable.getPrimaryKey();
		
		Set<? extends Entry<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>>> subPersisterPerSubEntityType = subEntitiesPersisters.entrySet();
		// Below we keep order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
		// impacts but very difficult to measure
		Map<Class<? extends C>, ConfiguredRelationalPersister<? extends C, I>> subclassSelectExecutors = Iterables.map(subPersisterPerSubEntityType, Entry::getKey,
				Entry::getValue, KeepOrderMap::new);
		this.subclassIdMappingStrategies = Iterables.map(subPersisterPerSubEntityType, Entry::getKey, e -> (IdMapping<C, I>) e.getValue().getMapping().getIdMapping());
		
		this.tablePerSubEntityType = Iterables.map(this.subEntitiesPersisters.entrySet(),
				Entry::getKey,
				entry -> entry.getValue().getMapping().getTargetTable(), KeepOrderMap::new);
	}
	
	@Override
	public <LEFTTABLE extends Table<LEFTTABLE>, SUBTABLE extends Table<SUBTABLE>, JOINTYPE> void propagateMappedAssociationToSubTables(
			Key<SUBTABLE, JOINTYPE> foreignKey,
			PrimaryKey<LEFTTABLE, JOINTYPE> leftPrimaryKey,
			BiFunction<Key<SUBTABLE, JOINTYPE>, PrimaryKey<LEFTTABLE, JOINTYPE>, String> foreignKeyNamingFunction) {
		SUBTABLE mainTable = mainPersister.getMainTable();
		Key.KeyBuilder<SUBTABLE, JOINTYPE> projectedKeyBuilder = Key.from(mainTable);
		((Set<Column<SUBTABLE, ?>>) foreignKey.getColumns()).forEach(column -> {
			projectedKeyBuilder.addColumn(mainTable.addColumn(column.getName(), column.getJavaType(), column.getSize()));
		});
		Key<SUBTABLE, JOINTYPE> projectedKey = projectedKeyBuilder.build();
		mainPersister.getEntityJoinTree().addPassiveJoin(EntityJoinTree.ROOT_JOIN_NAME, foreignKey, projectedKey, EntityJoinTree.JoinType.INNER, java.util.Collections.emptySet());
		mainTable.addForeignKey(foreignKeyNamingFunction, projectedKey, leftPrimaryKey);
	}
	
	@Override
	public Selectable<?> getColumn(List<? extends ValueAccessPoint<?>> accessorChain) {
		return criteriaSupport.getAggregateColumnMapping().giveColumn(accessorChain);
	}
	
	@Override
	public Set<Class<? extends C>> getSupportedEntityTypes() {
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
		// Implied tables are those of sub entities.
		// Note that doing this lately (not in constructor) guaranties that it is up-to-date because sub entities may have relations which are
		// configured out of constructor by caller
		List<Table<?>> subTables = subEntitiesPersisters.values().stream().flatMap(p -> p.giveImpliedTables().stream()).collect(Collectors.toList());
		return Collections.cat(mainPersister.giveImpliedTables(), subTables);
	}
	
	@Override
	public void doInsert(Iterable<? extends C> entities) {
		mainPersister.insert(entities);
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(InsertExecutor::insert);
	}
	
	@Override
	public void doUpdateById(Iterable<? extends C> entities) {
		mainPersister.updateById(entities);
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(UpdateExecutor::updateById);
	}
	
	@Override
	public void doUpdate(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		mainPersister.update(differencesIterable, allColumnsStatement);
		
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
	public void doDelete(Iterable<? extends C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(DeleteExecutor::delete);
		mainPersister.delete(entities);
	}
	
	@Override
	public void doDeleteById(Iterable<? extends C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(DeleteExecutor::deleteById);
		mainPersister.deleteById(entities);
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
		// nothing to do here, called by one-to-many engines, which actually call joinWithMany()
	}
	
	/**
	 * Implementation that captures {@link EntityMapping#addTransformerListener(TransformerListener)}
	 * Made to dispatch those methods subclass strategies since their persisters are in charge of managing their entities (not the parent one).
	 * <p>
	 * Design question : one may think that's not a good design to override a getter, caller should invoke an intention-clear method on
	 * ourselves (Persister) but the case is to add a transformer which is not the goal of the Persister to know implementation
	 * detail : they are to manage cascades and coordinate their mapping strategies. {@link EntityMapping} are in charge of knowing
	 * {@link Column} actions.
	 *
	 * @return an enhanced version of our main persister mapping strategy which dispatches transformer listeners to sub-entities ones
	 */
	@Override
	public <T extends Table<T>> EntityMapping<C, I, T> getMapping() {
		return new EntityMappingWrapper<C, I, T>(mainPersister.getMapping()) {
			@Override
			public void addTransformerListener(TransformerListener<C> listener) {
				super.addTransformerListener(listener);
				subEntitiesPersisters.values().forEach(persister -> persister.getMapping().addTransformerListener(listener));
			}
		};
	}
	
	@Override
	public <SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsOne(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																							 Accessor<SRC, C> propertyAccessor,
																							 Key<T1, JOINID> leftColumn,
																							 Key<T2, JOINID> rightColumn,
																							 String rightTableAlias,
																							 BeanRelationFixer<SRC, C> beanRelationFixer,
																							 boolean optional,
																							 boolean loadSeparately) {
		
		if (loadSeparately) {
			// because subgraph loading is made in 2 phases (load ids, then entities in a second SQL request done by load listener) we add a passive join
			// (we don't need to create bean nor fulfill properties in first phase) 
			// NB: here rightColumn is parent class primary key or reverse column that owns property (depending how one-to-one relation is mapped) 
			String mainTableJoinName = sourcePersister.getEntityJoinTree().addPassiveJoin(ROOT_JOIN_NAME,
					leftColumn, rightColumn, optional ? JoinType.OUTER : JoinType.INNER, rightColumn.getColumns());
			PrimaryKey<?, I> primaryKey = this.<Table>getMapping().getTargetTable().getPrimaryKey();
			this.subclassIdMappingStrategies.forEach((c, idMappingStrategy) -> {
				PrimaryKey<?, I> subclassPrimaryKey = this.tablePerSubEntityType.get(c).getPrimaryKey();
				sourcePersister.getEntityJoinTree().addMergeJoin(mainTableJoinName,
						// Note that "this" uses EntityFinder with identifier as criteria to select entities
						new FirstPhaseRelationLoader<>(idMappingStrategy, this,
								(ThreadLocal<Queue<Set<RelationIds<Object, C, I>>>>) (ThreadLocal) CURRENT_2PHASES_LOAD_CONTEXT),
						primaryKey,
						subclassPrimaryKey,
						// since we don't know what kind of sub entity is present we must do an OUTER join between common truck and all sub tables
						JoinType.OUTER);
			});
			
			// adding second phase loader
			sourcePersister.addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, CURRENT_2PHASES_LOAD_CONTEXT));
			
			return mainTableJoinName;
		} else {
			return join(
					sourcePersister.getEntityJoinTree(),
					ROOT_JOIN_NAME,
					mainPersister,
					propertyAccessor,
					leftColumn,
					rightColumn,
					new HashSet<>(this.subEntitiesPersisters.values()),
					beanRelationFixer,
					null);
		}
	}
	
	@Override
	public <SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsMany(String joinName,
																							  RelationalEntityPersister<SRC, SRCID> sourcePersister,
																							  Accessor<SRC, ?> propertyAccessor,
																							  Key<T1, JOINID> leftColumn,
																							  Key<T2, JOINID> rightColumn,
																							  BeanRelationFixer<SRC, C> beanRelationFixer,
																							  @Nullable Function<ColumnedRow, Object> duplicateIdentifierProvider,
																							  Set<? extends Column<T2, ?>> selectableColumns,
																							  boolean optional,
																							  boolean loadSeparately) {
		if (loadSeparately) {
			String createdJoinName = sourcePersister.getEntityJoinTree().addPassiveJoin(joinName,
					leftColumn,
					rightColumn,
					optional ? JoinType.OUTER : JoinType.INNER, 
					selectableColumns);
			
			// Subgraph loading is made in 2 phases (load ids, then entities in a second SQL request done by load listener)
			this.subclassIdMappingStrategies.forEach((c, idMappingStrategy) -> {
				PrimaryKey<T2, I> subclassPrimaryKey = this.tablePerSubEntityType.get(c).getPrimaryKey();
				sourcePersister.getEntityJoinTree().addMergeJoin(createdJoinName,
						// Note that "this" uses EntityFinder with identifier as criteria to select entities
						new FirstPhaseRelationLoader<C, I>(idMappingStrategy, this,
								(ThreadLocal<Queue<Set<RelationIds<Object,C,I>>>>) (ThreadLocal) CURRENT_2PHASES_LOAD_CONTEXT),
						mainTablePrimaryKey,
						subclassPrimaryKey,
						// since we don't know what kind of sub entity is present we must do an OUTER join between common truck and all sub tables
						JoinType.OUTER);
			});
			
			// adding second phase loader
			sourcePersister.addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, CURRENT_2PHASES_LOAD_CONTEXT));
			
			return createdJoinName;
		} else {
			return join(
					sourcePersister.getEntityJoinTree(),
					joinName,
					mainPersister,
					propertyAccessor,
					leftColumn,
					rightColumn,
					new HashSet<>(this.subEntitiesPersisters.values()),
					beanRelationFixer,
					duplicateIdentifierProvider);
		}
	}
	
	private <SRC, SRCID, U, T1 extends Table<T1>, T2 extends Table<T2>, ID, JOINID> String join(
			EntityJoinTree<SRC, SRCID> entityJoinTree,
			String leftStrategyName,
			ConfiguredRelationalPersister<U, ID> mainPersister,
			Accessor<SRC, ?> propertyAccessor,
			Key<T1, JOINID> leftJoinColumn,
			Key<T2, JOINID> rightJoinColumn,
			Set<ConfiguredRelationalPersister<? extends U, ID>> subPersisters,
			BeanRelationFixer<SRC, U> beanRelationFixer,
			@Nullable Function<ColumnedRow, Object> relationIdentifierProvider) {
		
		Holder<JoinTablePolymorphicRelationJoinNode<U, T1, T2, JOINID, ID>> createdJoinHolder = new Holder<>();
		String relationJoinName = entityJoinTree.addJoin(leftStrategyName, parent -> {
			JoinTablePolymorphicRelationJoinNode<U, T1, T2, JOINID, ID> polymorphicRelationJoinNode = new JoinTablePolymorphicRelationJoinNode<U, T1, T2, JOINID, ID>(
					(JoinNode<SRC, T1>) (JoinNode) parent,
					propertyAccessor,
					leftJoinColumn,
					rightJoinColumn,
					JoinType.OUTER,
					mainPersister.<T2>getMainTable().getColumns(),
					null,
					new EntityMappingAdapter<>(mainPersister.<T1>getMapping()),
					(BeanRelationFixer<Object, U>) beanRelationFixer,
					relationIdentifierProvider);
			createdJoinHolder.set(polymorphicRelationJoinNode);
			return polymorphicRelationJoinNode;
		});
		
		this.addPolymorphicSubPersistersJoins(entityJoinTree, relationJoinName, mainPersister, createdJoinHolder.get(), subPersisters);
		
		return relationJoinName;
	}
	
	private <SRC, SRCID, U, V extends U, T1 extends Table<T1>, T2 extends Table<T2>, ID> void addPolymorphicSubPersistersJoins(
			EntityJoinTree<SRC, SRCID> entityJoinTree,
			String mainPolymorphicJoinNodeName,
			ConfiguredRelationalPersister<U, ID> mainPersister,
			JoinTablePolymorphicRelationJoinNode<U, T1, T2, ?, ID> mainPersisterJoin,
			Set<ConfiguredRelationalPersister<? extends U, ID>> subPersisters) {
		
		subPersisters.forEach(subPersister -> {
			ConfiguredRelationalPersister<V, ID> localSubPersister = (ConfiguredRelationalPersister<V, ID>) subPersister;
			entityJoinTree.addJoin(mainPolymorphicJoinNodeName, parent -> new MergeJoinNode<V, T1, T2, ID>(
					(JoinNode<SRC, T1>) (JoinNode) parent,
					mainPersister.<T1>getMainTable().getPrimaryKey(),
					subPersister.<T2>getMainTable().getPrimaryKey(),
					JoinType.OUTER,
					null,
					new EntityMergerAdapter<>((EntityMapping<V, ID, ?>) localSubPersister.getMapping())) {
				@Override
				public MergeJoinRowConsumer<V> toConsumer(JoinNode<V, T2> joinNode) {
					PolymorphicMergeJoinRowConsumer<V, ID> joinRowConsumer = new PolymorphicMergeJoinRowConsumer<>(
							(MergeJoinNode<V, ?, ?, ID>) joinNode, 
							localSubPersister.getMapping());
					mainPersisterJoin.addSubPersisterJoin(joinRowConsumer);
					return joinRowConsumer;
				}
			});
		});
	}
}
