package org.codefilarete.stalactite.engine.runtime;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.PersistExecutor;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecordMapping.KeyValueRecordIdMapping;
import org.codefilarete.stalactite.engine.configurer.map.RecordId;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport;
import org.codefilarete.stalactite.mapping.AccessorWrapperIdAccessor;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.query.EntityFinder;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.operator.In;
import org.codefilarete.stalactite.query.model.operator.TupleIn;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

/**
 * Persister that registers relations of entities joined on "foreign key = primary key".
 * This does not handle inheritance nor entities mapped on several tables, it focuses on select part : a main table is defined by
 * {@link DefaultEntityMapping} passed to constructor which then it can be added to some other {@link RelationalEntityPersister} thanks to
 * {@link RelationalEntityPersister#joinAsMany(String, RelationalEntityPersister, Accessor, Key, Key, BeanRelationFixer, Function, boolean, boolean)} and
 * {@link RelationalEntityPersister#joinAsOne(RelationalEntityPersister, Accessor, Key, Key, String, BeanRelationFixer, boolean, boolean)}.
 * 
 * Entity load is defined by a select that joins all tables, each {@link DefaultEntityMapping} is called to complete
 * entity loading.
 * 
 * In the orm module this class replace {@link BeanPersister} in case of single table, because it has methods for join support whereas {@link BeanPersister}
 * doesn't.
 * 
 * @param <C> the main class to be persisted
 * @param <I> the type of main class identifiers
 * @param <T> the main target table
 * @author Guillaume Mary
 */
public class SimpleRelationalEntityPersister<C, I, T extends Table<T>>
		extends PersisterListenerWrapper<C, I>
		implements ConfiguredRelationalPersister<C, I>, AdvancedEntityPersister<C, I> {
	
	protected final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
	
	/**
	 * Current storage of entities to be loaded during the 2-Phases load algorithm.
	 * Tracked as a {@link Queue} to solve resource cleaning issue in case of recursive polymorphism. This may be solved by avoiding to have a static field
	 */
	// TODO : try a non-static field to remove Queue usage which impacts code complexity
	@SuppressWarnings("java:S5164" /* remove() is called by SecondPhaseRelationLoader.afterSelect() */)
	private static final ThreadLocal<Queue<Set<RelationIds<Object /* E */, Object /* target */, Object /* target identifier */ >>>> CURRENT_2PHASES_LOAD_CONTEXT = new ThreadLocal<>();
	
	private final BeanPersister<C, I, T> persister;
	/** Support for {@link EntityCriteria} query execution */
	private final EntityFinder<C, I> entityFinder;
	/** Support for defining entity criteria on {@link #selectWhere()} */
	private final EntityCriteriaSupport<C> criteriaSupport;
	private final PersistExecutor<C> persistExecutor;
	private final EntityJoinTree<C, I> entityJoinTree;
	protected final Dialect dialect;
	
	public SimpleRelationalEntityPersister(DefaultEntityMapping<C, I, T> mainMappingStrategy,
										   Dialect dialect,
										   ConnectionConfiguration connectionConfiguration) {
		this.persister = new BeanPersister<>(mainMappingStrategy, dialect, connectionConfiguration);
		this.entityJoinTree = new EntityJoinTree<>(new EntityMappingAdapter<>(persister.getMapping()), persister.getMapping().getTargetTable());
		this.dialect = dialect;
		this.entityFinder = new RelationalEntityFinder<>(entityJoinTree, this, persister.getConnectionProvider(), dialect);
		this.criteriaSupport = new EntityCriteriaSupport<>(entityJoinTree);
		
		if (persister.getMapping().getIdMapping().getIdentifierInsertionManager() instanceof AlreadyAssignedIdentifierManager) {
			// we redirect all invocations to ourselves because targeted methods invoke their listeners
			this.persistExecutor = new AlreadyAssignedIdentifierPersistExecutor<>(this);
		} else {
			// we redirect all invocations to ourselves because targeted methods invoke their listeners
			this.persistExecutor = new DefaultPersistExecutor<>(this);
		}
	}
	
	@Override
	public I getId(C entity) {
		return this.persister.getId(entity);
	}
	
	@Override
	public T getMainTable() {
		return this.persister.getMainTable();
	}
	
	public InsertExecutor<C, I, T> getInsertExecutor() {
		return persister.getInsertExecutor();
	}
	
	public UpdateExecutor<C, I, T> getUpdateExecutor() {
		return persister.getUpdateExecutor();
	}
	
	@Override
	public EntityFinder<C, I> getEntityFinder() {
		return this.entityFinder;
	}
	
	public DeleteExecutor<C, I, T> getDeleteExecutor() {
		return persister.getDeleteExecutor();
	}
	
	@Override
	public EntityJoinTree<C, I> getEntityJoinTree() {
		return this.entityJoinTree;
	}
	
	@Override
	public EntityMapping<C, I, T> getMapping() {
		return persister.getMapping();
	}
	
	@Override
	public Set<Table<?>> giveImpliedTables() {
		return getEntityJoinTree().giveTables();
	}
	
	@Override
	public ExecutableEntityQueryCriteria<C, ?> selectWhere() {
		return newCriteriaSupport().wrapIntoExecutable();
	}
	
	@Override
	public EntityQueryCriteriaSupport<C, I> newCriteriaSupport() {
		return new EntityQueryCriteriaSupport<>(entityFinder, criteriaSupport.copy());
	}
	
	@Override
	public ProjectionQueryCriteriaSupport<C, I> newProjectionCriteriaSupport(Consumer<Select> selectAdapter) {
		return new ProjectionQueryCriteriaSupport<>(entityFinder, newCriteriaSupport().getEntityCriteriaSupport(), selectAdapter);
	}
	
	@Override
	public ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<Select> selectAdapter) {
		ProjectionQueryCriteriaSupport<C, I> projectionSupport = new ProjectionQueryCriteriaSupport<>(entityFinder, selectAdapter);
		return projectionSupport.wrapIntoExecutable();
	}
	
	/**
	 * Select all instances with all relations fetched.
	 * 
	 * @return all instance found in database
	 */
	@Override
	public Set<C> selectAll() {
		return selectWhere().execute(Accumulators.toSet());
	}
	
	@Override
	public boolean isNew(C entity) {
		return persister.isNew(entity);
	}
	
	@Override
	public Class<C> getClassToPersist() {
		return persister.getClassToPersist();
	}
	
	/**
	 * Implementation for simple one-to-one cases : we add our joins to given persister
	 * 
	 * @return created join name
	 */
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
			String mainTableJoinName = sourcePersister.getEntityJoinTree().addMergeJoin(ROOT_JOIN_NAME,
					// Note that "this" uses EntityFinder with identifier as criteria to select entities
					new FirstPhaseRelationLoader<>(this.getMapping().getIdMapping(), this,
							(ThreadLocal<Queue<Set<RelationIds<Object, C, I>>>>) (ThreadLocal) CURRENT_2PHASES_LOAD_CONTEXT),
					leftColumn,
					rightColumn,
					JoinType.OUTER);
			// adding second phase loader
			sourcePersister.addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, CURRENT_2PHASES_LOAD_CONTEXT));
			return mainTableJoinName;
		} else {
			// We use our own select system since SelectListener is not aimed at joining table
			EntityMappingAdapter<C, I, T> strategy = new EntityMappingAdapter<>(getMapping());
			String createdJoinNodeName = sourcePersister.getEntityJoinTree().addRelationJoin(
					EntityJoinTree.ROOT_JOIN_NAME,
					// because joinAsOne can be called in either case of owned relation or reversely owned relation, generics can't be set correctly,
					// so we simply cast first argument
					(EntityInflater) strategy,
					propertyAccessor,
					leftColumn,
					rightColumn,
					rightTableAlias,
					optional ? JoinType.OUTER : JoinType.INNER,
					beanRelationFixer, Collections.emptySet());
			
			copyRootJoinsTo(sourcePersister.getEntityJoinTree(), createdJoinNodeName);
			
			return createdJoinNodeName;
		}
	}
	
	/**
	 * Implementation for simple one-to-many cases : we add our joins to given persister
	 */
	@Override
	public <SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsMany(String joinName,
																							  RelationalEntityPersister<SRC, SRCID> sourcePersister,
																							  Accessor<SRC, ?> propertyAccessor,
																							  Key<T1, JOINID> leftColumn,
																							  Key<T2, JOINID> rightColumn,
																							  BeanRelationFixer<SRC, C> beanRelationFixer,
																							  @Nullable Function<ColumnedRow, Object> relationIdentifierProvider,
																							  Set<? extends Column<T2, ?>> selectableColumns,
																							  boolean optional,
																							  boolean loadSeparately) {
		if (loadSeparately) {
			String mainTableJoinName = sourcePersister.getEntityJoinTree().addMergeJoin(ROOT_JOIN_NAME,
					// Note that "this" uses EntityFinder with identifier as criteria to select entities
					new FirstPhaseRelationLoader<>(this.getMapping().getIdMapping(), this,
							(ThreadLocal<Queue<Set<RelationIds<Object, C, I>>>>) (ThreadLocal) CURRENT_2PHASES_LOAD_CONTEXT),
					leftColumn,
					rightColumn,
					JoinType.OUTER);
			// adding second phase loader
			sourcePersister.addSelectListener(new SecondPhaseRelationLoader<>(beanRelationFixer, CURRENT_2PHASES_LOAD_CONTEXT));
			return mainTableJoinName;
		} else {
			String createdJoinNodeName = sourcePersister.getEntityJoinTree().addRelationJoin(
					joinName,
					new EntityMappingAdapter<>(getMapping()),
					propertyAccessor,
					leftColumn,
					rightColumn,
					null,
					optional ? JoinType.OUTER : JoinType.INNER,
					beanRelationFixer,
					selectableColumns,
					relationIdentifierProvider);
			
			// adding our subgraph select to source persister
			copyRootJoinsTo(sourcePersister.getEntityJoinTree(), createdJoinNodeName);
			
			return createdJoinNodeName;
		}
	}
	
	@Override
	public <E, ID> void copyRootJoinsTo(EntityJoinTree<E, ID> entityJoinTree, String joinName) {
		getEntityJoinTree().projectTo(entityJoinTree, joinName);
	}
	
	@Override
	protected void doPersist(Iterable<? extends C> entities) {
		persistExecutor.persist(entities);
	}
	
	/**
	 * Overridden to implement a load by joining tables
	 *
	 * @param ids entity identifiers
	 * @return a Set of loaded entities corresponding to identifiers passed as parameter
	 */
	@Override
	protected Set<C> doSelect(Iterable<I> ids) {
		LOGGER.debug("selecting entities {}", ids);
		// Note that executor emits select listener events
		IdMapping<C, I> idMapping = persister.getMapping().getIdMapping();
		if (idMapping.getIdentifierAssembler() instanceof ComposedIdentifierAssembler) {
			// && dialect.supportTupleIn
			Map<? extends Column<?, ?>, ?> columnValues = ((ComposedIdentifierAssembler<I, ?>) idMapping.getIdentifierAssembler()).getColumnValues(ids);
			TupleIn tupleIn = TupleIn.transformBeanColumnValuesToTupleInValues(Iterables.size(ids), columnValues);
			EntityQueryCriteriaSupport<C, I> newCriteriaSupport = newCriteriaSupport();
			newCriteriaSupport.getEntityCriteriaSupport().getCriteria().and(tupleIn);
			return newCriteriaSupport.wrapIntoExecutable().execute(Accumulators.toSet());
		} else {
			ReversibleAccessor<C, I> criteriaAccessor;
			if (idMapping.getIdAccessor() instanceof AccessorWrapperIdAccessor) {
				criteriaAccessor = ((AccessorWrapperIdAccessor<C, I>) idMapping.getIdAccessor()).getIdAccessor();
			} else if (idMapping.getIdAccessor() instanceof KeyValueRecordIdMapping.KeyValueRecordIdAccessor) {
				PropertyAccessor<RecordId, ?> accessor = Accessors.accessor(RecordId::getId);
				criteriaAccessor = (ReversibleAccessor<C, I>) accessor;
			} else {
				throw new UnsupportedOperationException("Unsupported id accessor type: " + idMapping.getIdAccessor().getClass());
			}
			In<I> in = new In<>();
			Set<C> result = new HashSet<>();
			ExecutableEntityQuery<C, ?> executableEntityQuery = selectWhere().and(criteriaAccessor, in);
			Iterables.forEachChunk(
					ids,
					dialect.getInOperatorMaxSize(),
					chunks -> {},
					chunkSize -> null,	// no particular initialization to do
					(context, chunk) -> {
						in.setValue(chunk);
						result.addAll(executableEntityQuery.execute(Accumulators.toSet()));
					},
					context -> {}
			);
			return result;
		}
	}
	
	@Override
	public void doDelete(Iterable<? extends C> entities) {
		persister.delete(entities);
	}
	
	@Override
	public void doDeleteById(Iterable<? extends C> entities) {
		persister.deleteById(entities);
	}
	
	@Override
	public void doInsert(Iterable<? extends C> entities) {
		persister.insert(entities);
	}
	
	@Override
	public void doUpdateById(Iterable<? extends C> entities) {
		persister.updateById(entities);
	}
	
	@Override
	public void doUpdate(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		persister.update(differencesIterable, allColumnsStatement);
	}
}
