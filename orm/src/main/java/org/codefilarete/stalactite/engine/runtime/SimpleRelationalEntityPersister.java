package org.codefilarete.stalactite.engine.runtime;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.PersistExecutor;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.query.EntityFinder;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Duo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persister that registers relations of entities joined on "foreign key = primary key".
 * This does not handle inheritance nor entities mapped on several tables, it focuses on select part : a main table is defined by
 * {@link ClassMapping} passed to constructor which then it can be added to some other {@link RelationalEntityPersister} thanks to
 * {@link RelationalEntityPersister#joinAsMany(RelationalEntityPersister, Key, Key, BeanRelationFixer, Function, String, boolean, boolean)} and
 * {@link RelationalEntityPersister#joinAsOne(RelationalEntityPersister, Key, Key, String, BeanRelationFixer, boolean, boolean)}.
 * 
 * Entity load is defined by a select that joins all tables, each {@link ClassMapping} is called to complete
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
	
	private final BeanPersister<C, I, T> persister;
	/** Support for {@link EntityCriteria} query execution */
	private final EntityFinder<C, I> entityFinder;
	/** Support for defining entity criteria on {@link #selectWhere()} */
	private final EntityCriteriaSupport<C> criteriaSupport;
	private final EntityMappingTreeSelectExecutor<C, I, T> selectGraphExecutor;
	private final PersistExecutor<C> persistExecutor;
	
	public SimpleRelationalEntityPersister(ClassMapping<C, I, T> mainMappingStrategy,
										   Dialect dialect,
										   ConnectionConfiguration connectionConfiguration) {
		this(new BeanPersister<>(mainMappingStrategy, dialect, connectionConfiguration), dialect, connectionConfiguration);
	}
	
	public SimpleRelationalEntityPersister(BeanPersister<C, I, T> persister, Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.persister = persister;
		this.criteriaSupport = new EntityCriteriaSupport<>(persister.getMapping());
		this.selectGraphExecutor = newSelectExecutor(persister.getMapping(), connectionConfiguration.getConnectionProvider(), dialect);
		this.entityFinder = newEntitySelectExecutor(dialect);
		if (persister.getMapping().getIdMapping().getIdentifierInsertionManager() instanceof AlreadyAssignedIdentifierManager) {
			// we redirect all invocations to ourselves because targeted methods invoke their listeners
			this.persistExecutor = new AlreadyAssignedIdentifierPersistExecutor<>(this);
		} else {
			// we redirect all invocations to ourselves because targeted methods invoke their listeners
			this.persistExecutor = new DefaultPersistExecutor<>(this);
		}
	}
	
	protected EntityMappingTreeSelectExecutor<C, I, T> newSelectExecutor(EntityMapping<C, I, T> mappingStrategy,
																		 ConnectionProvider connectionProvider,
																		 Dialect dialect) {
		return new EntityMappingTreeSelectExecutor<>(mappingStrategy, dialect, connectionProvider);
	}
	
	protected EntityFinder<C, I> newEntitySelectExecutor(Dialect dialect) {
		return new EntityGraphSelector<>(
				getEntityJoinTree(),
				persister.getConnectionProvider(),
				dialect);
	}
	
	@Override
	public Column getColumn(List<? extends ValueAccessPoint<?>> accessorChain) {
		return criteriaSupport.getRootConfiguration().giveColumn(accessorChain);
	}
	
	/**
	 * Gives access to the select executor for further manipulations on {@link EntityJoinTree} for advanced usage
	 * 
	 * @return the executor for whole entity graph loading
	 */
	public EntityMappingTreeSelectExecutor<C, I, T> getEntityMappingTreeSelectExecutor() {
		return this.selectGraphExecutor;
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
	
	public EntityMappingTreeSelectExecutor<C, I, T> getSelectExecutor() {
		return this.selectGraphExecutor;
	}
	
	public DeleteExecutor<C, I, T> getDeleteExecutor() {
		return persister.getDeleteExecutor();
	}
	
	@Override
	public EntityJoinTree<C, I> getEntityJoinTree() {
		return getEntityMappingTreeSelectExecutor().getEntityJoinTree();
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
		return new EntityQueryCriteriaSupport<>(criteriaSupport, entityFinder, getPersisterListener());
	}
	
	@Override
	public ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<Select> selectAdapter) {
		ProjectionQueryCriteriaSupport<C, I> projectionSupport = new ProjectionQueryCriteriaSupport<>(criteriaSupport, entityFinder, selectAdapter);
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
	
	@Override
	public void registerRelation(ValueAccessPoint<C> relation, ConfiguredRelationalPersister<?, ?> persister) {
		criteriaSupport.registerRelation(relation, persister);
	}
	
	/**
	 * Implementation for simple one-to-one cases : we add our joins to given persister
	 * 
	 * @return created join name
	 */
	@Override
	public <SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsOne(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																							 Key<T1, JOINID> leftColumn,
																							 Key<T2, JOINID> rightColumn,
																							 String rightTableAlias,
																							 BeanRelationFixer<SRC, C> beanRelationFixer,
																							 boolean optional,
																							 boolean loadSeparately) {
		
		// We use our own select system since SelectListener is not aimed at joining table
		EntityMappingAdapter<C, I, T> strategy = new EntityMappingAdapter<>(getMapping());
		String createdJoinNodeName = sourcePersister.getEntityJoinTree().addRelationJoin(
				EntityJoinTree.ROOT_STRATEGY_NAME,
				// because joinAsOne can be called in either case of owned-relation or reversly-owned-relation, generics can't be set correctly,
				// so we simply cast first argument
				(EntityInflater) strategy,
				leftColumn,
				rightColumn,
				rightTableAlias,
				optional ? JoinType.OUTER : JoinType.INNER,
				beanRelationFixer,
				Collections.emptySet());
		
		copyRootJoinsTo(sourcePersister.getEntityJoinTree(), createdJoinNodeName);
		
		return createdJoinNodeName;
	}
	
	/**
	 * Implementation for simple one-to-many cases : we add our joins to given persister
	 */
	@Override
	public <SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsMany(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																							  Key<T1, JOINID> leftColumn,
																							  Key<T2, JOINID> rightColumn,
																							  BeanRelationFixer<SRC, C> beanRelationFixer,
																							  @Nullable Function<ColumnedRow, Object> relationIdentifierProvider,
																							  String joinName,
																							  Set<? extends Column<T2, ?>> selectableColumns,
																							  boolean optional,
																							  boolean loadSeparately) {
		
		EntityMappingAdapter<C, I, T> strategy = new EntityMappingAdapter<>(getMapping());
		String createdJoinNodeName = sourcePersister.getEntityJoinTree().addRelationJoin(
				joinName,
				strategy,
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
		return selectGraphExecutor.select(ids);
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
