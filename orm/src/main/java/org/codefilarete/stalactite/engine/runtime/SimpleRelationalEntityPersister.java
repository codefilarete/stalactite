package org.codefilarete.stalactite.engine.runtime;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.stalactite.engine.ExecutableQuery;
import org.codefilarete.stalactite.engine.PersistExecutor;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersistListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateByIdListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.query.EntityGraphSelectExecutor;
import org.codefilarete.stalactite.query.EntitySelectExecutor;
import org.codefilarete.stalactite.query.RelationalEntityCriteria;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

/**
 * Persister that registers relations of entities joined on "foreign key = primary key".
 * This does not handle inheritance nor entities mapped on several tables, it focuses on select part : a main table is defined by
 * {@link ClassMapping} passed to constructor which then it can be added to some other {@link RelationalEntityPersister} thanks to
 * {@link RelationalEntityPersister#joinAsMany(RelationalEntityPersister, Key, Key, BeanRelationFixer, BiFunction, String, boolean, boolean)} and
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
public class SimpleRelationalEntityPersister<C, I, T extends Table<T>> implements ConfiguredRelationalPersister<C, I> {
	
	private final BeanPersister<C, I, T> persister;
	/** Support for {@link EntityCriteria} query execution */
	private final EntitySelectExecutor<C> entitySelectExecutor;
	/** Support for defining entity criteria on {@link #newWhere()} */
	private final EntityCriteriaSupport<C> criteriaSupport;
	private final EntityMappingTreeSelectExecutor<C, I, T> selectGraphExecutor;
	
	public SimpleRelationalEntityPersister(ClassMapping<C, I, T> mainMappingStrategy, Dialect dialect,
										   ConnectionConfiguration connectionConfiguration) {
		this(new BeanPersister<>(mainMappingStrategy, dialect, connectionConfiguration), dialect, connectionConfiguration);
	}
	
	public SimpleRelationalEntityPersister(BeanPersister<C, I, T> persister, Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.persister = persister;
		this.criteriaSupport = new EntityCriteriaSupport<>(persister.getMapping());
		this.selectGraphExecutor = newSelectExecutor(persister.getMapping(), connectionConfiguration.getConnectionProvider(), dialect);
		this.entitySelectExecutor = newEntitySelectExecutor(dialect);
	}
	
	protected EntityMappingTreeSelectExecutor<C, I, T> newSelectExecutor(EntityMapping<C, I, T> mappingStrategy,
																		 ConnectionProvider connectionProvider,
																		 Dialect dialect) {
		return new EntityMappingTreeSelectExecutor<>(mappingStrategy, dialect, connectionProvider);
	}
	
	protected EntitySelectExecutor<C> newEntitySelectExecutor(Dialect dialect) {
		return new EntityGraphSelectExecutor<>(
				selectGraphExecutor.getEntityJoinTree(),
				persister.getConnectionProvider(),
				dialect);
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
	public Set<C> select(Iterable<I> ids) {
		if (Iterables.isEmpty(ids)) {
			return new HashSet<>();
		} else {
			return getPersisterListener().doWithSelectListener(ids, () -> doSelect(ids));
		}
	}
	
	/**
	 * Overridden to implement a load by joining tables
	 * 
	 * @param ids entity identifiers
	 * @return a Set of loaded entities corresponding to identifiers passed as parameter
	 */
	protected Set<C> doSelect(Iterable<I> ids) {
		return selectGraphExecutor.select(ids);
	}
	
	@Override
	public EntityMapping<C, I, T> getMapping() {
		return persister.getMapping();
	}
	
	@Override
	public Set<Table> giveImpliedTables() {
		return getEntityJoinTree().giveTables();
	}
	
	@Override
	public PersisterListenerCollection<C, I> getPersisterListener() {
		return persister.getPersisterListener();
	}
	
	private EntityCriteriaSupport<C> newWhere() {
		// we must clone the underlying support, else it would be modified for all subsequent invocations and criteria will aggregate
		return new EntityCriteriaSupport<>(criteriaSupport);
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
						(Accumulator<C, ?, Set<C>> accumulator) -> getPersisterListener().doWithSelectListener(emptySet(), () -> entitySelectExecutor.loadGraph(localCriteriaSupport.getCriteria())))
				.redirect(CriteriaProvider::getCriteria, localCriteriaSupport::getCriteria)
				.redirect(RelationalEntityCriteria.class, localCriteriaSupport, true)
				.build((Class<RelationalExecutableEntityQuery<C>>) (Class) RelationalExecutableEntityQuery.class);
	}
	
	/**
	 * Select all instances with all relations fetched.
	 * 
	 * @return all instance found in database
	 */
	@Override
	public Set<C> selectAll() {
		return getPersisterListener().doWithSelectListener(emptyList(), () ->
				entitySelectExecutor.loadGraph(newWhere().getCriteria())
		);
	}
	
	@Override
	public boolean isNew(C entity) {
		return persister.isNew(entity);
	}
	
	@Override
	public Class<C> getClassToPersist() {
		return persister.getClassToPersist();
	}
	
	public EntityCriteriaSupport<C> getCriteriaSupport() {
		return criteriaSupport;
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
																							  @Nullable BiFunction<Row, ColumnedRow, Object> relationIdentifierProvider,
																							  String joinName,
																							  Set<? extends Column<T2, Object>> selectableColumns,
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
	public void delete(Iterable<? extends C> entities) {
		persister.delete(entities);
	}
	
	@Override
	public void deleteById(Iterable<? extends C> entities) {
		persister.deleteById(entities);
	}
	
	@Override
	public void insert(Iterable<? extends C> entities) {
		persister.insert(entities);
	}

	@Override
	public void persist(Iterable<? extends C> entities) {
		if (persister instanceof CompositeKeyedBeanPersister) {
			// TODO : review this : CompositeKeyedBeanPersister implements a doPersist() with some identifier loading mechanism for a right isNew
			// TODO : implementation but it is finally not so good because its load doesn't take into account relation, so, when caller modify any
			// TODO : related entity (one-to-one / one-to-many) they are not updated because they weren't loaded (collection is empty).
			// TODO : Below code fix it by correctly loading entities but it bypass CompositeKeyedBeanPersister algorithm which should handle it.
			// TODO : but since it is in core module it doesn't handle relation, so ... find a right solution. Don't forget to take into account
			// TODO : CompositeKeyAlreadyAssignedIdentifierInsertionManager which was made to keep track of identifier.
			getPersisterListener().doWithPersistListener(entities, () -> {
				PersistExecutor.persist(entities, this, this, this, this::getId);
			});
		} else {
			getPersisterListener().doWithPersistListener(entities, () -> {
				PersistExecutor.persist(entities, this::isNew, this, this, this, this::getId);
			});
		}
	}
	
	@Override
	public void updateById(Iterable<? extends C> entities) {
		persister.updateById(entities);
	}
	
	@Override
	public void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		persister.update(differencesIterable, allColumnsStatement);
	}
	
	@Override
	public void addPersistListener(PersistListener<? extends C> persistListener) {
		persister.addPersistListener(persistListener);
	}
	
	@Override
	public void addInsertListener(InsertListener<? extends C> insertListener) {
		persister.addInsertListener(insertListener);
	}
	
	@Override
	public void addUpdateListener(UpdateListener<? extends C> updateListener) {
		persister.addUpdateListener(updateListener);
	}
	
	@Override
	public void addUpdateByIdListener(UpdateByIdListener<? extends C> updateByIdListener) {
		persister.addUpdateByIdListener(updateByIdListener);
	}
	
	@Override
	public void addSelectListener(SelectListener<? extends C, I> selectListener) {
		persister.addSelectListener(selectListener);
	}
	
	@Override
	public void addDeleteListener(DeleteListener<? extends C> deleteListener) {
		persister.addDeleteListener(deleteListener);
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener<? extends C> deleteListener) {
		persister.addDeleteByIdListener(deleteListener);
	}
	
	/**
	 * Interface that allows access to the {@link CriteriaChain} of the {@link EntityCriteriaSupport} wrapped into the proxy returned by
	 * {@link #wrapIntoExecutable(EntityCriteriaSupport)}.
	 * Mainly created from test purpose that requires access to underlying objects
	 */
	public interface CriteriaProvider {
		
		CriteriaChain getCriteria();
		
	}
}
