package org.gama.stalactite.persistence.engine.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Duo;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.stalactite.persistence.engine.ExecutableQuery;
import org.gama.stalactite.persistence.engine.ISelectExecutor;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.runtime.EntityMappingStrategyTreeJoinPoint.JoinType;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.query.EntityCriteriaSupport;
import org.gama.stalactite.persistence.query.EntitySelectExecutor;
import org.gama.stalactite.persistence.query.IEntitySelectExecutor;
import org.gama.stalactite.persistence.query.RelationalEntityCriteria;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.IConnectionConfiguration;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.AbstractRelationalOperator;
import org.gama.stalactite.query.model.CriteriaChain;
import org.gama.stalactite.sql.ConnectionProvider;

import static java.util.Collections.emptyList;

/**
 * Persister that registers relations of entities joined on "foreign key = primary key".
 * This does not handle inheritance nor entities mapped on several tables, it focuses on select part : a main table is defined by the
 * {@link ClassMappingStrategy} passed to constructor and then it can be added to some other {@link IJoinedTablesPersister} thanks to
 * {@link #joinAsMany(IJoinedTablesPersister, Column, Column, BeanRelationFixer, String, boolean)} and
 * {@link #joinAsOne(IJoinedTablesPersister, Column, Column, BeanRelationFixer, boolean)}.
 * 
 * Entity load is defined by a select that joins all tables, each {@link ClassMappingStrategy} is called to complete
 * entity loading.
 * 
 * In the orm module this class replace {@link Persister} in case of single table, because it has methods for join support whereas {@link Persister}
 * doesn't.
 * 
 * @param <C> the main class to be persisted
 * @param <I> the type of main class identifiers
 * @param <T> the main target table
 * @author Guillaume Mary
 */
public class JoinedTablesPersister<C, I, T extends Table> implements IEntityConfiguredJoinedTablesPersister<C, I> {
	
	private final Persister<C, I, T> persister;
	/** Support for {@link EntityCriteria} query execution */
	private final IEntitySelectExecutor<C> entitySelectExecutor;
	/** Support for defining Entity criteria on {@link #newWhere()} */
	private final EntityCriteriaSupport<C> criteriaSupport;
	private final EntityMappingStrategyTreeSelectExecutor<C, I, T> selectGraphExecutor;
	
	public JoinedTablesPersister(PersistenceContext persistenceContext, IEntityMappingStrategy<C, I, T> mainMappingStrategy) {
		this(mainMappingStrategy, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
	}
	
	public JoinedTablesPersister(IEntityMappingStrategy<C, I, T> mainMappingStrategy, Dialect dialect, IConnectionConfiguration connectionConfiguration) {
		this.persister = new Persister<>(mainMappingStrategy, dialect, connectionConfiguration);
		this.criteriaSupport = new EntityCriteriaSupport<>(getMappingStrategy());
		this.selectGraphExecutor = newSelectExecutor(mainMappingStrategy, connectionConfiguration.getConnectionProvider(), dialect);
		this.entitySelectExecutor = newEntitySelectExecutor(dialect);
	}
	
	protected EntityMappingStrategyTreeSelectExecutor<C, I, T> newSelectExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
																				 ConnectionProvider connectionProvider,
													  							 Dialect dialect) {
		return new EntityMappingStrategyTreeSelectExecutor<>(mappingStrategy, dialect, connectionProvider);
	}
	
	protected IEntitySelectExecutor<C> newEntitySelectExecutor(Dialect dialect) {
		return new EntitySelectExecutor<>(getEntityMappingStrategyTreeSelectExecutor().getEntityMappingStrategyTreeSelectBuilder(),
				persister.getConnectionProvider(), dialect.getColumnBinderRegistry());
	}
	
	/**
	 * Gives access to the select executor for further manipulations on {@link EntityMappingStrategyTreeSelectBuilder} for advanced usage
	 * @return parent {@link org.gama.stalactite.persistence.engine.ISelectExecutor} casted as a {@link EntityMappingStrategyTreeSelectExecutor}
	 */
	public EntityMappingStrategyTreeSelectExecutor<C, I, T> getEntityMappingStrategyTreeSelectExecutor() {
		return this.selectGraphExecutor;
	}
	
	public T getMainTable() {
		return this.persister.getMainTable();
	}
	
	public InsertExecutor<C, I, T> getInsertExecutor() {
		return persister.getInsertExecutor();
	}
	
	public UpdateExecutor<C, I, T> getUpdateExecutor() {
		return persister.getUpdateExecutor();
	}
	
	public ISelectExecutor<C, I> getSelectExecutor() {
		return this.selectGraphExecutor;
	}
	
	public DeleteExecutor<C, I, T> getDeleteExecutor() {
		return persister.getDeleteExecutor();
	}
	
	@Override
	public EntityMappingStrategyTreeSelectBuilder<C, I, ?> getEntityMappingStrategyTreeSelectBuilder() {
		return getEntityMappingStrategyTreeSelectExecutor().getEntityMappingStrategyTreeSelectBuilder();
	}
	
	@Override
	public List<C> select(Iterable<I> ids) {
		if (Iterables.isEmpty(ids)) {
			return new ArrayList<>();
		} else {
			return getPersisterListener().doWithSelectListener(ids, () -> doSelect(ids));
		}
	}
	
	/**
	 * Overriden to implement a load by joining tables
	 * 
	 * @param ids entity identifiers
	 * @return a List of loaded entities corresponding to identifiers passed as parameter
	 */
	protected List<C> doSelect(Iterable<I> ids) {
		return selectGraphExecutor.select(ids);
	}
	
	@Override
	public IEntityMappingStrategy<C, I, T> getMappingStrategy() {
		return persister.getMappingStrategy();
	}
	
	@Override
	public Set<Table> giveImpliedTables() {
		return getEntityMappingStrategyTreeSelectExecutor().getEntityMappingStrategyTreeSelectBuilder().giveTables();
	}
	
	@Override
	public PersisterListener<C, I> getPersisterListener() {
		return persister.getPersisterListener();
	}
	
	private EntityCriteriaSupport<C> newWhere() {
		// we must clone the underlying support, else it would be modified for all subsequent invokations and criteria will aggregate
		return new EntityCriteriaSupport<>(criteriaSupport);
	}
	
	@Override
	public int persist(Iterable<? extends C> entities) {
		return persister.persist(entities);
	}
	
	/**
	 * Creates a query which criteria target mapped properties.
	 * <strong>As for now aggregate result is truncated to entities returned by SQL selection : for example, if criteria on collection is used,
	 * only entities returned by SQL criteria will be loaded. This does not respect aggregate principle and should be enhanced in future.</strong>
	 * 
	 * @param <O> value type returned by property accessor
	 * @param getter a property accessor
	 * @param operator criteria for the property
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute()}
	 */
	@Override
	public <O> RelationalExecutableEntityQuery<C> selectWhere(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator) {
		EntityCriteriaSupport<C> localCriteriaSupport = newWhere();
		localCriteriaSupport.and(getter, operator);
		return wrapIntoExecutable(localCriteriaSupport);
	}
	
	/**
	 * Creates a query which criteria target mapped properties
	 * <strong>As for now aggregate result is truncated to entities returned by SQL selection : for example, if criteria on collection is used,
	 * only entities returned by SQL criteria will be loaded. This does not respect aggregate principle and should be enhanced in future.</strong>
	 *
	 * @param setter a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute()}
	 */
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
						() -> getPersisterListener().doWithSelectListener(emptyList(), () -> entitySelectExecutor.loadGraph(localCriteriaSupport.getCriteria())))
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
	public List<C> selectAll() {
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
	 */
	@Override
	public <SRC, T1 extends Table, T2 extends Table> void joinAsOne(IJoinedTablesPersister<SRC, I> sourcePersister,
								Column<T1, I> leftColumn, Column<T2, I> rightColumn, BeanRelationFixer<SRC, C> beanRelationFixer, boolean optional) {
		
		// We use our own select system since SelectListener is not aimed at joining table
		String createdJoinNodeName = sourcePersister.getEntityMappingStrategyTreeSelectBuilder().addRelationJoin(
				EntityMappingStrategyTreeSelectBuilder.ROOT_STRATEGY_NAME,
				(IEntityMappingStrategy) this.getMappingStrategy(),
				leftColumn,
				rightColumn,
				optional ? JoinType.OUTER : JoinType.INNER,
				beanRelationFixer);
		
		copyJoinsRootTo(sourcePersister.getEntityMappingStrategyTreeSelectBuilder(), createdJoinNodeName);
	}
	
	/**
	 * Implementation for simple one-to-many cases : we add our joins to given persister
	 */
	@Override
	public <SRC, T1 extends Table, T2 extends Table, J> void joinAsMany(IJoinedTablesPersister<SRC, J> sourcePersister,
																	 Column<T1, J> leftColumn, Column<T2, J> rightColumn,
																	 BeanRelationFixer<SRC, C> beanRelationFixer, String joinName, boolean optional) {
		
		String createdJoinNodeName = sourcePersister.getEntityMappingStrategyTreeSelectBuilder().addRelationJoin(
				joinName,
				(IEntityMappingStrategy) getMappingStrategy(),
				leftColumn,
				rightColumn,
				optional ? JoinType.OUTER : JoinType.INNER,
				beanRelationFixer);
		
		// adding our subgraph select to source persister
		copyJoinsRootTo(sourcePersister.getEntityMappingStrategyTreeSelectBuilder(), createdJoinNodeName);
	}
	
	@Override
	public <E, ID, T extends Table> void copyJoinsRootTo(EntityMappingStrategyTreeSelectBuilder<E, ID, T> entityMappingStrategyTreeSelectBuilder, String joinName) {
		getEntityMappingStrategyTreeSelectBuilder().getRoot().copyTo(entityMappingStrategyTreeSelectBuilder, joinName);
	}
	
	@Override
	public int delete(Iterable<C> entities) {
		return persister.delete(entities);
	}
	
	@Override
	public int deleteById(Iterable<C> entities) {
		return persister.deleteById(entities);
	}
	
	@Override
	public int insert(Iterable<? extends C> entities) {
		return persister.insert(entities);
	}
	
	@Override
	public int updateById(Iterable<C> entities) {
		return persister.updateById(entities);
	}
	
	@Override
	public int update(Iterable<? extends Duo<? extends C, ? extends C>> differencesIterable, boolean allColumnsStatement) {
		return persister.update(differencesIterable, allColumnsStatement);
	}
	
	@Override
	public void addInsertListener(InsertListener<C> insertListener) {
		persister.addInsertListener(insertListener);
	}
	
	@Override
	public void addUpdateListener(UpdateListener<C> updateListener) {
		persister.addUpdateListener(updateListener);
	}
	
	@Override
	public void addSelectListener(SelectListener<C, I> selectListener) {
		persister.addSelectListener(selectListener);
	}
	
	@Override
	public void addDeleteListener(DeleteListener<C> deleteListener) {
		persister.addDeleteListener(deleteListener);
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener<C> deleteListener) {
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
