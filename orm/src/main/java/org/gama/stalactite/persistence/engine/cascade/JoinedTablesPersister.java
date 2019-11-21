package org.gama.stalactite.persistence.engine.cascade;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Nullable;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.ExecutableQuery;
import org.gama.stalactite.persistence.engine.ISelectExecutor;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.StrategyJoins;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.query.EntityCriteria;
import org.gama.stalactite.persistence.query.EntityCriteriaSupport;
import org.gama.stalactite.persistence.query.EntitySelectExecutor;
import org.gama.stalactite.persistence.query.IEntitySelectExecutor;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.AbstractRelationalOperator;
import org.gama.stalactite.query.model.CriteriaChain;
import org.gama.stalactite.sql.ConnectionProvider;

import static java.util.Collections.emptyList;

/**
 * Persister for entity with multiple joined tables with "foreign key = primary key".
 * A main table is defined by the {@link ClassMappingStrategy} passed to constructor. Complementary tables are defined
 * with {@link #addPersister(String, Persister, BeanRelationFixer, Column, Column, boolean)}.
 * Entity load is defined by a select that joins all tables, each {@link ClassMappingStrategy} is called to complete
 * entity loading.
 * 
 * @param <C> the main class to be persisted
 * @param <I> the type of main class identifiers
 * @param <T> the main target table
 * @author Guillaume Mary
 */
public class JoinedTablesPersister<C, I, T extends Table> extends Persister<C, I, T> {
	
	/** Support for {@link EntityCriteria} query execution */
	private IEntitySelectExecutor<C> entitySelectExecutor;
	/** Support for defining Entity criteria on {@link #newWhere()} */
	private final EntityCriteriaSupport<C> criteriaSupport;
	
	public JoinedTablesPersister(PersistenceContext persistenceContext, IEntityMappingStrategy<C, I, T> mainMappingStrategy) {
		this(mainMappingStrategy, persistenceContext.getDialect(), persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
	}
	
	public JoinedTablesPersister(IEntityMappingStrategy<C, I, T> mainMappingStrategy, Dialect dialect, ConnectionProvider connectionProvider, int jdbcBatchSize) {
		super(mainMappingStrategy, dialect, connectionProvider, jdbcBatchSize);
		this.criteriaSupport = new EntityCriteriaSupport<>(getMappingStrategy());
		this.entitySelectExecutor = newEntitySelectExecutor(dialect);
	}
	
	@Override
	protected ISelectExecutor<C, I> newSelectExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy, ConnectionProvider connectionProvider,
													  Dialect dialect) {
		return new JoinedStrategiesSelectExecutor<>(mappingStrategy, dialect, connectionProvider);
	}
	
	protected IEntitySelectExecutor<C> newEntitySelectExecutor(Dialect dialect) {
		return new EntitySelectExecutor<>(getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect(), getConnectionProvider(), dialect.getColumnBinderRegistry());
	}
	
	/**
	 * Gives access to the select executor for further manipulations on {@link JoinedStrategiesSelect} for advanced usage
	 * @return parent {@link org.gama.stalactite.persistence.engine.ISelectExecutor} casted as a {@link JoinedStrategiesSelectExecutor}
	 */
	public JoinedStrategiesSelectExecutor<C, I, T> getJoinedStrategiesSelectExecutor() {
		return (JoinedStrategiesSelectExecutor<C, I, T>) super.getSelectExecutor();
	}
	
	/**
	 * Adds a mapping strategy to be applied for persistence. It will be called after the main strategy
	 * (passed to constructor), in order of the Collection, or in reverse order for delete actions to take into account
	 * potential foreign keys.
	 * 
	 * @param ownerStrategyName the name of the strategy on which the mappingStrategy parameter will be added
	 * @param persister the {@link Persister} which strategy must be added
	 * @param beanRelationFixer will help to fix the relation between instance at selection time
	 * @param leftJoinColumn the column of the owning strategy to be used for joining with the newly added one (mappingStrategy parameter)
	 * @param rightJoinColumn the column of the newly added strategy to be used for joining with the owning one
	 * @param isOuterJoin true to use a left outer join (optional relation)
	 * @see JoinedStrategiesSelect#addRelationJoin(String, IEntityMappingStrategy, Column, Column, boolean, BeanRelationFixer)
	 */
	public <U, J, Z> String addPersister(String ownerStrategyName,
										 Persister<U, J, ?> persister,
										 BeanRelationFixer<Z, U> beanRelationFixer,
										 Column leftJoinColumn,
										 Column rightJoinColumn,
										 boolean isOuterJoin) {
		// We use our own select system since SelectListener is not aimed at joining table
		return getJoinedStrategiesSelectExecutor().addRelation(ownerStrategyName, persister.getMappingStrategy(), beanRelationFixer,
				leftJoinColumn, rightJoinColumn, isOuterJoin);
	}
	
	/**
	 * Overriden to implement a load by joining tables
	 * 
	 * @param ids entity identifiers
	 * @return a List of loaded entities corresponding to identifiers passed as parameter
	 */
	@Override
	protected List<C> doSelect(Iterable<I> ids) {
		return getSelectExecutor().select(ids);
	}
	
	/**
	 * Gives the {@link ClassMappingStrategy} of a join node.
	 * Node name must be known so one should have kept it from the {@link #addPersister(String, Persister, BeanRelationFixer, Column, Column, boolean)}
	 * return, else, since node naming strategy is not exposed it is not recommanded to use this method out of any test or debug purpose. 
	 * 
	 * @param nodeName a name of a added strategy
	 * @return the {@link ClassMappingStrategy} behind a join node, null if not found
	 */
	public IEntityMappingStrategy giveJoinedStrategy(String nodeName) {
		return Nullable.nullable(getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect().getStrategyJoins(nodeName)).map(StrategyJoins::getStrategy).get();
	}
	
	@Override
	public Set<Table> giveImpliedTables() {
		return getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect().giveTables();
	}
	
	private EntityCriteriaSupport<C> newWhere() {
		// we must clone the underlying support, else it would be modified for all subsequent invokations and criteria will aggregate
		return new EntityCriteriaSupport<>(criteriaSupport);
	}
	
	/**
	 * Creates a query which criteria target mapped properties.
	 * <strong>As for now aggregate result is truncated to entities returned by SQL selection : for example, if criteria on collection is used,
	 * only entities returned by SQL criteria will be loaded. This does not respect aggregate principle and should be enhanced in future.</strong>
	 * 
	 * @param getter a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute()}
	 */
	public <O> ExecutableEntityQuery<C> selectWhere(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator) {
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
	public <O> ExecutableEntityQuery<C> selectWhere(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator) {
		EntityCriteriaSupport<C> localCriteriaSupport = newWhere();
		localCriteriaSupport.and(setter, operator);
		return wrapIntoExecutable(localCriteriaSupport);
	}
	
	private ExecutableEntityQuery<C> wrapIntoExecutable(EntityCriteriaSupport<C> localCriteriaSupport) {
		MethodReferenceDispatcher methodDispatcher = new MethodReferenceDispatcher();
		return methodDispatcher
				.redirect((SerializableFunction<ExecutableQuery, List<C>>) ExecutableQuery::execute,
						() -> getPersisterListener().doWithSelectListener(emptyList(), () -> entitySelectExecutor.loadGraph(localCriteriaSupport.getCriteria())))
				.redirect(CriteriaProvider::getCriteria, localCriteriaSupport::getCriteria)
				.redirect(EntityCriteria.class, localCriteriaSupport, true)
				.build((Class<ExecutableEntityQuery<C>>) (Class) ExecutableEntityQuery.class);
	}
	
	/**
	 * Select all instances with all relations fetched.
	 * 
	 * @return all instance found in database
	 */
	public List<C> selectAll() {
		return getPersisterListener().doWithSelectListener(emptyList(), () ->
				entitySelectExecutor.loadGraph(newWhere().getCriteria())
		);
	}
	
	public EntityCriteriaSupport<C> getCriteriaSupport() {
		return criteriaSupport;
	}
	
	public void addPersisterJoins(String joinName, JoinedTablesPersister<?, I, ?> sourcePersister) {
		StrategyJoins sourceJoinsSubgraphRoot = sourcePersister.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect().getJoinsRoot();
		sourceJoinsSubgraphRoot.copyTo(getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect(), joinName);
	}
	
	/**
	 * Mashup between {@link EntityCriteria} and {@link ExecutableQuery} to make an {@link EntityCriteria} executable
	 * @param <C> type of object returned by query execution
	 */
	public interface ExecutableEntityQuery<C> extends EntityCriteria<C>, ExecutableQuery<C>, CriteriaProvider {
		
		<O> ExecutableEntityQuery<C> and(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator);
		
		<O> ExecutableEntityQuery<C> and(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator);
		
		<O> ExecutableEntityQuery<C> or(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator);
		
		<O> ExecutableEntityQuery<C> or(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator);
		
		<A, B> ExecutableEntityQuery<C> and(SerializableFunction<C, A> getter1, SerializableFunction<A, B> getter2, AbstractRelationalOperator<B> operator);
		
		<S extends Collection<A>, A, B> ExecutableEntityQuery<C> andMany(SerializableFunction<C, S> getter1, SerializableFunction<A, B> getter2, AbstractRelationalOperator<B> operator);
		
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
