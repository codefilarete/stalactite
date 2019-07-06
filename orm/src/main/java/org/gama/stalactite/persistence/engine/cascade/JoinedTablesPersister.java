package org.gama.stalactite.persistence.engine.cascade;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Nullable;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.sql.ConnectionProvider;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.ExecutableQuery;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.SelectExecutor;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.StrategyJoins;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.query.EntityCriteria;
import org.gama.stalactite.persistence.query.EntityCriteriaSupport;
import org.gama.stalactite.persistence.query.EntitySelectExecutor;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.AbstractRelationalOperator;

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
	
	/** Select clause helper because of its complexity */
	private final JoinedStrategiesSelectExecutor<C, I, T> joinedStrategiesSelectExecutor;
	/** Support for {@link EntityCriteria} query execution */
	private final EntitySelectExecutor<C, I, T> entitySelectExecutor;
	/** Support for defining Entity criteria on {@link #newWhere()} */
	private final EntityCriteriaSupport<C> criteriaSupport;
	
	public JoinedTablesPersister(PersistenceContext persistenceContext, ClassMappingStrategy<C, I, T> mainMappingStrategy) {
		this(mainMappingStrategy, persistenceContext.getDialect(), persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
	}
	
	public JoinedTablesPersister(ClassMappingStrategy<C, I, T> mainMappingStrategy, Dialect dialect, ConnectionProvider connectionProvider, int jdbcBatchSize) {
		super(mainMappingStrategy, connectionProvider, dialect.getDmlGenerator(),
				dialect.getWriteOperationRetryer(), jdbcBatchSize, dialect.getInOperatorMaxSize());
		this.joinedStrategiesSelectExecutor = new JoinedStrategiesSelectExecutor<>(mainMappingStrategy, dialect, connectionProvider);
		this.entitySelectExecutor = new EntitySelectExecutor<>(joinedStrategiesSelectExecutor.getJoinedStrategiesSelect(), getConnectionProvider(),
				dialect.getColumnBinderRegistry());
		this.criteriaSupport = new EntityCriteriaSupport<>(getMappingStrategy());
	}
	
	@Override
	protected <U> SelectExecutor<U, I, T> newSelectExecutor(ClassMappingStrategy<U, I, T> mappingStrategy, ConnectionProvider connectionProvider,
															DMLGenerator dmlGenerator, int inOperatorMaxSize) {
		// since getSelectExecutor() is overriden we don't care about returning a good instance, actually it implies that EntitySelectExecutor
		// is a subtype of SelectExecutor which is not the case, and can hardly be the case
		return null;
	}
	
	/**
	 * Gives access to the select executor for further manipulations on {@link JoinedStrategiesSelect} for advanced usage
	 * @return never null
	 */
	@Nonnull
	public JoinedStrategiesSelectExecutor<C, I, T> getJoinedStrategiesSelectExecutor() {
		return joinedStrategiesSelectExecutor;
	}
	
	@Override
	public JoinedStrategiesSelectExecutor<C, I, T> getSelectExecutor() {
		return joinedStrategiesSelectExecutor;
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
	 * @see JoinedStrategiesSelect#add(String, ClassMappingStrategy, Column, Column, boolean, BeanRelationFixer)
	 */
	public <U, J, Z> String addPersister(String ownerStrategyName,
										 Persister<U, J, ?> persister,
										 BeanRelationFixer<Z, U> beanRelationFixer,
										 Column leftJoinColumn,
										 Column rightJoinColumn,
										 boolean isOuterJoin) {
		ClassMappingStrategy<U, J, ?> mappingStrategy = persister.getMappingStrategy();
		
		// We use our own select system since SelectListener is not aimed at joining table
		return joinedStrategiesSelectExecutor.addComplementaryTable(ownerStrategyName, mappingStrategy, beanRelationFixer,
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
		return joinedStrategiesSelectExecutor.select(ids);
	}
	
	/**
	 * Gives the {@link ClassMappingStrategy} of a join node.
	 * Node name must be known so one should have kept it from the {@link #addPersister(String, Persister, BeanRelationFixer, Column, Column, boolean)}
	 * return, else, since node naming strategy is not exposed it is not recommanded to use this method out of any test or debug purpose. 
	 * 
	 * @param nodeName a name of a added strategy
	 * @return the {@link ClassMappingStrategy} behind a join node, null if not found
	 */
	public ClassMappingStrategy giveJoinedStrategy(String nodeName) {
		return Nullable.nullable(joinedStrategiesSelectExecutor.getJoinedStrategiesSelect().getStrategyJoins(nodeName)).map(StrategyJoins::getStrategy).get();
	}
	
	@Override
	public Set<Table> giveImpliedTables() {
		return joinedStrategiesSelectExecutor.getJoinedStrategiesSelect().giveTables();
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
		SerializableFunction<ExecutableQuery, List<C>> execute = ExecutableQuery::execute;
		return methodDispatcher
				.redirect(execute, () -> getPersisterListener().doWithSelectListener(emptyList(), () -> entitySelectExecutor.loadGraph(localCriteriaSupport)))
				.redirect(EntityCriteria.class, localCriteriaSupport)
				.build((Class<ExecutableEntityQuery<C>>) (Class) ExecutableEntityQuery.class);
	}
	
	/**
	 * Select all instances with all relations fetched.
	 * 
	 * @return all instance found in database
	 */
	public List<C> selectAll() {
		return getPersisterListener().doWithSelectListener(emptyList(), () ->
				entitySelectExecutor.loadGraph(newWhere())
		);
	}
	
	public EntityCriteriaSupport<C> getCriteriaSupport() {
		return criteriaSupport;
	}
	
	/**
	 * Mashup between {@link EntityCriteria} and {@link ExecutableQuery} to make an {@link EntityCriteria} executable
	 * @param <C> type of object returned by query execution
	 */
	public interface ExecutableEntityQuery<C> extends EntityCriteria<C>, ExecutableQuery<C> {
		
	}
}
