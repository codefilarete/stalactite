package org.gama.stalactite.persistence.engine.cascade;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;

import org.gama.sql.ConnectionProvider;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

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
public class JoinedTablesPersister<C, I, T extends Table<T>> extends Persister<C, I, T> {
	
	/** Select clause helper because of its complexity */
	private final JoinedStrategiesSelectExecutor<C, I> joinedStrategiesSelectExecutor;
	
	public JoinedTablesPersister(PersistenceContext persistenceContext, ClassMappingStrategy<C, I, T> mainMappingStrategy) {
		this(mainMappingStrategy, persistenceContext.getDialect(), persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
	}
	
	public JoinedTablesPersister(ClassMappingStrategy<C, I, T> mainMappingStrategy, Dialect dialect, ConnectionProvider connectionProvider, int jdbcBatchSize) {
		super(mainMappingStrategy, connectionProvider, dialect.getDmlGenerator(),
				dialect.getWriteOperationRetryer(), jdbcBatchSize, dialect.getInOperatorMaxSize());
		this.joinedStrategiesSelectExecutor = new JoinedStrategiesSelectExecutor<>(mainMappingStrategy, dialect, connectionProvider);
	}
	
	/**
	 * Gives access to the select executor for further manipulations on {@link JoinedStrategiesSelect} for advanced usage
	 * @return never null
	 */
	@Nonnull
	public JoinedStrategiesSelectExecutor<C, I> getJoinedStrategiesSelectExecutor() {
		return joinedStrategiesSelectExecutor;
	}
	
	/**
	 * Adds a mapping strategy to be applied for persistence. It will be called after the main strategy
	 * (passed to constructor), in order of the Collection, or in reverse order for delete actions to take into account
	 * potential foreign keys.
	 * 
	 * @param ownerStrategyName the name of the strategy on which the mappingStrategy parameter will be added
	 * @param persister the {@link Persister} who strategy must be added to be added
	 * @param beanRelationFixer will help to fix the relation between instance at selection time
	 * @param leftJoinColumn the column of the owning strategy to be used for joining with the newly added one (mappingStrategy parameter)
	 * @param rightJoinColumn the column of the newly added strategy to be used for joining with the owning one
	 * @param isOuterJoin true to use a left outer join (optional relation)
	 * @see JoinedStrategiesSelect#add(String, ClassMappingStrategy, Column, Column, boolean, BeanRelationFixer)
	 */
	public <U, J, Z> String addPersister(String ownerStrategyName, Persister<U, J, ?> persister,
																BeanRelationFixer<Z, U> beanRelationFixer,
																Column leftJoinColumn, Column rightJoinColumn, boolean isOuterJoin) {
		ClassMappingStrategy<U, J, ?> mappingStrategy = persister.getMappingStrategy();
		
		// We use our own select system since ISelectListener is not aimed at joining table
		return addSelectExecutor(ownerStrategyName, mappingStrategy, beanRelationFixer, leftJoinColumn, rightJoinColumn, isOuterJoin);
	}
	
	private <U, J> String addSelectExecutor(String leftStrategyName, ClassMappingStrategy<U, J, ?> mappingStrategy, BeanRelationFixer beanRelationFixer,
										 Column leftJoinColumn, Column rightJoinColumn, boolean isOuterJoin) {
		return joinedStrategiesSelectExecutor.addComplementaryTables(leftStrategyName, mappingStrategy, beanRelationFixer,
				leftJoinColumn, rightJoinColumn, isOuterJoin);
	}
	
	/**
	 * Overriden to implement a load by joining tables
	 * 
	 * @param ids entity identifiers
	 * @return a List of loaded entities corresponding to identifiers passed as parameter
	 */
	@Override
	protected List<C> doSelect(Collection<I> ids) {
		return joinedStrategiesSelectExecutor.select(ids);
	}
}
