package org.gama.stalactite.persistence.engine.cascade;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.gama.lang.collection.PairIterator;
import org.gama.sql.ConnectionProvider;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.NoopUpdateListener;
import org.gama.stalactite.persistence.engine.listening.NoopUpdateRoughlyListener;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * Persister for entity with multiple joined tables by primary key.
 * A main table is defined by the {@link ClassMappingStrategy} passed to constructor. Complementary tables are defined
 * with {@link #addPersister(String, Persister, BeanRelationFixer, Column, Column, boolean)}.
 * Entity load is defined by a select that joins all tables, each {@link ClassMappingStrategy} is called to complete
 * entity loading.
 * 
 * @author Guillaume Mary
 */
public class JoinedTablesPersister<T extends Identified, I extends StatefullIdentifier> extends Persister<T, I> {
	
	/** Select clause helper because of its complexity */
	private final JoinedStrategiesSelectExecutor<T, I> joinedStrategiesSelectExecutor;
	
	public JoinedTablesPersister(PersistenceContext persistenceContext, ClassMappingStrategy<T, I> mainMappingStrategy) {
		this(mainMappingStrategy, persistenceContext.getDialect(), persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
	}
	
	public JoinedTablesPersister(ClassMappingStrategy<T, I> mainMappingStrategy, Dialect dialect, ConnectionProvider connectionProvider, int jdbcBatchSize) {
		super(mainMappingStrategy, connectionProvider, dialect.getDmlGenerator(),
				dialect.getWriteOperationRetryer(), jdbcBatchSize, dialect.getInOperatorMaxSize());
		this.joinedStrategiesSelectExecutor = new JoinedStrategiesSelectExecutor<>(mainMappingStrategy, dialect, connectionProvider);
	}
	
	/**
	 * Add a mapping strategy to be applied for persistence. It will be called after the main strategy
	 * (passed in constructor), in order of the Collection, or in reverse order for delete actions to take into account
	 * potential foreign keys.
	 * @param ownerStrategyName the name of the strategy on which the mappingStrategy parameter will be added
	 * @param persister the {@link Persister} who strategy must be added to be added
	 * @param beanRelationFixer will help to fix the relation between instance at selection time
	 * @param leftJoinColumn the column of the owning strategy to be used for joining with the newly added one (mappingStrategy parameter)
	 * @param rightJoinColumn the column of the newly added strategy to be used for joining with the owning one
	 * @param isOuterJoin true to use a left outer join (optional relation)
	 * @see JoinedStrategiesSelect#add(String, ClassMappingStrategy, Column, Column, boolean, BeanRelationFixer
	 */
	public <U extends Identified, J extends StatefullIdentifier> String addPersister(String ownerStrategyName, Persister<U, J> persister,
																					 BeanRelationFixer beanRelationFixer,
																					 Column leftJoinColumn, Column rightJoinColumn, boolean isOuterJoin) {
		ClassMappingStrategy<U, J> mappingStrategy = persister.getMappingStrategy();
//		addUpdateExecutor(persister, additionalInstancesProvider);
//		addUpdateRoughlyExecutor(persister, additionalInstancesProvider);
		
		// We use our own select system since ISelectListener is not aimed at joining table
		return addSelectExecutor(ownerStrategyName, mappingStrategy, beanRelationFixer, leftJoinColumn, rightJoinColumn, isOuterJoin);
	}
	
	private <U, J> void addUpdateExecutor(Persister<U, J> persister, Function<Iterable<T>, Iterable<U>> additionalInstancesProvider) {
		getPersisterListener().addUpdateListener(new NoopUpdateListener<T>() {
			@Override
			public void afterUpdate(Iterable<Map.Entry<T, T>> iterables, boolean allColumnsStatement) {
				// Creation of an Entry<U, U> iterator from the Entry<T, T> iterator by applying additionalInstancesProvider on each.
				// Not really optimized since we create 2 lists but I couldn't find better without changing method signature
				// or calling numerous time additionalInstancesProvider.apply(..) (one time per T instance)
				List<T> keysIterable = new ArrayList<>();
				List<T> valuesIterable = new ArrayList<>();
				for (Map.Entry<T, T> entry : iterables) {
					keysIterable.add(entry.getKey());
					valuesIterable.add(entry.getValue());
				}
				PairIterator<U, U> pairIterator = new PairIterator<>(additionalInstancesProvider.apply(keysIterable), additionalInstancesProvider.apply(valuesIterable));
				
				persister.update(() -> pairIterator, allColumnsStatement);
			}
		});
	}
	
	private <U, J> void addUpdateRoughlyExecutor(Persister<U, J>persister, Function<Iterable<T>, Iterable<U>> complementaryInstancesProvider) {
		getPersisterListener().addUpdateRouglyListener(new NoopUpdateRoughlyListener<T>() {
			@Override
			public void afterUpdateRoughly(Iterable<T> iterables) {
				persister.updateRoughly(complementaryInstancesProvider.apply(iterables));
			}
		});
	}
	
	private <U, J> String addSelectExecutor(String leftStrategyName, ClassMappingStrategy<U, J> mappingStrategy, BeanRelationFixer beanRelationFixer,
										 Column leftJoinColumn, Column rightJoinColumn, boolean isOuterJoin) {
		return joinedStrategiesSelectExecutor.addComplementaryTables(leftStrategyName, mappingStrategy, beanRelationFixer,
				leftJoinColumn, rightJoinColumn, isOuterJoin);
	}
	
	/**
	 * Overriden to implement a load by joining tables
	 * @param ids entity identifiers
	 * @return a List of loaded entities corresponding to identifiers passed as parameter
	 */
	@Override
	protected List<T> doSelect(Iterable<I> ids) {
		return joinedStrategiesSelectExecutor.select(ids);
	}
}
