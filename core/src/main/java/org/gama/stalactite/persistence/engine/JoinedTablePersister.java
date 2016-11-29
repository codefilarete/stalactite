package org.gama.stalactite.persistence.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.gama.lang.collection.PairIterator;
import org.gama.stalactite.persistence.engine.listening.NoopDeleteListener;
import org.gama.stalactite.persistence.engine.listening.NoopDeleteRoughlyListener;
import org.gama.stalactite.persistence.engine.listening.NoopInsertListener;
import org.gama.stalactite.persistence.engine.listening.NoopUpdateListener;
import org.gama.stalactite.persistence.engine.listening.NoopUpdateRoughlyListener;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * Persister for entity with multiple joined tables by primary key.
 * A main table is defined by the {@link ClassMappingStrategy} passed to constructor. Complementary tables are defined
 * with {@link #addMappingStrategy(ClassMappingStrategy, Function, Column, Column)}.
 * Entity load is defined by a select that joins all tables, each {@link ClassMappingStrategy} is called to complete
 * entity loading.
 * 
 * @author Guillaume Mary
 */
public class JoinedTablePersister<T, I> extends Persister<T, I> {
	
	/** Select clause helper because of its complexity */
	private final JoinSelectExecutor<T, I> joinSelectExecutor;
	
	public JoinedTablePersister(PersistenceContext persistenceContext, ClassMappingStrategy<T, I> mainMappingStrategy) {
		this(mainMappingStrategy, persistenceContext.getDialect(), persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
	}
	
	public JoinedTablePersister(ClassMappingStrategy<T, I> mainMappingStrategy, Dialect dialect, ConnectionProvider connectionProvider, int jdbcBatchSize) {
		super(mainMappingStrategy, connectionProvider, dialect.getDmlGenerator(),
				dialect.getWriteOperationRetryer(), jdbcBatchSize, dialect.getInOperatorMaxSize());
		this.joinSelectExecutor = new JoinSelectExecutor<>(mainMappingStrategy, dialect, connectionProvider);
	}
	
	/**
	 * Add a mapping strategy to be applied for persistence. It will be called after the main strategy
	 * (passed in constructor), in order of the Collection, or in reverse order for delete actions to take into account
	 * potential foreign keys.
	 */
	public <U> void addMappingStrategy(ClassMappingStrategy<U, I> mappingStrategy, Function<Iterable<T>, Iterable<U>> additionalInstancesProvider,
									   Column leftJoinColumn, Column rightJoinColumn) {
		addInsertExecutor(mappingStrategy, additionalInstancesProvider);
		addUpdateExecutor(mappingStrategy, additionalInstancesProvider);
		addUpdateRoughlyExecutor(mappingStrategy, additionalInstancesProvider);
		addDeleteExecutor(mappingStrategy, additionalInstancesProvider);
		addDeleteRoughlyExecutor(mappingStrategy, additionalInstancesProvider);
		
		// We use our own select system since ISelectListener is not aimed at joining table
		// the addition must be done after mapping strategy addition because it needs them all
		addSelectExecutor(mappingStrategy, null, leftJoinColumn, rightJoinColumn);
	}
	
	private <U> void addInsertExecutor(ClassMappingStrategy<U, I> mappingStrategy, Function<Iterable<T>, Iterable<U>> additionalInstancesProvider) {
		InsertExecutor<U, I> insertExecutor = newInsertExecutor(mappingStrategy,
				getConnectionProvider(),
				getDmlGenerator(),
				getWriteOperationRetryer(),
				getBatchSize(),
				getInOperatorMaxSize());
		getPersisterListener().addInsertListener(new NoopInsertListener<T>() {
			@Override
			public void afterInsert(Iterable<T> iterables) {
				insertExecutor.insert(additionalInstancesProvider.apply(iterables));
			}
		});
	}
	
	private <U> void addUpdateExecutor(ClassMappingStrategy<U, I> mappingStrategy, Function<Iterable<T>, Iterable<U>> additionalInstancesProvider) {
		UpdateExecutor<U, I> updateExecutor = newUpdateExecutor(
				mappingStrategy,
				getConnectionProvider(),
				getDmlGenerator(),
				getWriteOperationRetryer(),
				getBatchSize(),
				getInOperatorMaxSize());
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
				
				updateExecutor.update(() -> pairIterator, allColumnsStatement);
			}
		});
	}
	
	private <U> void addUpdateRoughlyExecutor(ClassMappingStrategy<U, I> mappingStrategy, Function<Iterable<T>, Iterable<U>> complementaryInstancesProvider) {
		UpdateExecutor<U, I> updateExecutor = newUpdateExecutor(
				mappingStrategy,
				getConnectionProvider(),
				getDmlGenerator(),
				getWriteOperationRetryer(),
				getBatchSize(),
				getInOperatorMaxSize());
		getPersisterListener().addUpdateRouglyListener(new NoopUpdateRoughlyListener<T>() {
			@Override
			public void afterUpdateRoughly(Iterable<T> iterables) {
				updateExecutor.updateRoughly(complementaryInstancesProvider.apply(iterables));
			}
		});
	}
	
	private <U> void addDeleteExecutor(ClassMappingStrategy<U, I> mappingStrategy, Function<Iterable<T>, Iterable<U>> complementaryInstancesProvider) {
		DeleteExecutor<U, I> deleteExecutor = newDeleteExecutor(
				mappingStrategy,
				getConnectionProvider(),
				getDmlGenerator(),
				getWriteOperationRetryer(),
				getBatchSize(),
				getInOperatorMaxSize());
		getPersisterListener().addDeleteListener(new NoopDeleteListener<T>() {
			@Override
			public void beforeDelete(Iterable<T> iterables) {
				deleteExecutor.delete(complementaryInstancesProvider.apply(iterables));
			}
		});
	}
	
	private <U> void addDeleteRoughlyExecutor(ClassMappingStrategy<U, I> mappingStrategy, Function<Iterable<T>, Iterable<U>> complementaryInstancesProvider) {
		DeleteExecutor<U, I> deleteExecutor = newDeleteExecutor(
				mappingStrategy,
				getConnectionProvider(),
				getDmlGenerator(),
				getWriteOperationRetryer(),
				getBatchSize(),
				getInOperatorMaxSize());
		getPersisterListener().addDeleteRoughlyListener(new NoopDeleteRoughlyListener<T>() {
			@Override
			public void beforeDeleteRoughly(Iterable<T> iterables) {
				deleteExecutor.deleteRoughly(complementaryInstancesProvider.apply(iterables));
			}
		});
	}
	
	private <U> void addSelectExecutor(ClassMappingStrategy<U, I> mappingStrategy, Function<T, Iterable<U>> setter, Column leftJoinColumn, Column rightJoinColumn) {
		joinSelectExecutor.addComplementaryTables(mappingStrategy, setter, leftJoinColumn, rightJoinColumn);
	}
	
	/**
	 * Overriden to implement a load by joining tables
	 * @param ids entity identifiers
	 * @return a List of loaded entities corresponding to identifiers passed as parameter
	 */
	@Override
	protected List<T> doSelect(Iterable<I> ids) {
		return joinSelectExecutor.select(ids);
	}
}
