package org.gama.stalactite.persistence.engine;

import org.gama.lang.Retryer;
import org.gama.lang.bean.ISilentDelegate;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * CRUD Persistent features dedicated to an entity class. Entry point for persistent operations on entities.
 * 
 * @author Guillaume Mary
 */
public class Persister<T> {
	
	private final ConnectionProvider connectionProvider;
	private final DMLGenerator dmlGenerator;
	private final Retryer writeOperationRetryer;
	private final int batchSize;
	private final int inOperatorMaxSize;
	private ClassMappingStrategy<T> mappingStrategy;
	private PersisterListener<T> persisterListener = new PersisterListener<>();
	private InsertExecutor<T> insertExecutor;
	private UpdateExecutor<T> updateExecutor;
	private DeleteExecutor<T> deleteExecutor;
	private SelectExecutor<T> selectExecutor;
	
	public Persister(PersistenceContext persistenceContext, ClassMappingStrategy<T> mappingStrategy) {
		this(mappingStrategy, persistenceContext.getDialect(), persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
	}
	
	public Persister(ClassMappingStrategy<T> mappingStrategy, Dialect dialect, ConnectionProvider connectionProvider, int jdbcBatchSize) {
		this(mappingStrategy, connectionProvider, dialect.getDmlGenerator(),
				dialect.getWriteOperationRetryer(), jdbcBatchSize, dialect.getInOperatorMaxSize());
	}
	
	protected Persister(ClassMappingStrategy<T> mappingStrategy, ConnectionProvider connectionProvider,
						DMLGenerator dmlGenerator, Retryer writeOperationRetryer, int jdbcBatchSize, int inOperatorMaxSize) {
		this.mappingStrategy = mappingStrategy;
		this.connectionProvider = connectionProvider;
		this.dmlGenerator = dmlGenerator;
		this.writeOperationRetryer = writeOperationRetryer;
		this.batchSize = jdbcBatchSize;
		this.inOperatorMaxSize = inOperatorMaxSize;
		this.insertExecutor = newInsertExecutor(mappingStrategy, connectionProvider, dmlGenerator,
				writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize);
		this.updateExecutor = newUpdateExecutor(mappingStrategy, connectionProvider, dmlGenerator,
				writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize);
		this.deleteExecutor = newDeleteExecutor(mappingStrategy, connectionProvider, dmlGenerator,
				writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize);
		this.selectExecutor = newSelectExecutor(mappingStrategy, connectionProvider, dmlGenerator, inOperatorMaxSize);
	}
	
	protected <U> InsertExecutor<U> newInsertExecutor(ClassMappingStrategy<U> mappingStrategy,
													  ConnectionProvider connectionProvider,
													  DMLGenerator dmlGenerator,
													  Retryer writeOperationRetryer,
													  int jdbcBatchSize,
													  int inOperatorMaxSize) {
		return new InsertExecutor<>(mappingStrategy, connectionProvider, dmlGenerator,
				writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize);
	}
	
	protected <U> UpdateExecutor<U> newUpdateExecutor(ClassMappingStrategy<U> mappingStrategy,
													  ConnectionProvider connectionProvider,
													  DMLGenerator dmlGenerator,
													  Retryer writeOperationRetryer,
													  int jdbcBatchSize,
													  int inOperatorMaxSize) {
		return new UpdateExecutor<>(mappingStrategy, connectionProvider, dmlGenerator,
				writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize);
	}
	
	protected <U> DeleteExecutor<U> newDeleteExecutor(ClassMappingStrategy<U> mappingStrategy,
											   ConnectionProvider connectionProvider,
											   DMLGenerator dmlGenerator,
											   Retryer writeOperationRetryer,
											   int jdbcBatchSize,
											   int inOperatorMaxSize) {
		return new DeleteExecutor<>(mappingStrategy, connectionProvider, dmlGenerator,
				writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize);
	}
	
	protected <U> SelectExecutor<U> newSelectExecutor(ClassMappingStrategy<U> mappingStrategy,
												ConnectionProvider connectionProvider,
												DMLGenerator dmlGenerator,
												int inOperatorMaxSize) {
		return new SelectExecutor<>(mappingStrategy, connectionProvider, dmlGenerator, inOperatorMaxSize);
	}
	
	public ConnectionProvider getConnectionProvider() {
		return connectionProvider;
	}
	
	public DMLGenerator getDmlGenerator() {
		return dmlGenerator;
	}
	
	public Retryer getWriteOperationRetryer() {
		return writeOperationRetryer;
	}
	
	public int getBatchSize() {
		return batchSize;
	}
	
	public int getInOperatorMaxSize() {
		return inOperatorMaxSize;
	}
	
	public ClassMappingStrategy<T> getMappingStrategy() {
		return mappingStrategy;
	}
	
	public Table getTargetTable() {
		return getMappingStrategy().getTargetTable();
	}
	
	public void setPersisterListener(PersisterListener<T> persisterListener) {
		// prevent from null instance
		if (persisterListener != null) {
			this.persisterListener = persisterListener;
		}
	}
	
	/**
	 * should never be null for simplicity (skip "if" on each CRUD method)
	 * 
	 * @return not null
	 */
	public PersisterListener<T> getPersisterListener() {
		return persisterListener;
	}
	
	public void persist(T t) {
		// determine insert or update operation
		if (isNew(t)) {
			insert(t);
		} else {
			updateRoughly(t);
		}
	}
	
	public void persist(Iterable<T> iterable) {
		// determine insert or update operation
		List<T> toInsert = new ArrayList<>(20);
		List<T> toUpdate = new ArrayList<>(20);
		for (T t : iterable) {
			if (isNew(t)) {
				toInsert.add(t);
			} else {
				toUpdate.add(t);
			}
		}
		if (!toInsert.isEmpty()) {
			insert(toInsert);
		}
		if (!toUpdate.isEmpty()) {
			updateRoughly(toUpdate);
		}
	}
	
	public int insert(T t) {
		return insert(Collections.singletonList(t));
	}
	
	public int insert(final Iterable<T> iterable) {
		return getPersisterListener().doWithInsertListener(iterable, new ISilentDelegate<Integer>() {
			@Override
			public Integer execute() {
				return doInsert(iterable);
			}
		});
	}
	
	protected int doInsert(Iterable<T> iterable) {
		return insertExecutor.insert(iterable);
	}
	
	public void updateRoughly(T t) {
		updateRoughly(Collections.singletonList(t));
	}
	
	/**
	 * Update roughly some instances: no difference are computed, only update statements (full column) are applied.
	 * @param iterable iterable of instances
	 */
	public int updateRoughly(final Iterable<T> iterable) {
		return getPersisterListener().doWithUpdateRouglyListener(iterable, new ISilentDelegate<Integer>() {
			@Override
			public Integer execute() {
				return doUpdateRoughly(iterable);
			}
		});
	}
	
	protected int doUpdateRoughly(Iterable<T> iterable) {
		return updateExecutor.updateRoughly(iterable);
	}
	
	public int update(T modified, T unmodified, boolean allColumnsStatement) {
		return update(Collections.singletonList((Entry<T, T>) new HashMap.SimpleImmutableEntry<>(modified, unmodified)), allColumnsStatement);
	}
	
	/**
	 * Update instances that has changes.
	 * Groups statements to benefit from JDBC batch. Usefull overall when allColumnsStatement
	 * is set to false.
	 *  @param differencesIterable pairs of modified-unmodified instances, used to compute differences side by side
	 * @param allColumnsStatement true if all columns must be in the SQL statement, false if only modified ones should be..
	 */
	public Integer update(final Iterable<Entry<T, T>> differencesIterable, final boolean allColumnsStatement) {
		return getPersisterListener().doWithUpdateListener(differencesIterable, allColumnsStatement, new ISilentDelegate<Integer>() {
			@Override
			public Integer execute() {
				return doUpdate(differencesIterable, allColumnsStatement);
			}
		});
	}
	
	protected int doUpdate(Iterable<Entry<T, T>> differencesIterable, boolean allColumnsStatement) {
		return updateExecutor.update(differencesIterable, allColumnsStatement);
	}
	
	public int delete(T t) {
		return delete(Collections.singletonList(t));
	}
	
	public int delete(final Iterable<T> iterable) {
		return getPersisterListener().doWithDeleteListener(iterable, new ISilentDelegate<Integer>() {
			@Override
			public Integer execute() {
				return doDelete(iterable);
			}
		});
	}
	
	protected int doDelete(Iterable<T> iterable) {
		return deleteExecutor.delete(iterable);
	}
	
	/**
	 * Indicates if a bean is persisted or not, implemented on null identifier 
	 * @param t a bean
	 * @return true if bean's id is null, false otherwise
	 */
	public boolean isNew(T t) {
		return mappingStrategy.getId(t) == null;
	}
	
	public T select(final Serializable id) {
		return Iterables.first(select(Collections.singleton(id)));
	}
	
	public List<T> select(final Iterable<Serializable> ids) {
		if (!Iterables.isEmpty(ids)) {
			return getPersisterListener().doWithSelectListener(ids, new ISilentDelegate<List<T>>() {
				@Override
				public List<T> execute() {
					return doSelect(ids);
				}
			});
		} else {
			throw new IllegalArgumentException("Non selectable entity " + mappingStrategy.getClassToPersist() + " because of null id");
		}
	}
	
	protected List<T> doSelect(Iterable<Serializable> ids) {
		return selectExecutor.select(ids);
	}
}
