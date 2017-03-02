package org.gama.stalactite.persistence.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.gama.lang.Retryer;
import org.gama.lang.collection.Iterables;
import org.gama.sql.ConnectionProvider;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;

/**
 * CRUD Persistent features dedicated to an entity class. Entry point for persistent operations on entities.
 * 
 * @author Guillaume Mary
 */
public class Persister<T, I> {
	
	private final ConnectionProvider connectionProvider;
	private final DMLGenerator dmlGenerator;
	private final Retryer writeOperationRetryer;
	private final int batchSize;
	private final int inOperatorMaxSize;
	private ClassMappingStrategy<T, I> mappingStrategy;
	private PersisterListener<T, I> persisterListener = new PersisterListener<>();
	private InsertExecutor<T, I> insertExecutor;
	private UpdateExecutor<T, I> updateExecutor;
	private DeleteExecutor<T, I> deleteExecutor;
	private SelectExecutor<T, I> selectExecutor;
	
	public Persister(PersistenceContext persistenceContext, ClassMappingStrategy<T, I> mappingStrategy) {
		this(mappingStrategy, persistenceContext.getDialect(), persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
	}
	
	public Persister(ClassMappingStrategy<T, I> mappingStrategy, Dialect dialect, ConnectionProvider connectionProvider, int jdbcBatchSize) {
		this(mappingStrategy, connectionProvider, dialect.getDmlGenerator(),
				dialect.getWriteOperationRetryer(), jdbcBatchSize, dialect.getInOperatorMaxSize());
	}
	
	protected Persister(ClassMappingStrategy<T, I> mappingStrategy, ConnectionProvider connectionProvider,
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
	
	protected <U> InsertExecutor<U, I> newInsertExecutor(ClassMappingStrategy<U, I> mappingStrategy,
													  ConnectionProvider connectionProvider,
													  DMLGenerator dmlGenerator,
													  Retryer writeOperationRetryer,
													  int jdbcBatchSize,
													  int inOperatorMaxSize) {
		return new InsertExecutor<>(mappingStrategy, connectionProvider, dmlGenerator,
				writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize);
	}
	
	protected <U> UpdateExecutor<U, I> newUpdateExecutor(ClassMappingStrategy<U, I> mappingStrategy,
													  ConnectionProvider connectionProvider,
													  DMLGenerator dmlGenerator,
													  Retryer writeOperationRetryer,
													  int jdbcBatchSize,
													  int inOperatorMaxSize) {
		return new UpdateExecutor<>(mappingStrategy, connectionProvider, dmlGenerator,
				writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize);
	}
	
	protected <U> DeleteExecutor<U, I> newDeleteExecutor(ClassMappingStrategy<U, I> mappingStrategy,
											   ConnectionProvider connectionProvider,
											   DMLGenerator dmlGenerator,
											   Retryer writeOperationRetryer,
											   int jdbcBatchSize,
											   int inOperatorMaxSize) {
		return new DeleteExecutor<>(mappingStrategy, connectionProvider, dmlGenerator,
				writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize);
	}
	
	protected <U> SelectExecutor<U, I> newSelectExecutor(ClassMappingStrategy<U, I> mappingStrategy,
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
	
	public ClassMappingStrategy<T, I> getMappingStrategy() {
		return mappingStrategy;
	}
	
	public Table getTargetTable() {
		return getMappingStrategy().getTargetTable();
	}
	
	public void setPersisterListener(PersisterListener<T, I> persisterListener) {
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
	public PersisterListener<T, I> getPersisterListener() {
		return persisterListener;
	}
	
	public InsertExecutor<T, I> getInsertExecutor() {
		return insertExecutor;
	}
	
	public UpdateExecutor<T, I> getUpdateExecutor() {
		return updateExecutor;
	}
	
	public DeleteExecutor<T, I> getDeleteExecutor() {
		return deleteExecutor;
	}
	
	public SelectExecutor<T, I> getSelectExecutor() {
		return selectExecutor;
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
	
	public int insert(Iterable<T> iterable) {
		return getPersisterListener().doWithInsertListener(iterable, () -> doInsert(iterable));
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
	public int updateRoughly(Iterable<T> iterable) {
		return getPersisterListener().doWithUpdateRouglyListener(iterable, () -> doUpdateRoughly(iterable));
	}
	
	protected int doUpdateRoughly(Iterable<T> iterable) {
		return updateExecutor.updateRoughly(iterable);
	}
	
	public int update(T modified, T unmodified, boolean allColumnsStatement) {
		return update(Collections.singletonList(new HashMap.SimpleImmutableEntry<>(modified, unmodified)), allColumnsStatement);
	}
	
	/**
	 * Update instances that has changes.
	 * Groups statements to benefit from JDBC batch. Usefull overall when allColumnsStatement
	 * is set to false.
	 *  @param differencesIterable pairs of modified-unmodified instances, used to compute differences side by side
	 * @param allColumnsStatement true if all columns must be in the SQL statement, false if only modified ones should be..
	 */
	public Integer update(Iterable<Entry<T, T>> differencesIterable, boolean allColumnsStatement) {
		return getPersisterListener().doWithUpdateListener(differencesIterable, allColumnsStatement,
				() -> doUpdate(differencesIterable, allColumnsStatement));
	}
	
	protected int doUpdate(Iterable<Entry<T, T>> differencesIterable, boolean allColumnsStatement) {
		return updateExecutor.update(differencesIterable, allColumnsStatement);
	}
	
	public int delete(T t) {
		return delete(Collections.singletonList(t));
	}
	
	public int delete(Iterable<T> iterable) {
		return getPersisterListener().doWithDeleteListener(iterable, () -> doDelete(iterable));
	}
	
	protected int doDelete(Iterable<T> iterable) {
		return deleteExecutor.delete(iterable);
	}
	
	public int deleteRougly(T t) {
		return deleteRoughly(Collections.singletonList(t));
	}
	
	public int deleteRoughly(Iterable<T> iterable) {
		return getPersisterListener().doWithDeleteRoughlyListener(iterable, () -> doDeleteRoughly(iterable));
	}
	
	protected int doDeleteRoughly(Iterable<T> iterable) {
		return deleteExecutor.deleteRoughly(iterable);
	}
	
	/**
	 * Indicates if a bean is persisted or not. Delegated to {@link ClassMappingStrategy}
	 * 
	 * @param t a bean
	 * @return true if a bean is already persisted
	 * @see ClassMappingStrategy#isNew(Object)
	 * @see org.gama.stalactite.persistence.mapping.IdMappingStrategy.IsNewDeterminer
	 */
	private boolean isNew(T t) {
		return mappingStrategy.isNew(t);
	}
	
	public T select(I id) {
		return Iterables.first(select(Collections.singleton(id)));
	}
	
	public List<T> select(Iterable<I> ids) {
		if (!Iterables.isEmpty(ids)) {
			return getPersisterListener().doWithSelectListener(ids, () -> doSelect(ids));
		} else {
			throw new IllegalArgumentException("Non selectable entity " + mappingStrategy.getClassToPersist() + " because of null id");
		}
	}
	
	protected List<T> doSelect(Iterable<I> ids) {
		return selectExecutor.select(ids);
	}
}
