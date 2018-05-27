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
public class Persister<C, I, T extends Table> {
	
	private final ConnectionProvider connectionProvider;
	private final DMLGenerator dmlGenerator;
	private final Retryer writeOperationRetryer;
	private final int batchSize;
	private final int inOperatorMaxSize;
	private ClassMappingStrategy<C, I, T> mappingStrategy;
	private PersisterListener<C, I> persisterListener = new PersisterListener<>();
	private InsertExecutor<C, I, T> insertExecutor;
	private UpdateExecutor<C, I, T> updateExecutor;
	private DeleteExecutor<C, I, T> deleteExecutor;
	private SelectExecutor<C, I, T> selectExecutor;
	
	public Persister(PersistenceContext persistenceContext, ClassMappingStrategy<C, I, T> mappingStrategy) {
		this(mappingStrategy, persistenceContext.getDialect(), persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
	}
	
	public Persister(ClassMappingStrategy<C, I, T> mappingStrategy, Dialect dialect, ConnectionProvider connectionProvider, int jdbcBatchSize) {
		this(mappingStrategy, connectionProvider, dialect.getDmlGenerator(),
				dialect.getWriteOperationRetryer(), jdbcBatchSize, dialect.getInOperatorMaxSize());
	}
	
	protected Persister(ClassMappingStrategy<C, I, T> mappingStrategy, ConnectionProvider connectionProvider,
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
	
	protected <U> InsertExecutor<U, I, T> newInsertExecutor(ClassMappingStrategy<U, I, T> mappingStrategy,
																ConnectionProvider connectionProvider,
																DMLGenerator dmlGenerator,
																Retryer writeOperationRetryer,
																int jdbcBatchSize,
																int inOperatorMaxSize) {
		return new InsertExecutor<>(mappingStrategy, connectionProvider, dmlGenerator,
				writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize);
	}
	
	protected <U> UpdateExecutor<U, I, T> newUpdateExecutor(ClassMappingStrategy<U, I, T> mappingStrategy,
													  ConnectionProvider connectionProvider,
													  DMLGenerator dmlGenerator,
													  Retryer writeOperationRetryer,
													  int jdbcBatchSize,
													  int inOperatorMaxSize) {
		return new UpdateExecutor<>(mappingStrategy, connectionProvider, dmlGenerator,
				writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize);
	}
	
	protected <U> DeleteExecutor<U, I, T> newDeleteExecutor(ClassMappingStrategy<U, I, T> mappingStrategy,
															ConnectionProvider connectionProvider,
															DMLGenerator dmlGenerator,
															Retryer writeOperationRetryer,
															int jdbcBatchSize,
															int inOperatorMaxSize) {
		return new DeleteExecutor<>(mappingStrategy, connectionProvider, dmlGenerator,
				writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize);
	}
	
	protected <U> SelectExecutor<U, I, T> newSelectExecutor(ClassMappingStrategy<U, I, T> mappingStrategy,
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
	
	public ClassMappingStrategy<C, I, T> getMappingStrategy() {
		return mappingStrategy;
	}
	
	public T getTargetTable() {
		return getMappingStrategy().getTargetTable();
	}
	
	public void setPersisterListener(PersisterListener<C, I> persisterListener) {
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
	public PersisterListener<C, I> getPersisterListener() {
		return persisterListener;
	}
	
	public InsertExecutor<C, I, T> getInsertExecutor() {
		return insertExecutor;
	}
	
	public UpdateExecutor<C, I, T> getUpdateExecutor() {
		return updateExecutor;
	}
	
	public DeleteExecutor<C, I, T> getDeleteExecutor() {
		return deleteExecutor;
	}
	
	public SelectExecutor<C, I, T> getSelectExecutor() {
		return selectExecutor;
	}
	
	public void persist(C c) {
		// determine insert or update operation
		if (isNew(c)) {
			insert(c);
		} else {
			updateById(c);
		}
	}
	
	public void persist(Iterable<C> iterable) {
		// determine insert or update operation
		List<C> toInsert = new ArrayList<>(20);
		List<C> toUpdate = new ArrayList<>(20);
		for (C c : iterable) {
			if (isNew(c)) {
				toInsert.add(c);
			} else {
				toUpdate.add(c);
			}
		}
		if (!toInsert.isEmpty()) {
			insert(toInsert);
		}
		if (!toUpdate.isEmpty()) {
			updateById(toUpdate);
		}
	}
	
	public int insert(C c) {
		return insert(Collections.singletonList(c));
	}
	
	public int insert(Iterable<C> iterable) {
		return getPersisterListener().doWithInsertListener(iterable, () -> doInsert(iterable));
	}
	
	protected int doInsert(Iterable<C> iterable) {
		return insertExecutor.insert(iterable);
	}
	
	public void updateById(C c) {
		updateById(Collections.singletonList(c));
	}
	
	/**
	 * Update roughly some instances: no difference are computed, only update statements (full column) are applied.
	 * @param iterable iterable of instances
	 */
	public int updateById(Iterable<C> iterable) {
		return getPersisterListener().doWithUpdateByIdListener(iterable, () -> doUpdateById(iterable));
	}
	
	protected int doUpdateById(Iterable<C> iterable) {
		return updateExecutor.updateById(iterable);
	}
	
	public int update(C modified, C unmodified, boolean allColumnsStatement) {
		return update(Collections.singletonList(new HashMap.SimpleImmutableEntry<>(modified, unmodified)), allColumnsStatement);
	}
	
	/**
	 * Update instances that has changes.
	 * Groups statements to benefit from JDBC batch. Usefull overall when allColumnsStatement
	 * is set to false.
	 *  @param differencesIterable pairs of modified-unmodified instances, used to compute differences side by side
	 * @param allColumnsStatement true if all columns must be in the SQL statement, false if only modified ones should be..
	 */
	public Integer update(Iterable<Entry<C, C>> differencesIterable, boolean allColumnsStatement) {
		return getPersisterListener().doWithUpdateListener(differencesIterable, allColumnsStatement,
				() -> doUpdate(differencesIterable, allColumnsStatement));
	}
	
	protected int doUpdate(Iterable<Entry<C, C>> differencesIterable, boolean allColumnsStatement) {
		return updateExecutor.update(differencesIterable, allColumnsStatement);
	}
	
	public int delete(C c) {
		return delete(Collections.singletonList(c));
	}
	
	public int delete(Iterable<C> iterable) {
		return getPersisterListener().doWithDeleteListener(iterable, () -> doDelete(iterable));
	}
	
	protected int doDelete(Iterable<C> iterable) {
		return deleteExecutor.delete(iterable);
	}
	
	public int deleteById(C c) {
		return deleteById(Collections.singletonList(c));
	}
	
	public int deleteById(Iterable<C> iterable) {
		return getPersisterListener().doWithDeleteByIdListener(iterable, () -> doDeleteById(iterable));
	}
	
	protected int doDeleteById(Iterable<C> iterable) {
		return deleteExecutor.deleteById(iterable);
	}
	
	/**
	 * Indicates if a bean is persisted or not. Delegated to {@link ClassMappingStrategy}
	 * 
	 * @param c a bean
	 * @return true if a bean is already persisted
	 * @see ClassMappingStrategy#isNew(Object)
	 * @see org.gama.stalactite.persistence.mapping.IdMappingStrategy.IsNewDeterminer
	 */
	private boolean isNew(C c) {
		return mappingStrategy.isNew(c);
	}
	
	public C select(I id) {
		return Iterables.first(select(Collections.singleton(id)));
	}
	
	public List<C> select(Iterable<I> ids) {
		if (!Iterables.isEmpty(ids)) {
			return getPersisterListener().doWithSelectListener(ids, () -> doSelect(ids));
		} else {
			throw new IllegalArgumentException("Non selectable entity " + mappingStrategy.getClassToPersist() + " because of null id");
		}
	}
	
	protected List<C> doSelect(Iterable<I> ids) {
		return selectExecutor.select(ids);
	}
}
