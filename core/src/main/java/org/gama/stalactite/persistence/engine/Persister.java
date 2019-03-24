package org.gama.stalactite.persistence.engine;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.gama.lang.Duo;
import org.gama.lang.Retryer;
import org.gama.lang.collection.Iterables;
import org.gama.sql.ConnectionProvider;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener.UpdatePayload;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.SimpleIdMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;

/**
 * CRUD Persistent features dedicated to an entity class. Entry point for persistent operations on entities.
 * 
 * @author Guillaume Mary
 */
@Nonnull
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
	@Nonnull
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
	
	/**
	 * Persists an instance either it is already persisted or not (insert or update).
	 * 
	 * Check between insert or update is determined by id state which itself depends on identifier policy,
	 * see {@link SimpleIdMappingStrategy#IsNewDeterminer} implementations and
	 * {@link org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager} implementations for id value computation. 
	 * 
	 * @param entity an entity to be persisted
	 * @return persisted + updated rows count
	 * @throws StaleObjectExcepion if updated row count differs from entities count
	 * @see #insert(Iterable) 
	 * @see #update(Iterable, boolean)  
	 */
	public int persist(C entity) {
		// determine insert or update operation
		return persist(Collections.singleton(entity));
	}
	
	public int persist(Iterable<C> entities) {
		if (Iterables.isEmpty(entities)) {
			return 0;
		}
		// determine insert or update operation
		List<C> toInsert = new ArrayList<>(20);
		List<C> toUpdate = new ArrayList<>(20);
		for (C c : entities) {
			if (isNew(c)) {
				toInsert.add(c);
			} else {
				toUpdate.add(c);
			}
		}
		int writtenRowCount = 0;
		if (!toInsert.isEmpty()) {
			writtenRowCount += insert(toInsert);
		}
		if (!toUpdate.isEmpty()) {
			writtenRowCount += updateById(toUpdate);
		}
		return writtenRowCount;
	}
	
	public int insert(C c) {
		return insert(Collections.singletonList(c));
	}
	
	public int insert(Iterable<? extends C> entities) {
		if (Iterables.isEmpty(entities)) {
			return 0;
		} else {
			return getPersisterListener().doWithInsertListener(entities, () -> doInsert(entities));
		}
	}
	
	protected int doInsert(Iterable<? extends C> entities) {
		return insertExecutor.insert(entities);
	}
	
	public int updateById(C c) {
		return updateById(Collections.singletonList(c));
	}
	
	/**
	 * Updates roughly some instances: no difference are computed, only update statements (full column) are applied.
	 * 
	 * @param entities iterable of entities
	 */
	public int updateById(Iterable<C> entities) {
		if (Iterables.isEmpty(entities)) {
			// nothing to update => we return immediatly without any call to listeners
			return 0;
		} else {
			return getPersisterListener().doWithUpdateByIdListener(entities, () -> doUpdateById(entities));
		}
	}
	
	protected int doUpdateById(Iterable<C> entities) {
		return updateExecutor.updateById(entities);
	}
	
	/**
	 * Updates an instance that may have changes.
	 * Groups statements to benefit from JDBC batch. Usefull overall when allColumnsStatement
	 * is set to false.
	 *
	 * @param modified the supposing entity that has differences againt {@code unmodified} entity
	 * @param unmodified the "original" (freshly loaded from database ?) entity
	 * @param allColumnsStatement true if all columns must be in the SQL statement, false if only modified ones should be in
	 */
	public int update(C modified, C unmodified, boolean allColumnsStatement) {
		return update(Collections.singletonList(new Duo<>(modified, unmodified)), allColumnsStatement);
	}
	
	/**
	 * Updates instances that may have changes.
	 * Groups statements to benefit from JDBC batch. Usefull overall when allColumnsStatement
	 * is set to false.
	 * 
	 * @param differencesIterable pairs of modified-unmodified instances, used to compute differences side by side
	 * @param allColumnsStatement true if all columns must be in the SQL statement, false if only modified ones should be in
	 */
	public int update(Iterable<? extends Duo<? extends C, ? extends C>> differencesIterable, boolean allColumnsStatement) {
		Iterable<UpdatePayload<C, T>> updatePayloads = UpdateListener.computePayloads(differencesIterable, allColumnsStatement, getMappingStrategy());
		if (Iterables.isEmpty(updatePayloads)) {
			// nothing to update => we return immediatly without any call to listeners
			return 0;
		} else {
			return getPersisterListener().doWithUpdateListener(updatePayloads, allColumnsStatement, this::doUpdate);
		}
	}
	
	protected int doUpdate(Iterable<UpdatePayload<C, T>> updatePayloads, boolean allColumnsStatement) {
		if (allColumnsStatement) {
			return updateExecutor.updateMappedColumns(updatePayloads);
		} else {
			return updateExecutor.updateVariousColumns(updatePayloads);
		}
	}
	
	/**
	 * Deletes instances.
	 * Takes optimistic lock into account.
	 *
	 * @param entity entity to be deleted
	 * @return deleted row count
	 * @throws StaleObjectExcepion if deleted row count differs from entities count
	 */
	public int delete(C entity) {
		return delete(Collections.singletonList(entity));
	}
	
	/**
	 * Deletes instances.
	 * Takes optimistic lock into account.
	 *
	 * @param entities entites to be deleted
	 * @return deleted row count
	 * @throws StaleObjectExcepion if deleted row count differs from entities count
	 */
	public int delete(Iterable<C> entities) {
		if (Iterables.isEmpty(entities)) {
			return 0;
		}
		return getPersisterListener().doWithDeleteListener(entities, () -> doDelete(entities));
	}
	
	protected int doDelete(Iterable<C> entities) {
		return deleteExecutor.delete(entities);
	}
	
	/**
	 * Will delete instances only by their identifier.
	 * This method will not take optimisic lock (versioned entity) into account, so it will delete database rows "roughly".
	 *
	 * @param entity entity to be deleted
	 * @return deleted row count
	 */
	public int deleteById(C entity) {
		return deleteById(Collections.singletonList(entity));
	}
	
	/**
	 * Will delete instances only by their identifier.
	 * This method will not take optimisic lock (versioned entity) into account, so it will delete database rows "roughly".
	 *
	 * @param entities entites to be deleted
	 * @return deleted row count
	 */
	public int deleteById(Iterable<C> entities) {
		if (Iterables.isEmpty(entities)) {
			return 0;
		}
		return getPersisterListener().doWithDeleteByIdListener(entities, () -> doDeleteById(entities));
	}
	
	protected int doDeleteById(Iterable<C> entities) {
		return deleteExecutor.deleteById(entities);
	}
	
	/**
	 * Indicates if a bean is persisted or not. Delegated to {@link ClassMappingStrategy}
	 * 
	 * @param c a bean
	 * @return true if a bean is already persisted
	 * @see ClassMappingStrategy#isNew(Object)
	 * @see SimpleIdMappingStrategy.IsNewDeterminer
	 */
	private boolean isNew(C c) {
		return mappingStrategy.isNew(c);
	}
	
	public C select(I id) {
		return Iterables.first(select(Collections.singleton(id)));
	}
	
	public List<C> select(Collection<I> ids) {
		if (Iterables.isEmpty(ids)) {
			return new ArrayList<>();
		} else {
			return getPersisterListener().doWithSelectListener(ids, () -> doSelect(ids));
		}
	}
	
	protected List<C> doSelect(Collection<I> ids) {
		return selectExecutor.select(ids);
	}
}
