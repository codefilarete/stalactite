package org.gama.stalactite.persistence.engine;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Duo;
import org.gama.lang.Retryer;
import org.gama.lang.collection.Iterables;
import org.gama.lang.exception.NotImplementedException;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.mapping.SimpleIdMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.IConnectionConfiguration;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.AbstractRelationalOperator;
import org.gama.stalactite.sql.ConnectionProvider;

/**
 * CRUD Persistent features dedicated to an entity class. Entry point for persistent operations on entities.
 * 
 * @author Guillaume Mary
 */
@Nonnull
public class Persister<C, I, T extends Table> implements IEntityPersister<C, I>, IConfiguredPersister<C, I> {
	
	private final IEntityMappingStrategy<C, I, T> mappingStrategy;
	private final IConnectionConfiguration connectionProvider;
	private final DMLGenerator dmlGenerator;
	private final Retryer writeOperationRetryer;
	private final int inOperatorMaxSize;
	private PersisterListener<C, I> persisterListener = new PersisterListener<>();
	private final InsertExecutor<C, I, T> insertExecutor;
	private final UpdateExecutor<C, I, T> updateExecutor;
	private final DeleteExecutor<C, I, T> deleteExecutor;
	private final ISelectExecutor<C, I> selectExecutor;
	
	public Persister(IEntityMappingStrategy<C, I, T> mappingStrategy, PersistenceContext persistenceContext) {
		this(mappingStrategy, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
	}
	
	public Persister(IEntityMappingStrategy<C, I, T> mappingStrategy, Dialect dialect, IConnectionConfiguration connectionConfiguration) {
		this.mappingStrategy = mappingStrategy;
		this.connectionProvider = connectionConfiguration;
		this.dmlGenerator = dialect.getDmlGenerator();
		this.writeOperationRetryer = dialect.getWriteOperationRetryer();
		this.inOperatorMaxSize = dialect.getInOperatorMaxSize();
		this.insertExecutor = newInsertExecutor(mappingStrategy, connectionProvider, dmlGenerator,
				writeOperationRetryer, inOperatorMaxSize);
		this.updateExecutor = newUpdateExecutor(mappingStrategy, connectionProvider, dmlGenerator,
				writeOperationRetryer, inOperatorMaxSize);
		this.deleteExecutor = newDeleteExecutor(mappingStrategy, connectionProvider, dmlGenerator,
				writeOperationRetryer, inOperatorMaxSize);
		this.selectExecutor = newSelectExecutor(mappingStrategy, connectionProvider.getConnectionProvider(), dialect);
		
		// Transfering identifier manager InsertListerner to here
		getPersisterListener().addInsertListener(
				getMappingStrategy().getIdMappingStrategy().getIdentifierInsertionManager().getInsertListener());
	}
	
	public Persister(IConnectionConfiguration connectionConfiguration, DMLGenerator dmlGenerator, Retryer writeOperationRetryer,
					 int inOperatorMaxSize, IEntityMappingStrategy<C, I, T> mappingStrategy, InsertExecutor<C, I, T> insertExecutor,
					 UpdateExecutor<C, I, T> updateExecutor, DeleteExecutor<C, I, T> deleteExecutor, ISelectExecutor<C, I> selectExecutor) {
		this.mappingStrategy = mappingStrategy;
		this.connectionProvider = connectionConfiguration;
		this.dmlGenerator = dmlGenerator;
		this.writeOperationRetryer = writeOperationRetryer;
		this.inOperatorMaxSize = inOperatorMaxSize;
		this.insertExecutor = insertExecutor;
		this.updateExecutor = updateExecutor;
		this.deleteExecutor = deleteExecutor;
		this.selectExecutor = selectExecutor;
	}
	
	protected InsertExecutor<C, I, T> newInsertExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
														IConnectionConfiguration connectionConfiguration,
														DMLGenerator dmlGenerator,
														Retryer writeOperationRetryer,
														int inOperatorMaxSize) {
		return new InsertExecutor<>(mappingStrategy, connectionConfiguration, dmlGenerator,
				writeOperationRetryer, inOperatorMaxSize);
	}
	
	protected UpdateExecutor<C, I, T> newUpdateExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
														IConnectionConfiguration connectionProvider,
													  DMLGenerator dmlGenerator,
													  Retryer writeOperationRetryer,
													  int inOperatorMaxSize) {
		return new UpdateExecutor<>(mappingStrategy, connectionProvider, dmlGenerator,
				writeOperationRetryer, inOperatorMaxSize);
	}
	
	protected DeleteExecutor<C, I, T> newDeleteExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
														IConnectionConfiguration connectionConfiguration,
															DMLGenerator dmlGenerator,
															Retryer writeOperationRetryer,
															int inOperatorMaxSize) {
		return new DeleteExecutor<>(mappingStrategy, connectionConfiguration, dmlGenerator,
				writeOperationRetryer, inOperatorMaxSize);
	}
	
	protected ISelectExecutor<C, I> newSelectExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
													  ConnectionProvider connectionProvider,
													  Dialect dialect) {
		return new SelectExecutor<>(mappingStrategy, connectionProvider, dialect.getDmlGenerator(), dialect.getInOperatorMaxSize());
	}
	
	public ConnectionProvider getConnectionProvider() {
		return connectionProvider.getConnectionProvider();
	}
	
	public DMLGenerator getDmlGenerator() {
		return dmlGenerator;
	}
	
	public Retryer getWriteOperationRetryer() {
		return writeOperationRetryer;
	}
	
	public int getInOperatorMaxSize() {
		return inOperatorMaxSize;
	}
	
	@Override
	public Class<C> getClassToPersist() {
		return getMappingStrategy().getClassToPersist();
	}
	
	@Override
	public IEntityMappingStrategy<C, I, T> getMappingStrategy() {
		return mappingStrategy;
	}
	
	public T getMainTable() {
		return getMappingStrategy().getTargetTable();
	}
	
	/**
	 * Gives all tables implied in the persistence of the entity such as joined tables or duplicating tables (depending
	 * on the implementation of this {@link Persister})
	 * Usefull to generate schema creation script.
	 * 
	 * @return a {@link Set} of implied tables, not expected to be writable
	 */
	public Set<Table> giveImpliedTables() {
		return Collections.singleton(getMainTable());
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
	@Override
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
	
	public ISelectExecutor<C, I> getSelectExecutor() {
		return selectExecutor;
	}
	
	/**
	 * Saves given entities : will apply insert or update according to entities persistent state.
	 * Triggers cascade on relations. Please note that in case of already-persisted entities (not new), entities will be reloaded from database
	 * to compute differences and cascade only modifications.
	 * If one already has a copy of the "unmodified" instance, you may prefer to direcly use {@link #update(Object, Object, boolean)}
	 * 
	 * @param entities some entities, can be a mix of none-persistent or already-persisted entities
	 * @return number of rows inserted and updated (relation-less counter) (maximum is argument size, may be 0 if no modifications were found between memory and database)
	 */
	@Override
	public int persist(Iterable<C> entities) {
		return IEntityPersister.persist(entities, this::isNew, this, this, this, getMappingStrategy()::getId);
	}
	
	@Override
	public <O> ExecutableEntityQuery<C> selectWhere(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator) {
		throw new NotImplementedException("Not yet implemented");
	}
	
	@Override
	public <O> ExecutableEntityQuery<C> selectWhere(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator) {
		throw new NotImplementedException("Not yet implemented");
	}
	
	@Override
	public List<C> selectAll() {
		throw new NotImplementedException("Not yet implemented");
	}
	
	@Override
	public void addInsertListener(InsertListener insertListener) {
		getPersisterListener().addInsertListener(insertListener);
	}
	
	@Override
	public void addUpdateListener(UpdateListener updateListener) {
		getPersisterListener().addUpdateListener(updateListener);
	}
	
	@Override
	public void addSelectListener(SelectListener selectListener) {
		getPersisterListener().addSelectListener(selectListener);
	}
	
	@Override
	public void addDeleteListener(DeleteListener deleteListener) {
		getPersisterListener().addDeleteListener(deleteListener);
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener deleteListener) {
		getPersisterListener().addDeleteByIdListener(deleteListener);
	}
	
	/**
	 * Saves given entity : will apply insert or update according to entity persistent state.
	 * Triggers cascade on relations. Please note that in case of already-persisted entity (not new), entity will be reloaded from database
	 * to compute differences and cascade only modifications.
	 * If one already has a copy of the "unmodified" instance, you may prefer to direcly use {@link #update(Object, Object, boolean)}
	 *
	 * @param entities entities, none-persistent or already-persisted
	 * @return number of rows inserted (relation-less counter) (maximum is argument size)
	 */
	@Override
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
	
	/**
	 * Updates roughly given entity: no differences are computed, only update statements (full column) are applied.
	 *
	 * @param entity an entity
	 * @return number of rows updated (relation-less counter) (maximum is 1, may be 0 if row wasn't found in database)
	 */
	public int updateById(C entity) {
		return updateById(Collections.singletonList(entity));
	}
	
	/**
	 * Updates roughly some entities: no differences are computed, only update statements (full column) are applied.
	 * 
	 * @param entities iterable of entities
	 * @return number of rows updated (relation-less counter) (maximum is argument size, may be 0 if rows weren't found in database)
	 */
	@Override
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
	 * Updates instances that may have changes.
	 * Groups statements to benefit from JDBC batch. Usefull overall when allColumnsStatement
	 * is set to false.
	 * 
	 * @param differencesIterable pairs of modified-unmodified instances, used to compute differences side by side
	 * @param allColumnsStatement true if all columns must be in the SQL statement, false if only modified ones should be in
	 * @return number of rows updated (relation-less counter) (maximum is argument size, may be 0 if row wasn't found in database)
	 */
	@Override
	public int update(Iterable<? extends Duo<? extends C, ? extends C>> differencesIterable, boolean allColumnsStatement) {
		if (Iterables.isEmpty(differencesIterable)) {
			// nothing to update => we return immediatly without any call to listeners
			return 0;
		} else {
			return getPersisterListener().doWithUpdateListener(differencesIterable, allColumnsStatement, this::doUpdate);
		}
	}
	
	protected int doUpdate(Iterable<? extends Duo<? extends C, ? extends C>> entities, boolean allColumnsStatement) {
		return updateExecutor.update(entities, allColumnsStatement);
	}
	
	/**
	 * Deletes instances.
	 * Takes optimistic lock into account.
	 *
	 * @param entities entites to be deleted
	 * @return deleted row count
	 * @throws StaleObjectExcepion if deleted row count differs from entities count
	 */
	@Override
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
	 * @param entities entites to be deleted
	 * @return deleted row count
	 */
	@Override
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
	@Override
	public boolean isNew(C c) {
		return mappingStrategy.isNew(c);
	}
	
	@Override
	public List<C> select(Iterable<I> ids) {
		if (Iterables.isEmpty(ids)) {
			return new ArrayList<>();
		} else {
			return getPersisterListener().doWithSelectListener(ids, () -> doSelect(ids));
		}
	}
	
	protected List<C> doSelect(Iterable<I> ids) {
		return selectExecutor.select(ids);
	}
}
