package org.codefilarete.stalactite.engine.runtime;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.StaleStateObjectException;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.exception.NotImplementedException;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.mapping.SimpleIdMapping;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.query.model.AbstractRelationalOperator;
import org.codefilarete.stalactite.sql.ConnectionProvider;

/**
 * CRUD Persistent features dedicated to an entity class. Entry point for persistent operations on entities.
 * 
 * @author Guillaume Mary
 */
@Nonnull
public class Persister<C, I, T extends Table> implements ConfiguredPersister<C, I> {
	
	private final EntityMapping<C, I, T> mappingStrategy;
	private final ConnectionConfiguration connectionConfiguration;
	private final DMLGenerator dmlGenerator;
	private final WriteOperationFactory writeOperationFactory;
	private final int inOperatorMaxSize;
	private PersisterListenerCollection<C, I> persisterListener = new PersisterListenerCollection<>();
	private final InsertExecutor<C, I, T> insertExecutor;
	private final UpdateExecutor<C, I, T> updateExecutor;
	private final DeleteExecutor<C, I, T> deleteExecutor;
	private final org.codefilarete.stalactite.engine.SelectExecutor<C, I> selectExecutor;
	
	public Persister(EntityMapping<C, I, T> mappingStrategy, PersistenceContext persistenceContext) {
		this(mappingStrategy, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
	}
	
	public Persister(EntityMapping<C, I, T> mappingStrategy, Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.mappingStrategy = mappingStrategy;
		this.connectionConfiguration = connectionConfiguration;
		this.dmlGenerator = dialect.getDmlGenerator();
		this.writeOperationFactory = dialect.getWriteOperationFactory();
		this.inOperatorMaxSize = dialect.getInOperatorMaxSize();
		this.insertExecutor = newInsertExecutor(mappingStrategy, this.connectionConfiguration, dmlGenerator,
				writeOperationFactory, inOperatorMaxSize);
		this.updateExecutor = newUpdateExecutor(mappingStrategy, this.connectionConfiguration, dmlGenerator,
				writeOperationFactory, inOperatorMaxSize);
		this.deleteExecutor = newDeleteExecutor(mappingStrategy, this.connectionConfiguration, dmlGenerator,
				writeOperationFactory, inOperatorMaxSize);
		this.selectExecutor = newSelectExecutor(mappingStrategy, this.connectionConfiguration.getConnectionProvider(), dialect);
		
		// Transfering identifier manager InsertListerner to here
		getPersisterListener().addInsertListener(
			getMapping().getIdMapping().getIdentifierInsertionManager().getInsertListener());
		getPersisterListener().addSelectListener(
			getMapping().getIdMapping().getIdentifierInsertionManager().getSelectListener());
	}
	
	public Persister(ConnectionConfiguration connectionConfiguration, DMLGenerator dmlGenerator, WriteOperationFactory writeOperationFactory,
					 int inOperatorMaxSize, EntityMapping<C, I, T> mappingStrategy, InsertExecutor<C, I, T> insertExecutor,
					 UpdateExecutor<C, I, T> updateExecutor, DeleteExecutor<C, I, T> deleteExecutor, org.codefilarete.stalactite.engine.SelectExecutor<C, I> selectExecutor) {
		this.mappingStrategy = mappingStrategy;
		this.connectionConfiguration = connectionConfiguration;
		this.dmlGenerator = dmlGenerator;
		this.writeOperationFactory = writeOperationFactory;
		this.inOperatorMaxSize = inOperatorMaxSize;
		this.insertExecutor = insertExecutor;
		this.updateExecutor = updateExecutor;
		this.deleteExecutor = deleteExecutor;
		this.selectExecutor = selectExecutor;
	}
	
	protected InsertExecutor<C, I, T> newInsertExecutor(EntityMapping<C, I, T> mappingStrategy,
														ConnectionConfiguration connectionConfiguration,
														DMLGenerator dmlGenerator,
														WriteOperationFactory writeOperationFactory,
														int inOperatorMaxSize) {
		return new InsertExecutor<>(mappingStrategy, connectionConfiguration, dmlGenerator,
				writeOperationFactory, inOperatorMaxSize);
	}
	
	protected UpdateExecutor<C, I, T> newUpdateExecutor(EntityMapping<C, I, T> mappingStrategy,
														ConnectionConfiguration connectionProvider,
														DMLGenerator dmlGenerator,
														WriteOperationFactory writeOperationFactory,
														int inOperatorMaxSize) {
		return new UpdateExecutor<>(mappingStrategy, connectionProvider, dmlGenerator,
				writeOperationFactory, inOperatorMaxSize);
	}
	
	protected DeleteExecutor<C, I, T> newDeleteExecutor(EntityMapping<C, I, T> mappingStrategy,
														ConnectionConfiguration connectionConfiguration,
														DMLGenerator dmlGenerator,
														WriteOperationFactory writeOperationFactory,
														int inOperatorMaxSize) {
		return new DeleteExecutor<>(mappingStrategy, connectionConfiguration, dmlGenerator,
				writeOperationFactory, inOperatorMaxSize);
	}
	
	protected org.codefilarete.stalactite.engine.SelectExecutor<C, I> newSelectExecutor(EntityMapping<C, I, T> mappingStrategy,
																						ConnectionProvider connectionProvider,
																						Dialect dialect) {
		return new org.codefilarete.stalactite.engine.runtime.SelectExecutor(mappingStrategy, connectionProvider, dialect.getDmlGenerator(), dialect.getInOperatorMaxSize());
	}
	
	public ConnectionProvider getConnectionProvider() {
		return connectionConfiguration.getConnectionProvider();
	}
	
	public DMLGenerator getDmlGenerator() {
		return dmlGenerator;
	}
	
	public int getInOperatorMaxSize() {
		return inOperatorMaxSize;
	}
	
	@Override
	public Class<C> getClassToPersist() {
		return getMapping().getClassToPersist();
	}
	
	@Override
	public EntityMapping<C, I, T> getMapping() {
		return mappingStrategy;
	}
	
	public T getMainTable() {
		return getMapping().getTargetTable();
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
	
	public void setPersisterListener(PersisterListenerCollection<C, I> persisterListener) {
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
	public PersisterListenerCollection<C, I> getPersisterListener() {
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
	
	public org.codefilarete.stalactite.engine.SelectExecutor<C, I> getSelectExecutor() {
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
	public void persist(Iterable<? extends C> entities) {
		EntityPersister.persist(entities, this::isNew, this, this, this, getMapping()::getId);
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
	public void insert(Iterable<? extends C> entities) {
		if (!Iterables.isEmpty(entities)) {
			getPersisterListener().doWithInsertListener(entities, () -> doInsert(entities));
		}
	}
	
	protected void doInsert(Iterable<? extends C> entities) {
		insertExecutor.insert(entities);
	}
	
	/**
	 * Updates roughly some entities: no differences are computed, only update statements (full column) are applied.
	 * 
	 * @param entities iterable of entities
	 * @return number of rows updated (relation-less counter) (maximum is argument size, may be 0 if rows weren't found in database)
	 */
	@Override
	public void updateById(Iterable<C> entities) {
		if (!Iterables.isEmpty(entities)) {
			getPersisterListener().doWithUpdateByIdListener(entities, () -> doUpdateById(entities));
		}
	}
	
	protected void doUpdateById(Iterable<C> entities) {
		updateExecutor.updateById(entities);
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
	public void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		if (!Iterables.isEmpty(differencesIterable)) {
			getPersisterListener().doWithUpdateListener(differencesIterable, allColumnsStatement, this::doUpdate);
		}
	}
	
	protected void doUpdate(Iterable<? extends Duo<C, C>> entities, boolean allColumnsStatement) {
		updateExecutor.update(entities, allColumnsStatement);
	}
	
	/**
	 * Deletes instances.
	 * Takes optimistic lock into account.
	 *
	 * @param entities entites to be deleted
	 * @return deleted row count
	 * @throws StaleStateObjectException if deleted row count differs from entities count
	 */
	@Override
	public void delete(Iterable<C> entities) {
		if (!Iterables.isEmpty(entities)) {
			getPersisterListener().doWithDeleteListener(entities, () -> doDelete(entities));
		}
	}
	
	protected void doDelete(Iterable<C> entities) {
		deleteExecutor.delete(entities);
	}
	
	/**
	 * Will delete instances only by their identifier.
	 * This method will not take optimisic lock (versioned entity) into account, so it will delete database rows "roughly".
	 *
	 * @param entities entites to be deleted
	 */
	@Override
	public void deleteById(Iterable<C> entities) {
		if (!Iterables.isEmpty(entities)) {
			getPersisterListener().doWithDeleteByIdListener(entities, () -> doDeleteById(entities));
		}
	}
	
	protected void doDeleteById(Iterable<C> entities) {
		deleteExecutor.deleteById(entities);
	}
	
	/**
	 * Indicates if a bean is persisted or not. Delegated to {@link ClassMapping}
	 * 
	 * @param c a bean
	 * @return true if a bean is already persisted
	 * @see ClassMapping#isNew(Object)
	 * @see SimpleIdMapping.IsNewDeterminer
	 */
	@Override
	public boolean isNew(C c) {
		return mappingStrategy.isNew(c);
	}
	
	@Override
	public I getId(C entity) {
		return mappingStrategy.getId(entity);
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