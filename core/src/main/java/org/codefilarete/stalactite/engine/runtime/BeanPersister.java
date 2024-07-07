package org.codefilarete.stalactite.engine.runtime;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.engine.PersistExecutor;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.StaleStateObjectException;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersistListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateByIdListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.SimpleIdMapping;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.exception.NotImplementedException;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * Main class for CRUD operations of an entity class. Doesn't manage relation between entities.
 * Delegates SQL operations to {@link InsertExecutor}, {@link UpdateExecutor}, {@link DeleteExecutor} and
 * {@link SelectExecutor}.
 * Will wrap every CRUD operation with dedicated listener before / after method invokation.
 * 
 * @param <C> entity type
 * @param <I> identifier type
 * @param <T> target table type
 * @author Guillaume Mary
 */
public class BeanPersister<C, I, T extends Table<T>> implements EntityConfiguredPersister<C, I> {

	/**
	 * Property mapping on which SQL executors will refer to build their SQL operation
	 */
	private final EntityMapping<C, I, T> mappingStrategy;

	/**
	 * Connection on which SQL operations will be run
	 */
	private final ConnectionConfiguration connectionConfiguration;

	/**
	 * SQL generator for CRUD operations
	 */
	private final DMLGenerator dmlGenerator;

	/**
	 * Factory of {@link WriteOperation}
	 */
	private final WriteOperationFactory writeOperationFactory;

	/**
	 * Size of "in" operator max size (depends on database vendor). Useful for select statement only.
	 */
	private final int inOperatorMaxSize;
	
	private PersisterListenerCollection<C, I> persisterListener = new PersisterListenerCollection<>();
	private final InsertExecutor<C, I, T> insertExecutor;
	private final UpdateExecutor<C, I, T> updateExecutor;
	private final DeleteExecutor<C, I, T> deleteExecutor;
	private final org.codefilarete.stalactite.engine.SelectExecutor<C, I> selectExecutor;
	
	public BeanPersister(EntityMapping<C, I, T> mappingStrategy, PersistenceContext persistenceContext) {
		this(mappingStrategy, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
	}
	
	public BeanPersister(EntityMapping<C, I, T> mappingStrategy, Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.mappingStrategy = mappingStrategy;
		this.connectionConfiguration = connectionConfiguration;
		this.dmlGenerator = dialect.getDmlGenerator();
		this.writeOperationFactory = dialect.getWriteOperationFactory();
		this.inOperatorMaxSize = dialect.getInOperatorMaxSize();
		this.insertExecutor = newInsertExecutor(this.mappingStrategy, this.connectionConfiguration, dmlGenerator,
				writeOperationFactory, inOperatorMaxSize);
		this.updateExecutor = newUpdateExecutor(this.mappingStrategy, this.connectionConfiguration, dmlGenerator,
				writeOperationFactory, inOperatorMaxSize);
		this.deleteExecutor = newDeleteExecutor(this.mappingStrategy, this.connectionConfiguration, dmlGenerator,
				writeOperationFactory, inOperatorMaxSize);
		this.selectExecutor = newSelectExecutor(this.mappingStrategy, this.connectionConfiguration.getConnectionProvider(), dialect);
		
		// Transferring identifier manager InsertListener to here
		getPersisterListener().addInsertListener(
			getMapping().getIdMapping().getIdentifierInsertionManager().getInsertListener());
		getPersisterListener().addSelectListener(
			getMapping().getIdMapping().getIdentifierInsertionManager().getSelectListener());
	}
	
	public BeanPersister(ConnectionConfiguration connectionConfiguration,
						 DMLGenerator dmlGenerator,
						 WriteOperationFactory writeOperationFactory,
						 int inOperatorMaxSize,
						 EntityMapping<C, I, T> mappingStrategy,
						 InsertExecutor<C, I, T> insertExecutor,
						 UpdateExecutor<C, I, T> updateExecutor,
						 DeleteExecutor<C, I, T> deleteExecutor,
						 org.codefilarete.stalactite.engine.SelectExecutor<C, I> selectExecutor) {
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
	 * on the implementation of this {@link BeanPersister})
	 * Useful to generate schema creation script.
	 * 
	 * @return a {@link Set} of implied tables, not expected to be writable
	 */
	public Set<Table> giveImpliedTables() {
		return Collections.singleton(getMainTable());
	}
	
	/**
	 * Set listener of each CRUD method.
	 * 
	 * @param persisterListener any {@link PersisterListenerCollection}, if null then it won't be set to avoid internal "if" on each CRUD method
	 */
	public void setPersisterListener(PersisterListenerCollection<C, I> persisterListener) {
		// prevent from null instance to avoid an "if" on each CRUD method
		if (persisterListener != null) {
			this.persisterListener = persisterListener;
		}
	}
	
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
	
	@Override
	public <O> ExecutableEntityQuery<C> selectWhere(SerializableFunction<C, O> getter, ConditionalOperator<O, ?> operator) {
		throw new NotImplementedException("Not yet implemented");
	}
	
	@Override
	public <O> ExecutableEntityQuery<C> selectWhere(SerializableBiConsumer<C, O> setter, ConditionalOperator<O, ?> operator) {
		throw new NotImplementedException("Not yet implemented");
	}
	
	@Override
	public <O> ExecutableEntityQuery<C> selectWhere(AccessorChain<C, O> accessorChain, ConditionalOperator<O, ?> operator) {
		throw new NotImplementedException("Not yet implemented");
	}
	
	@Override
	public <O> ExecutableProjectionQuery<C> selectProjectionWhere(Consumer<Select> selectAdapter, AccessorChain<C, O> accessorChain, ConditionalOperator<O, ?> operator) {
		throw new NotImplementedException("Not yet implemented");
	}
	
	@Override
	public Set<C> selectAll() {
		throw new NotImplementedException("Not yet implemented");
	}
	
	@Override
	public void addPersistListener(PersistListener<? extends C> persistListener) {
		getPersisterListener().addPersistListener(persistListener);
	}
	
	@Override
	public void addInsertListener(InsertListener<? extends C> insertListener) {
		getPersisterListener().addInsertListener(insertListener);
	}
	
	@Override
	public void addUpdateListener(UpdateListener<? extends C> updateListener) {
		getPersisterListener().addUpdateListener(updateListener);
	}
	
	@Override
	public void addUpdateByIdListener(UpdateByIdListener<? extends C> updateByIdListener) {
		getPersisterListener().addUpdateByIdListener(updateByIdListener);
	}
	
	@Override
	public void addSelectListener(SelectListener<? extends C, I> selectListener) {
		getPersisterListener().addSelectListener(selectListener);
	}
	
	@Override
	public void addDeleteListener(DeleteListener<? extends C> deleteListener) {
		getPersisterListener().addDeleteListener(deleteListener);
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener<? extends C> deleteListener) {
		getPersisterListener().addDeleteByIdListener(deleteListener);
	}
	
	/**
	 * Saves given entities : will apply insert or update according to entities persistent state.
	 * Triggers cascade on relations. Please note that in case of already-persisted entities (not new), entities will be reloaded from database
	 * to compute differences and cascade only modifications.
	 * If one already has a copy of the "unmodified" instance, you may prefer to directly use {@link #update(Object, Object, boolean)}
	 *
	 * @param entities some entities, can be a mix of none-persistent or already-persisted entities
	 */
	@Override
	public void persist(Iterable<? extends C> entities) {
		if (!Iterables.isEmpty(entities)) {
			getPersisterListener().doWithPersistListener(entities, () -> doPersist(entities));
		}
	}
	
	protected void doPersist(Iterable<? extends C> entities) {
		PersistExecutor.persist(entities, this::isNew, this, this, this, this::getId);
	}
	
	/**
	 * Insert given entities.
	 *
	 * @param entities some entities expected to be not persisted yet since inserting them again should cause some primary key constraint failure
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
	 */
	@Override
	public void updateById(Iterable<? extends C> entities) {
		if (!Iterables.isEmpty(entities)) {
			getPersisterListener().doWithUpdateByIdListener(entities, () -> doUpdateById(entities));
		}
	}
	
	protected void doUpdateById(Iterable<? extends C> entities) {
		updateExecutor.updateById(entities);
	}
	
	/**
	 * Update given entities that may have changes.
	 * Takes optimistic lock into account.
	 * Groups statements to benefit from JDBC batch. Useful overall when allColumnsStatement is set to false.
	 * 
	 * @param differencesIterable pairs of modified-unmodified instances, used to compute differences side by side
	 * @param allColumnsStatement true if all columns must be in the SQL statement, false if only modified ones should be in
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
	 * Delete given entities.
	 * Takes optimistic lock into account.
	 *
	 * @param entities entites to be deleted
	 * @throws StaleStateObjectException if deleted row count differs from entities count
	 */
	@Override
	public void delete(Iterable<? extends C> entities) {
		if (!Iterables.isEmpty(entities)) {
			getPersisterListener().doWithDeleteListener(entities, () -> doDelete(entities));
		}
	}
	
	protected void doDelete(Iterable<? extends C> entities) {
		deleteExecutor.delete(entities);
	}
	
	/**
	 * Will delete instances only by their identifier.
	 * This method will not take optimistic lock (versioned entity) into account, so it will delete database rows "roughly".
	 *
	 * @param entities entities to be deleted
	 */
	@Override
	public void deleteById(Iterable<? extends C> entities) {
		if (!Iterables.isEmpty(entities)) {
			getPersisterListener().doWithDeleteByIdListener(entities, () -> doDeleteById(entities));
		}
	}
	
	protected void doDeleteById(Iterable<? extends C> entities) {
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
	public Set<C> select(Iterable<I> ids) {
		if (Iterables.isEmpty(ids)) {
			return new HashSet<>();
		} else {
			return getPersisterListener().doWithSelectListener(ids, () -> doSelect(ids));
		}
	}
	
	protected Set<C> doSelect(Iterable<I> ids) {
		return selectExecutor.select(ids);
	}
}
