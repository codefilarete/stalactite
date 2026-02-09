package org.codefilarete.stalactite.engine.runtime;

import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;

import org.codefilarete.stalactite.engine.EntityCriteria;
import org.codefilarete.stalactite.engine.PersistExecutor;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.SimpleIdMapping;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.exception.NotImplementedException;

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
public class BeanPersister<C, I, T extends Table<T>>
		extends PersisterListenerWrapper<C, I>
		implements ConfiguredPersister<C, I> {

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

	private final PersistExecutor<C> persistExecutor;
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
		this.persistExecutor = newPersistExecutor();
		this.insertExecutor = newInsertExecutor(this.mappingStrategy, this.connectionConfiguration, dmlGenerator,
				writeOperationFactory, dialect.getInOperatorMaxSize());
		this.updateExecutor = newUpdateExecutor(this.mappingStrategy, this.connectionConfiguration, dmlGenerator,
				writeOperationFactory, dialect.getInOperatorMaxSize());
		this.deleteExecutor = newDeleteExecutor(this.mappingStrategy, this.connectionConfiguration, dmlGenerator,
				writeOperationFactory, dialect.getInOperatorMaxSize());
		this.selectExecutor = newSelectExecutor(this.mappingStrategy, this.connectionConfiguration, dialect);
	}
	
	protected PersistExecutor<C> newPersistExecutor() {
		return new DefaultPersistExecutor<>(this);
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
														ConnectionConfiguration connectionConfiguration,
														DMLGenerator dmlGenerator,
														WriteOperationFactory writeOperationFactory,
														int inOperatorMaxSize) {
		return new UpdateExecutor<>(mappingStrategy, connectionConfiguration, dmlGenerator,
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
																						ConnectionConfiguration connectionConfiguration,
																						Dialect dialect) {
		return new org.codefilarete.stalactite.engine.runtime.SelectExecutor<>(
				mappingStrategy,
				connectionConfiguration,
				dialect.getDmlGenerator(),
				dialect.getReadOperationFactory(),
				dialect.getInOperatorMaxSize());
	}
	
	public ConnectionProvider getConnectionProvider() {
		return connectionConfiguration.getConnectionProvider();
	}
	
	public DMLGenerator getDmlGenerator() {
		return dmlGenerator;
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
	public Set<Table<?>> giveImpliedTables() {
		return Collections.singleton(getMainTable());
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
	public ExecutableEntityQuery<C, ?> selectWhere() {
		throw new NotImplementedException("Not yet implemented");
	}
	
	@Override
	public ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter) {
		throw new NotImplementedException("Not yet implemented");
	}
	
	@Override
	public Set<C> selectAll() {
		throw new NotImplementedException("Not yet implemented");
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
	protected void doPersist(Iterable<? extends C> entities) {
		persistExecutor.persist(entities);
	}
	
	@Override
	protected void doInsert(Iterable<? extends C> entities) {
		insertExecutor.insert(entities);
	}
	
	@Override
	protected void doUpdateById(Iterable<? extends C> entities) {
		updateExecutor.updateById(entities);
	}
	
	@Override
	protected void doUpdate(Iterable<? extends Duo<C, C>> entities, boolean allColumnsStatement) {
		updateExecutor.update(entities, allColumnsStatement);
	}
	
	@Override
	protected void doDelete(Iterable<? extends C> entities) {
		deleteExecutor.delete(entities);
	}
	
	@Override
	protected void doDeleteById(Iterable<? extends C> entities) {
		deleteExecutor.deleteById(entities);
	}
	
	@Override
	protected Set<C> doSelect(Iterable<I> ids) {
		return selectExecutor.select(ids);
	}
	
	/**
	 * Indicates if a bean is persisted or not. Delegated to {@link DefaultEntityMapping}
	 * 
	 * @param c a bean
	 * @return true if a bean is already persisted
	 * @see DefaultEntityMapping#isNew(Object)
	 * @see SimpleIdMapping.IsNewDeterminer
	 */
	@Override
	public boolean isNew(C c) {
		return mappingStrategy.isNew(c);
	}
}
