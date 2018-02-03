package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.gama.lang.collection.ValueFactoryHashMap;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.binder.ParameterBinderProvider;
import org.gama.sql.dml.PreparedSQL;
import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.command.builder.DeleteCommandBuilder;
import org.gama.stalactite.command.builder.InsertCommandBuilder;
import org.gama.stalactite.command.builder.UpdateCommandBuilder;
import org.gama.stalactite.command.builder.UpdateCommandBuilder.UpdateStatement;
import org.gama.stalactite.command.model.Delete;
import org.gama.stalactite.command.model.Insert;
import org.gama.stalactite.command.model.Update;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.dml.ColumnParamedSQL;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Entry point for persistence in a database. Mix of configuration (Transaction, Dialect, ...) and registry for {@link Persister}s.
 *
 * @author Guillaume Mary
 */
public class PersistenceContext {
	
	private int jdbcBatchSize = 100;
	private final Map<Class<?>, Persister> persisterCache = new ValueFactoryHashMap<>(10, input -> newPersister(input));
	
	private Dialect dialect;
	private ConnectionProvider connectionProvider;
	private final Map<Class, ClassMappingStrategy> mappingStrategies = new HashMap<>(50);
	
	public PersistenceContext(ConnectionProvider connectionProvider, Dialect dialect) {
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
	}
	
	public ConnectionProvider getConnectionProvider() {
		return connectionProvider;
	}
	
	public Dialect getDialect() {
		return dialect;
	}
	
	public <T> ClassMappingStrategy<T, Object> getMappingStrategy(Class<T> aClass) {
		return mappingStrategies.get(aClass);
	}
	
	/**
	 * Add a persistence configuration to this instance
	 * 
	 * @param classMappingStrategy the persitence configuration
	 * @param <T> the entity type that is configured for persistence
	 * @param <I> the identifier type of the entity
	 * @return the newly created {@link Persister} for the configuration
	 */
	public <T, I> Persister<T, I> add(ClassMappingStrategy<T, I> classMappingStrategy) {
		mappingStrategies.put(classMappingStrategy.getClassToPersist(), classMappingStrategy);
		Persister<T, I> persister = new Persister<>(this, classMappingStrategy);
		persisterCache.put(classMappingStrategy.getClassToPersist(), persister);
		return persister;
	}
	
	public Map<Class, ClassMappingStrategy> getMappingStrategies() {
		return mappingStrategies;
	}
	
	public Connection getCurrentConnection() {
		return connectionProvider.getCurrentConnection();
	}
	
	public Set<Persister> getPersisters() {
		// copy the Set because values() is backed by the Map and getPersisters() is not expected to permit such modifications
		return new HashSet<>(persisterCache.values());
	}
	
	/**
	 * Returns the {@link Persister} mapped for a class.
	 * Prefer usage of that returned by {@link #add(ClassMappingStrategy)} because it's better typed (with identifier type)
	 * 
	 * @param clazz the class for which the {@link Persister} must be given
	 * @param <T> the type of the persisted entity
	 * @return never null
	 * @throws IllegalArgumentException if the class is not mapped
	 */
	public <T> Persister<T, ?> getPersister(Class<T> clazz) {
		return persisterCache.get(clazz);
	}
	
	public <T> void setPersister(Persister<T, ?> persister) {
		persisterCache.put(persister.getMappingStrategy().getClassToPersist(), persister);
	}
	
	protected <T> Persister<T, ?> newPersister(Class<T> clazz) {
		return new Persister<>(this, ensureMappedClass(clazz));
	}
	
	protected <T> ClassMappingStrategy<T, Object> ensureMappedClass(Class<T> clazz) {
		ClassMappingStrategy<T, Object> mappingStrategy = getMappingStrategy(clazz);
		if (mappingStrategy == null) {
			throw new IllegalArgumentException("Unmapped entity " + clazz);
		} else {
			return mappingStrategy;
		}
	}
	
	public int getJDBCBatchSize() {
		return jdbcBatchSize;
	}
	
	public void setJDBCBatchSize(int jdbcBatchSize) {
		this.jdbcBatchSize = jdbcBatchSize;
	}
	
	/**
	 * Create a {@link Query} 
	 * @param sql the sql to execute to populate beans
	 * @param beanType type of created beans, used for returned type marker
	 * @param <T> type of created beans
	 * @return a new {@link Query} that must be configured and executed
	 */
	public <T> Query<T> newQuery(CharSequence sql, Class<T> beanType) {
		return new Query<>(beanType, sql, ParameterBinderProvider.fromMap(getDialect().getColumnBinderRegistry().getParameterBinders()));
	}
	
	public ExecutableUpdate update(Table table) {
		return new ExecutableUpdate(table);
	}
	
	public ExecutableInsert insert(Table table) {
		return new ExecutableInsert(table);
	}
	
	public ExecutableDelete delete(Table table) {
		return new ExecutableDelete(table);
	}
	
	public class ExecutableUpdate extends Update {
		
		public ExecutableUpdate(Table targetTable) {
			super(targetTable);
		}
		
		/** Overriden to adapt return type */
		@Override
		public ExecutableUpdate set(Column column) {
			super.set(column);
			return this;
		}
		
		/** Overriden to adapt return type */
		@Override
		public ExecutableUpdate set(Column column, Object value) {
			super.set(column, value);
			return this;
		}
		
		/** Overriden to adapt return type */
		@Override
		public ExecutableUpdate set(Column column1, Column column2) {
			super.set(column1, column2);
			return this;
		}
		
		/**
		 * Executes this update statement. To be used when update has no parameters on where clause, else use {@link #execute(Map)}
		 * 
		 * @return the updated row count
		 */
		public int execute() {
			return execute(Collections.emptyMap());
		}
		
		/**
		 * Executes this update statement with given values.
		 *
		 * @return the updated row count
		 */
		public int execute(Map<Column, Object> values) {
			UpdateStatement updateStatement = new UpdateCommandBuilder(this).toStatement(getDialect().getColumnBinderRegistry());
			values.forEach(updateStatement::setValue);
			WriteOperation<Integer> writeOperation = new WriteOperation<>(updateStatement, connectionProvider);
			
			writeOperation.setValues(updateStatement.getValues());
			return writeOperation.execute();
		}
	}
	
	public class ExecutableInsert extends Insert {
		
		private final Map<Column, Object> values = new HashMap<>();
		
		public ExecutableInsert(Table table) {
			super(table);
		}
		
		public <T> ExecutableInsert set(Column<T> column, T value) {
			super.set(column);
			this.values.put(column, value);
			return this;
		}
		
		/**
		 * Executes this insert statement.
		 *
		 * @return the inserted row count
		 */
		public int execute() {
			ColumnParamedSQL insertStatement = new InsertCommandBuilder(this).toStatement(getDialect().getColumnBinderRegistry());
			values.forEach(insertStatement::setValue);
			WriteOperation<Column> writeOperation = new WriteOperation<>(insertStatement, connectionProvider);
			
			writeOperation.setValues(insertStatement.getValues());
			return writeOperation.execute();
		}
	}
	
	public class ExecutableDelete extends Delete {
		
		public ExecutableDelete(Table table) {
			super(table);
		}
		
		/**
		 * Executes this delete statement with given values.
		 *
		 * @return the deleted row count
		 */
		public int execute() {
			PreparedSQL deleteStatement = new DeleteCommandBuilder(this).toStatement(getDialect().getColumnBinderRegistry());
			WriteOperation<Integer> writeOperation = new WriteOperation<>(deleteStatement, connectionProvider);
			
			writeOperation.setValues(deleteStatement.getValues());
			return writeOperation.execute();
		}
	}
}
