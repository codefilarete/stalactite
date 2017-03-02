package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.gama.lang.bean.IFactory;
import org.gama.lang.collection.ValueFactoryHashMap;
import org.gama.sql.ConnectionProvider;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;

/**
 * Entry point for persistence in a database. Mix of configuration (Transaction, Dialect, ...) and registry for {@link Persister}s.
 *
 * @author Guillaume Mary
 */
public class PersistenceContext {
	
	private int jdbcBatchSize = 100;
	private final Map<Class<?>, Persister> persisterCache = new ValueFactoryHashMap<>(10, new IFactory<Class<?>, Persister>() {
		@Override
		public Persister createInstance(Class<?> input) {
			return newPersister(input);
		}
	});
	
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
}
