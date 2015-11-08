package org.gama.stalactite.persistence.engine;

import org.gama.lang.bean.IFactory;
import org.gama.lang.collection.ValueFactoryHashMap;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Entry point for persistence in a database. Mix of configuration (Transaction, Dialect, ...) and registry for {@link
 * Persister}s.
 *
 * @author Guillaume Mary
 */
public class PersistenceContext {
	
	private static final ThreadLocal<PersistenceContext> CURRENT_CONTEXT = new ThreadLocal<>();
	private int jdbcBatchSize = 100;
	private Map<Class<?>, Persister> persisterCache = new ValueFactoryHashMap<>(10, new IFactory<Class<?>, Persister>() {
		@Override
		public Persister createInstance(Class<?> input) {
			return newPersister(input);
		}
	});
	
	public static PersistenceContext getCurrent() {
		PersistenceContext currentContext = CURRENT_CONTEXT.get();
		if (currentContext == null) {
			throw new IllegalStateException("No context found for current thread");
		}
		return currentContext;
	}
	
	public static void setCurrent(PersistenceContext context) {
		CURRENT_CONTEXT.set(context);
	}
	
	public static void clearCurrent() {
		CURRENT_CONTEXT.remove();
	}
	
	private Dialect dialect;
	private TransactionManager transactionManager;
	private final Map<Class, ClassMappingStrategy> mappingStrategies = new HashMap<>(50);
	
	public PersistenceContext(TransactionManager transactionManager, Dialect dialect) {
		this.transactionManager = transactionManager;
		this.dialect = dialect;
	}
	
	public TransactionManager getTransactionManager() {
		return transactionManager;
	}
	
	public Dialect getDialect() {
		return dialect;
	}
	
	public <T> ClassMappingStrategy<T> getMappingStrategy(Class<T> aClass) {
		return mappingStrategies.get(aClass);
	}
	
	public void add(ClassMappingStrategy classMappingStrategy) {
		mappingStrategies.put(classMappingStrategy.getClassToPersist(), classMappingStrategy);
	}
	
	public Map<Class, ClassMappingStrategy> getMappingStrategies() {
		return mappingStrategies;
	}
	
	public Connection getCurrentConnection() {
		return transactionManager.getCurrentConnection();
	}
	
	public Set<Persister> getPersisters() {
		// copy the Set because values() is backed by the Map and getPersisters() is not expected to permit such modifications
		return new HashSet<>(persisterCache.values());
	}
	
	public <T> Persister<T> getPersister(Class<T> clazz) {
		return persisterCache.get(clazz);
	}
	
	public <T> void setPersister(Persister<T> persister) {
		persisterCache.put(persister.getMappingStrategy().getClassToPersist(), persister);
	}
	
	protected <T> Persister<T> newPersister(Class<T> clazz) {
		return new Persister<>(PersistenceContext.this, ensureMappedClass(clazz));
	}
	
	protected <T> ClassMappingStrategy<T> ensureMappedClass(Class<T> clazz) {
		ClassMappingStrategy<T> mappingStrategy = getMappingStrategy(clazz);
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
