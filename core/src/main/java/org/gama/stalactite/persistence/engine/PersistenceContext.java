package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.gama.stalactite.persistence.engine.TransactionManager.JdbcOperation;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;

/**
 * @author mary
 */
public class PersistenceContext {
	
	private static final ThreadLocal<PersistenceContext> CURRENT_CONTEXT = new ThreadLocal<>();
	private int jdbcBatchSize = 100;
	
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
	
	public Connection getCurrentConnection() throws SQLException {
		return transactionManager.getCurrentConnection();
	}
	
	public void executeInNewTransaction(JdbcOperation jdbcOperation) {
		transactionManager.executeInNewTransaction(jdbcOperation);
	}
	
	public <T> Persister<T> getPersister(Class<T> clazz) {
		return new Persister<>(this, ensureMappedClass(clazz));
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
