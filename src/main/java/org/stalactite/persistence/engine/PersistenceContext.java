package org.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.stalactite.persistence.engine.TransactionManager.JdbcOperation;
import org.stalactite.persistence.mapping.ClassMappingStrategy;
import org.stalactite.persistence.sql.Dialect;
import org.stalactite.persistence.sql.ddl.DDLGenerator;
import org.stalactite.persistence.structure.Table;

/**
 * @author mary
 */
public class PersistenceContext {
	
	private static final ThreadLocal<PersistenceContext> CURRENT_CONTEXT = new ThreadLocal<>();
	
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
	
	public void deployDDL() throws SQLException {
		DDLGenerator ddlTableGenerator = getDDLGenerator();
		for (String sql : ddlTableGenerator.getCreationScripts()) {
			execute(sql);
		}
	}
	
	public DDLGenerator getDDLGenerator() {
		List<Table> tablesToCreate = getTables();
		return new DDLGenerator(tablesToCreate, getDialect());
	}
	
	private List<Table> getTables() {
		List<Table> tablesToCreate = new ArrayList<>(this.mappingStrategies.size());
		for (ClassMappingStrategy classMappingStrategy : getMappingStrategies().values()) {
			tablesToCreate.add(classMappingStrategy.getTargetTable());
		}
		return tablesToCreate;
	}
	
	protected void execute(String sql) throws SQLException {
		try(Statement statement = getCurrentConnection().createStatement()) {
			statement.execute(sql);
		}
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
}
