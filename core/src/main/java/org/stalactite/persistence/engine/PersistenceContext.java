package org.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.stalactite.ILogger;
import org.stalactite.Logger;
import org.stalactite.persistence.engine.TransactionManager.JdbcOperation;
import org.stalactite.persistence.mapping.ClassMappingStrategy;
import org.stalactite.persistence.sql.Dialect;
import org.stalactite.persistence.sql.ddl.DDLGenerator;
import org.stalactite.persistence.structure.Table;

/**
 * @author mary
 */
public class PersistenceContext {
	
	private static final ILogger LOGGER = Logger.getLogger(PersistenceContext.class);
	
	private static final ThreadLocal<PersistenceContext> CURRENT_CONTEXT = new ThreadLocal<>();
	private List<Table> tables;
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
	
	public void deployDDL() throws SQLException {
		DDLGenerator ddlTableGenerator = getDDLGenerator();
		for (String sql : ddlTableGenerator.getCreationScripts()) {
			execute(sql);
		}
	}
	
	public void dropDDL() throws SQLException {
		DDLGenerator ddlTableGenerator = getDDLGenerator();
		for (String sql : ddlTableGenerator.getDropScripts()) {
			execute(sql);
		}
	}
	
	public DDLGenerator getDDLGenerator() {
		List<Table> tablesToCreate = getTables();
		return new DDLGenerator(tablesToCreate, getDialect());
	}
	
	public List<Table> getTables() {
		if (this.tables == null) {
			lookupTables();
		}
		return tables;
	}
	
	private void lookupTables() {
		tables = new ArrayList<>(this.mappingStrategies.size());
		for (ClassMappingStrategy classMappingStrategy : getMappingStrategies().values()) {
			tables.add(classMappingStrategy.getTargetTable());
		}
	}
	
	protected void execute(String sql) throws SQLException {
		try(Statement statement = getCurrentConnection().createStatement()) {
			LOGGER.debug(sql);
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
	
	public int getJDBCBatchSize() {
		return jdbcBatchSize;
	}
	
	public void setJDBCBatchSize(int jdbcBatchSize) {
		this.jdbcBatchSize = jdbcBatchSize;
	}
}
