package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.gama.stalactite.ILogger;
import org.gama.stalactite.Logger;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.ddl.DDLGenerator;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public class DDLDeployer {
	
	private static final ILogger LOGGER = Logger.getLogger(DDLDeployer.class);
	
	public static List<Table> lookupTables(PersistenceContext persistenceContext) {
		Map<Class, ClassMappingStrategy> mappingStrategies = persistenceContext.getMappingStrategies();
		List<Table> tables = new ArrayList<>(mappingStrategies.size());
		for (ClassMappingStrategy classMappingStrategy : mappingStrategies.values()) {
			tables.add(classMappingStrategy.getTargetTable());
		}
		return tables;
	}
	
	private final DDLGenerator ddlTableGenerator;
	
	public DDLDeployer(PersistenceContext persistenceContext) {
		this(new DDLGenerator(lookupTables(persistenceContext), persistenceContext.getDialect().getJavaTypeToSqlTypeMapping()));
	}
	
	public DDLDeployer(DDLGenerator ddlTableGenerator) {
		this.ddlTableGenerator = ddlTableGenerator;
	}
	
	public DDLGenerator getDDLGenerator() {
		return ddlTableGenerator;
	}
	
	public void deployDDL() throws SQLException {
		for (String sql : getDDLGenerator().getCreationScripts()) {
			execute(sql);
		}
	}
	
	public void dropDDL() throws SQLException {
		for (String sql : getDDLGenerator().getDropScripts()) {
			execute(sql);
		}
	}
	
	protected void execute(String sql) throws SQLException {
		try(Statement statement = getCurrentConnection().createStatement()) {
			LOGGER.debug(sql);
			statement.execute(sql);
		} catch (Throwable t) {
			throw new RuntimeException("Error executing \"" + sql + "\"", t);
		}
	}
	
	protected Connection getCurrentConnection() throws SQLException {
		return PersistenceContext.getCurrent().getCurrentConnection();
	}
	
}
