package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.ILogger;
import org.gama.stalactite.Logger;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.ddl.DDLSchemaGenerator;
import org.gama.stalactite.persistence.structure.Table;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
	
	private final DDLSchemaGenerator ddlTableGenerator;
	
	public DDLDeployer(PersistenceContext persistenceContext) {
		this(persistenceContext.getDialect().getDDLSchemaGenerator(lookupTables(persistenceContext)));
	}
	
	public DDLDeployer(DDLSchemaGenerator ddlTableGenerator) {
		this.ddlTableGenerator = ddlTableGenerator;
	}
	
	public DDLSchemaGenerator getDDLGenerator() {
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
