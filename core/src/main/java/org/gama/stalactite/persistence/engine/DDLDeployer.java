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
import java.util.Collection;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class DDLDeployer {
	
	private static final ILogger LOGGER = Logger.getLogger(DDLDeployer.class);
	
	public static List<Table> lookupTables(PersistenceContext persistenceContext) {
		Collection<ClassMappingStrategy> mappingStrategies = persistenceContext.getMappingStrategies().values();
		List<Table> tables = new ArrayList<>(mappingStrategies.size());
		for (ClassMappingStrategy classMappingStrategy : mappingStrategies) {
			tables.add(classMappingStrategy.getTargetTable());
		}
		return tables;
	}
	
	private final DDLSchemaGenerator ddlSchemaGenerator;
	private final ConnectionProvider connectionProvider;
	
	public DDLDeployer(PersistenceContext persistenceContext) {
		this(persistenceContext.getDialect().getDdlSchemaGenerator(), persistenceContext.getConnectionProvider());
		getDdlSchemaGenerator().setTables(lookupTables(persistenceContext));
	}
	
	public DDLDeployer(DDLSchemaGenerator ddlSchemaGenerator, ConnectionProvider connectionProvider) {
		this.ddlSchemaGenerator = ddlSchemaGenerator;
		this.connectionProvider = connectionProvider;
	}
	
	public DDLSchemaGenerator getDdlSchemaGenerator() {
		return ddlSchemaGenerator;
	}
	
	public void deployDDL() throws SQLException {
		for (String sql : getDdlSchemaGenerator().getCreationScripts()) {
			execute(sql);
		}
	}
	
	public void dropDDL() throws SQLException {
		for (String sql : getDdlSchemaGenerator().getDropScripts()) {
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
		return connectionProvider.getCurrentConnection();
	}
	
}
