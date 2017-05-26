package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.gama.sql.ConnectionProvider;
import org.gama.stalactite.ILogger;
import org.gama.stalactite.Logger;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.ddl.DDLSchemaGenerator;
import org.gama.stalactite.persistence.structure.Table;

/**
 * A class aimed at deploying DDL elements to a database. It gets its elements from a {@link DDLSchemaGenerator} and execute them
 * onto a {@link ConnectionProvider}, so it's more an entry point for high level usage.
 *
 * @author Guillaume Mary
 * @see DDLSchemaGenerator
 */
public class DDLDeployer {
	
	private static final ILogger LOGGER = Logger.getLogger(DDLDeployer.class);
	
	/**
	 * Find all tables defined in the given {@link PersistenceContext}
	 *
	 * @param persistenceContext a {@link PersistenceContext} to scan for tables
	 * @return a {@link Collection} of found tables
	 */
	public static Collection<Table> lookupTables(PersistenceContext persistenceContext) {
		Collection<ClassMappingStrategy> mappingStrategies = persistenceContext.getMappingStrategies().values();
		return mappingStrategies.stream().map(ClassMappingStrategy::getTargetTable).collect(Collectors.toList());
	}
	
	private final DDLSchemaGenerator ddlSchemaGenerator;
	private final ConnectionProvider connectionProvider;
	
	/**
	 * Simple constructor that gets its informations from the passed {@link PersistenceContext}: {@link DDLSchemaGenerator} and
	 * {@link ConnectionProvider}
	 * 
	 * @param persistenceContext a {@link PersistenceContext}, source of arguments for {@link #DDLDeployer(DDLSchemaGenerator, ConnectionProvider)}
	 * @see #DDLDeployer(DDLSchemaGenerator, ConnectionProvider)
	 */
	public DDLDeployer(PersistenceContext persistenceContext) {
		this(persistenceContext.getDialect().getDdlSchemaGenerator(), persistenceContext.getConnectionProvider());
		getDdlSchemaGenerator().setTables(lookupTables(persistenceContext));
	}
	
	/**
	 * Main constructor with mandatory objects for its work.
	 * 
	 * @param ddlSchemaGenerator the SQL scripts provider
	 * @param connectionProvider the {@link Connection} provider for executing SQL scripts
	 */
	public DDLDeployer(DDLSchemaGenerator ddlSchemaGenerator, ConnectionProvider connectionProvider) {
		this.ddlSchemaGenerator = ddlSchemaGenerator;
		this.connectionProvider = connectionProvider;
	}
	
	public DDLSchemaGenerator getDdlSchemaGenerator() {
		return ddlSchemaGenerator;
	}
	
	public void deployDDL() throws SQLException {
		for (String sql : getCreationScripts()) {
			execute(sql);
		}
	}
	
	public List<String> getCreationScripts() {
		return getDdlSchemaGenerator().getCreationScripts();
	}
	
	public void dropDDL() throws SQLException {
		for (String sql : getDropScripts()) {
			execute(sql);
		}
	}
	
	public List<String> getDropScripts() {
		return getDdlSchemaGenerator().getDropScripts();
	}
	
	protected void execute(String sql) throws SQLException {
		try (Statement statement = getCurrentConnection().createStatement()) {
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
