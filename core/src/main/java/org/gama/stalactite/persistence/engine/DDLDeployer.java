package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.gama.sql.ConnectionProvider;
import org.gama.sql.dml.SQLExecutionException;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.ddl.DDLSchemaGenerator;
import org.gama.stalactite.persistence.structure.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class aimed at deploying DDL elements to a database. It gets its elements from a {@link DDLSchemaGenerator} and execute them
 * onto a {@link ConnectionProvider}, so it's more an entry point for high level usage.
 *
 * @author Guillaume Mary
 * @see DDLSchemaGenerator
 */
public class DDLDeployer {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DDLDeployer.class);
	
	/**
	 * Find all tables defined in the given {@link PersistenceContext}
	 *
	 * @param persistenceContext a {@link PersistenceContext} to scan for tables
	 * @return a {@link Collection} of found tables
	 */
	public static Collection<Table> lookupTables(PersistenceContext persistenceContext) {
		Collection<ClassMappingStrategy> mappingStrategies = persistenceContext.getMappingStrategies().values();
		Stream<Table> objectStream = mappingStrategies.stream().map(ClassMappingStrategy::getTargetTable);
		return objectStream.collect(Collectors.toList());
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
	 * <strong>Tables to be generated must be declared via {@link DDLSchemaGenerator#addTables(Table, Table...)} </strong>
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
	
	public void deployDDL() {
		for (String sql : getCreationScripts()) {
			execute(sql);
		}
	}
	
	public List<String> getCreationScripts() {
		return getDdlSchemaGenerator().getCreationScripts();
	}
	
	public void dropDDL() {
		for (String sql : getDropScripts()) {
			execute(sql);
		}
	}
	
	public List<String> getDropScripts() {
		return getDdlSchemaGenerator().getDropScripts();
	}
	
	protected void execute(String sql) {
		try (Statement statement = getCurrentConnection().createStatement()) {
			LOGGER.debug(sql);
			statement.execute(sql);
		} catch (SQLException t) {
			throw new SQLExecutionException("Error executing \"" + sql + "\"", t);
		}
	}
	
	protected Connection getCurrentConnection() throws SQLException {
		return connectionProvider.getCurrentConnection();
	}
	
}
