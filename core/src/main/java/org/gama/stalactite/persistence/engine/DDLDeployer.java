package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.gama.stalactite.persistence.engine.runtime.ConfiguredPersister;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.DDLGenerator;
import org.gama.stalactite.persistence.sql.ddl.DDLTableGenerator;
import org.gama.stalactite.persistence.sql.ddl.SqlTypeRegistry;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.dml.SQLExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class aimed at deploying DDL elements to a database. It gets its elements from a {@link DDLGenerator} and execute them
 * onto a {@link ConnectionProvider}, so it's more an entry point for high level usage.
 *
 * @author Guillaume Mary
 * @see DDLGenerator
 */
public class DDLDeployer {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DDLDeployer.class);
	
	/**
	 * Collect tables defined in the given {@link PersistenceContext}
	 *
	 * @param persistenceContext a {@link PersistenceContext} to scan for tables
	 * @return a {@link Collection} of found tables
	 */
	public static Collection<Table> collectTables(PersistenceContext persistenceContext) {
		List<Table> result = new ArrayList<>(20);
		persistenceContext.getPersisters().forEach(p -> result.addAll(((ConfiguredPersister) p).giveImpliedTables()));
		return result;
	}
	
	private final ConnectionProvider connectionProvider;
	
	private final DDLGenerator ddlGenerator;
	
	/**
	 * Simple constructor that gets its informations from the given {@link PersistenceContext}: {@link DDLGenerator} from its
	 * {@link Dialect} and {@link ConnectionProvider}
	 * 
	 * @param persistenceContext a {@link PersistenceContext}, source of arguments for {@link #DDLDeployer(DDLTableGenerator, ConnectionProvider)}
	 * @see #DDLDeployer(DDLTableGenerator, ConnectionProvider) 
	 */
	public DDLDeployer(PersistenceContext persistenceContext) {
		this(persistenceContext.getDialect().getDdlTableGenerator(), persistenceContext.getConnectionProvider());
		ddlGenerator.addTables(collectTables(persistenceContext));
	}
	
	/**
	 * Basic constructor that will create id own default {@link DDLGenerator}
	 * Tables to deploy must be added throught {@link #getDdlGenerator()}.addTables(..)
	 * 
	 * @param sqlTypeRegistry the SQL types per Column or Java type provider
	 * @param connectionProvider the {@link Connection} provider for executing SQL scripts
	 */
	public DDLDeployer(SqlTypeRegistry sqlTypeRegistry, ConnectionProvider connectionProvider) {
		this(new DDLTableGenerator(sqlTypeRegistry), connectionProvider);
	}
	
	/**
	 * Main constructor with mandatory objects for its work.
	 * Tables to deploy must be added throught {@link #getDdlGenerator()}.addTables(..)
	 * 
	 * @param ddlTableGenerator the SQL scripts provider
	 * @param connectionProvider the {@link Connection} provider for executing SQL scripts
	 */
	public DDLDeployer(DDLTableGenerator ddlTableGenerator, ConnectionProvider connectionProvider) {
		this.connectionProvider = connectionProvider;
		this.ddlGenerator = new DDLGenerator(ddlTableGenerator);
	}
	
	public DDLGenerator getDdlGenerator() {
		return ddlGenerator;
	}
	
	public void deployDDL() {
		execute(getCreationScripts());
	}
	
	public List<String> getCreationScripts() {
		return getDdlGenerator().getCreationScripts();
	}
	
	public void dropDDL() {
		execute(getDropScripts());
	}
	
	public List<String> getDropScripts() {
		return getDdlGenerator().getDropScripts();
	}
	
	protected void execute(List<String> sqls) {
		Connection currentConnection = getCurrentConnection();
		for (String sql : sqls) {
			try (Statement statement = currentConnection.createStatement()) {
				LOGGER.debug(sql);
				statement.execute(sql);
			} catch (SQLException t) {
				throw new SQLExecutionException(sql, t);
			}
		}
	}
	
	protected Connection getCurrentConnection() {
		return connectionProvider.giveConnection();
	}
	
}
