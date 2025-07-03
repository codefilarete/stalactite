package org.codefilarete.stalactite.sql.ddl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.mapping.id.manager.BeforeInsertIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelector;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.codefilarete.stalactite.sql.ddl.structure.Table.COMPARATOR_ON_SCHEMA_AND_NAME;

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
	public static Collection<Table<?>> collectTables(PersistenceContext persistenceContext) {
		Set<Table<?>> result = new TreeSet<>(COMPARATOR_ON_SCHEMA_AND_NAME);
		persistenceContext.getPersisters().forEach(p -> result.addAll(((ConfiguredPersister<?, ?>) p).giveImpliedTables()));
		return result;
	}
	
	public static Collection<Sequence> collectSequences(PersistenceContext persistenceContext) {
		Set<Sequence> result = new LinkedHashSet<>(20);
		persistenceContext.getPersisters().forEach(p -> {
			IdentifierInsertionManager<?, ?> identifierInsertionManager = ((ConfiguredPersister<?, ?>) p).getMapping().getIdMapping().getIdentifierInsertionManager();
			if (identifierInsertionManager instanceof BeforeInsertIdentifierManager
					&& ((BeforeInsertIdentifierManager<?, ?>) identifierInsertionManager).getIdentifierFixer().getSequence() instanceof DatabaseSequenceSelector) {
				DatabaseSequenceSelector databaseSequenceSelector = (DatabaseSequenceSelector) ((BeforeInsertIdentifierManager<?, ?>) identifierInsertionManager).getIdentifierFixer().getSequence();
				result.add(databaseSequenceSelector.getDatabaseSequence());
			}
		});
		return result;
	}
	
	private final ConnectionProvider connectionProvider;
	
	private final DDLGenerator ddlGenerator;
	
	/**
	 * Simple constructor that gets its information from the given {@link PersistenceContext}: {@link DDLGenerator} from its
	 * {@link Dialect} and {@link ConnectionProvider}
	 * 
	 * @param persistenceContext a {@link PersistenceContext}, source of arguments for {@link #DDLDeployer(DDLTableGenerator, DDLSequenceGenerator, ConnectionProvider)}
	 * @see #DDLDeployer(DDLTableGenerator, DDLSequenceGenerator, ConnectionProvider) 
	 */
	public DDLDeployer(PersistenceContext persistenceContext) {
		this(persistenceContext.getDialect().getDdlTableGenerator(), persistenceContext.getDialect().getDdlSequenceGenerator(), persistenceContext.getConnectionProvider());
		ddlGenerator.addTables(collectTables(persistenceContext));
		ddlGenerator.addSequences(collectSequences(persistenceContext));
	}
	
	/**
	 * Main constructor with mandatory objects for its work.
	 * Tables to deploy must be added through {@link #getDdlGenerator()}.addTables(..)
	 * 
	 * @param ddlTableGenerator the SQL scripts provider
	 * @param connectionProvider the {@link Connection} provider for executing SQL scripts
	 */
	public DDLDeployer(DDLTableGenerator ddlTableGenerator, DDLSequenceGenerator ddlSequenceGenerator, ConnectionProvider connectionProvider) {
		this.connectionProvider = connectionProvider;
		this.ddlGenerator = new DDLGenerator(ddlTableGenerator, ddlSequenceGenerator);
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
