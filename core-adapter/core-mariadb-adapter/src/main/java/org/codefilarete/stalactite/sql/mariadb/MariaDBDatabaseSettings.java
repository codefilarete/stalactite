package org.codefilarete.stalactite.sql.mariadb;

import java.util.Collections;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.engine.SQLOperationsFactoriesBuilder;
import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelector;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.DMLNameProviderFactory;
import org.codefilarete.stalactite.sql.DatabaseSequenceSelectorFactory;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.mariadb.MariaDBDialectResolver.MariaDBDatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.DDLSequenceGenerator;
import org.codefilarete.stalactite.sql.mariadb.ddl.MariaDBDDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.mariadb.statement.binder.MariaDBParameterBinderRegistry;
import org.codefilarete.stalactite.sql.mariadb.statement.binder.MariaDBTypeMapping;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.CaseInsensitiveSet;

/**
 * 
 * @author Guillaume Mary
 */
public class MariaDBDatabaseSettings extends DatabaseVendorSettings {
	
	
	/**
	 * MariaDB keywords, took from <a href="https://mariadb.com/kb/en/reserved-words/">MariaDB documentation</a> because those of
	 * it JDBC Drivers are not enough / accurate. (see {@link org.mariadb.jdbc.DatabaseMetaData#getSQLKeywords()})
	 */
	public static final String[] KEYWORDS = new String[] {
			"ACCESSIBLE", "ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE",
			"BEFORE", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOTH", "BY",
			"CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK", "COLLATE", "COLUMN", "CONDITION", "CONSTRAINT", "CONTINUE", "CONVERT",
				"CREATE", "CROSS", "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR",
			"DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELAYED",
				"DELETE", "DELETE_DOMAIN_ID", "DESC", "DESCRIBE", "DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DO_DOMAIN_IDS", "DOUBLE", "DROP", "DUAL",
			"EACH", "ELSE", "ELSEIF", "ENCLOSED", "ESCAPED", "EXCEPT", "EXISTS", "EXIT", "EXPLAIN",
			"FALSE", "FETCH", "FLOAT", "FLOAT4", "FLOAT8", "FOR", "FORCE", "FOREIGN", "FROM", "FULLTEXT",
			"GENERAL", "GRANT", "GROUP",
			"HAVING", "HIGH_PRIORITY", "HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND",
			"IF", "IGNORE", "IGNORE_DOMAIN_IDS", "IGNORE_SERVER_IDS", "IN", "INDEX", "INFILE", "INNER", "INOUT", "INSENSITIVE", "INSERT", "INT",
				"INT1", "INT2", "INT3", "INT4", "INT8", "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IS", "ITERATE",
			"JOIN",
			"KEY", "KEYS", "KILL",
			"LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT", "LINEAR", "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB", "LONGTEXT", "LOOP", "LOW_PRIORITY",
			"MASTER_HEARTBEAT_PERIOD", "MASTER_SSL_VERIFY_SERVER_CERT", "MATCH", "MAXVALUE", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT",
				"MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "MODIFIES",
			"NATURAL", "NOT", "NO_WRITE_TO_BINLOG", "NULL", "NUMERIC",
			"OFFSET", "ON", "OPTIMIZE", "OPTION", "OPTIONALLY",
			"OR", "ORDER", "OUT", "OUTER", "OUTFILE", "OVER",
			"PAGE_CHECKSUM", "PARSE_VCOL_EXPR", "PARTITION", "PRECISION", "PRIMARY", "PROCEDURE", "PURGE",
			"RANGE", "READ", "READS", "READ_WRITE", "REAL", "RECURSIVE", "REF_SYSTEM_ID", "REFERENCES", "REGEXP", "RELEASE", "RENAME", "REPEAT",
				"REPLACE", "REQUIRE", "RESIGNAL", "RESTRICT", "RETURN", "RETURNING", "REVOKE", "RIGHT", "RLIKE", "ROW_NUMBER", "ROWS",
			"SCHEMA", "SCHEMAS", "SECOND_MICROSECOND", "SELECT", "SENSITIVE", "SEPARATOR", "SET", "SHOW", "SIGNAL", "SLOW", "SMALLINT", "SPATIAL",
				"SPECIFIC", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL",
				"STARTING", "STATS_AUTO_RECALC", "STATS_PERSISTENT", "STATS_SAMPLE_PAGES", "STRAIGHT_JOIN",
			"TABLE", "TERMINATED", "THEN", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING", "TRIGGER", "TRUE",
			"UNDO", "UNION", "UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE", "USAGE", "USE", "USING", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP",
			"VALUES", "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "VECTOR",
			"WHEN", "WHERE", "WHILE", "WINDOW", "WITH", "WRITE",
			"XOR",
			"YEAR_MONTH",
			"ZEROFILL",
	};
	
	public static final MariaDBDatabaseSettings MARIADB_10_0 = new MariaDBDatabaseSettings();
	
	private MariaDBDatabaseSettings() {
		this(new MariaDBSQLOperationsFactoriesBuilder(), new MariaDBParameterBinderRegistry());
	}
	
	private MariaDBDatabaseSettings(MariaDBSQLOperationsFactoriesBuilder sqlOperationsFactoriesBuilder, MariaDBParameterBinderRegistry parameterBinderRegistry) {
		super(new MariaDBDatabaseSignet(10, 0),
				Collections.unmodifiableSet(new CaseInsensitiveSet(KEYWORDS)),
				'`',
				new MariaDBTypeMapping(),
				parameterBinderRegistry,
				sqlOperationsFactoriesBuilder,
				new MariaDBGeneratedKeysReaderFactory(),
				1000,
				true);
	}
	
	private static class MariaDBSQLOperationsFactoriesBuilder implements SQLOperationsFactoriesBuilder {
		
		private final ReadOperationFactory readOperationFactory;
		private final MariaDBWriteOperationFactory writeOperationFactory;
		
		private MariaDBSQLOperationsFactoriesBuilder() {
			this.readOperationFactory = new ReadOperationFactory();
			this.writeOperationFactory = new MariaDBWriteOperationFactory();
		}
		
		private ReadOperationFactory getReadOperationFactory() {
			return readOperationFactory;
		}
		
		private WriteOperationFactory getWriteOperationFactory() {
			return writeOperationFactory;
		}
		
		@Override
		public SQLOperationsFactories build(ParameterBinderIndex<Column, ParameterBinder> parameterBinders, DMLNameProviderFactory dmlNameProviderFactory, SqlTypeRegistry sqlTypeRegistry) {
			DMLGenerator dmlGenerator = new DMLGenerator(parameterBinders, DMLGenerator.NoopSorter.INSTANCE, dmlNameProviderFactory);
			MariaDBDDLTableGenerator ddlTableGenerator = new MariaDBDDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
			DDLSequenceGenerator ddlSequenceGenerator = new DDLSequenceGenerator(dmlNameProviderFactory);
			return new SQLOperationsFactories(writeOperationFactory, readOperationFactory, dmlGenerator, ddlTableGenerator, ddlSequenceGenerator, new MariaDBSequenceSelectorFactory(readOperationFactory));
		}
	}
	
	private static class MariaDBSequenceSelectorFactory implements DatabaseSequenceSelectorFactory {
		
		private final ReadOperationFactory readOperationFactory;
		
		private MariaDBSequenceSelectorFactory(ReadOperationFactory readOperationFactory) {
			this.readOperationFactory = readOperationFactory;
		}
		
		@Override
		public DatabaseSequenceSelector create(org.codefilarete.stalactite.sql.ddl.structure.Sequence databaseSequence, ConnectionProvider connectionProvider) {
			return new DatabaseSequenceSelector(databaseSequence, "select next value for " + databaseSequence.getAbsoluteName(), readOperationFactory, connectionProvider);
		}
	}
	
	
	@VisibleForTesting
	static class MariaDBGeneratedKeysReaderFactory implements GeneratedKeysReaderFactory {
		@Override
		public <I> GeneratedKeysReader<I> build(String keyName, Class<I> columnType) {
			return (GeneratedKeysReader<I>) new MariaDBGeneratedKeysReader();
		}
	}
}
