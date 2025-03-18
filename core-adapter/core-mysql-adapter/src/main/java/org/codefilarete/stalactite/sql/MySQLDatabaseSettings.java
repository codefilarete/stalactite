package org.codefilarete.stalactite.sql;

import java.util.Collections;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.engine.SQLOperationsFactoriesBuilder;
import org.codefilarete.stalactite.mapping.id.sequence.SequenceStoredAsTableSelector;
import org.codefilarete.stalactite.sql.MySQLDialectResolver.MySQLDatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.DDLSequenceGenerator;
import org.codefilarete.stalactite.sql.ddl.MySQLDDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.MySQLParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.MySQLTypeMapping;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.CaseInsensitiveSet;

import static org.codefilarete.tool.bean.Objects.preventNull;

/**
 * 
 * @author Guillaume Mary
 */
public class MySQLDatabaseSettings extends DatabaseVendorSettings {
	
	/**
	 * MySQL keywords, took from <a href="https://MySQL.com/kb/en/reserved-words/">MySQL documentation</a> because those of
	 * it JDBC Drivers are took dynamically from database. (see {@link com.mysql.cj.jdbc.DatabaseMetaData#getSQLKeywords()})
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
	
	public static final MySQLDatabaseSettings MYSQL_5_6 = new MySQLDatabaseSettings();
	
	private MySQLDatabaseSettings() {
		this(new MySQLSQLOperationsFactoriesBuilder(), new MySQLParameterBinderRegistry());
	}
	
	private MySQLDatabaseSettings(MySQLSQLOperationsFactoriesBuilder sqlOperationsFactoriesBuilder, MySQLParameterBinderRegistry parameterBinderRegistry) {
		super(new MySQLDatabaseSignet(5, 6),
				Collections.unmodifiableSet(new CaseInsensitiveSet(KEYWORDS)),
				'`',
				new MySQLTypeMapping(),
				parameterBinderRegistry,
				sqlOperationsFactoriesBuilder,
				new MySQLGeneratedKeysReaderFactory(),
				1000,
				true);
	}
	
	private static class MySQLSQLOperationsFactoriesBuilder implements SQLOperationsFactoriesBuilder {
		
		private final ReadOperationFactory readOperationFactory;
		private final MySQLWriteOperationFactory writeOperationFactory;
		
		private MySQLSQLOperationsFactoriesBuilder() {
			this.readOperationFactory = new ReadOperationFactory();
			this.writeOperationFactory = new MySQLWriteOperationFactory();
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
			MySQLDDLTableGenerator ddlTableGenerator = new MySQLDDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
			DDLSequenceGenerator ddlSequenceGenerator = new DDLSequenceGenerator(dmlNameProviderFactory);
			return new SQLOperationsFactories(writeOperationFactory, readOperationFactory, dmlGenerator, ddlTableGenerator, ddlSequenceGenerator,
					new MySQLSequenceSelectorFactory(readOperationFactory, writeOperationFactory, dmlGenerator));
		}
	}
	
	@VisibleForTesting
	static class MySQLSequenceSelectorFactory implements DatabaseSequenceSelectorFactory {
		
		private final ReadOperationFactory readOperationFactory;
		private final WriteOperationFactory writeOperationFactory;
		private final DMLGenerator dmlGenerator;
		
		private MySQLSequenceSelectorFactory(ReadOperationFactory readOperationFactory, WriteOperationFactory writeOperationFactory, DMLGenerator dmlGenerator) {
			this.readOperationFactory = readOperationFactory;
			this.writeOperationFactory = writeOperationFactory;
			this.dmlGenerator = dmlGenerator;
		}
		
		@Override
		public org.codefilarete.tool.function.Sequence<Long> create(Sequence databaseSequence, ConnectionProvider connectionProvider) {
			return new SequenceStoredAsTableSelector(
					databaseSequence.getSchema(),
					databaseSequence.getName(),
					preventNull(databaseSequence.getInitialValue(), 1),
					preventNull(databaseSequence.getBatchSize(), 1),
					dmlGenerator,
					readOperationFactory,
					writeOperationFactory,
					connectionProvider);
		}
	}
	
	
	@VisibleForTesting
	static class MySQLGeneratedKeysReaderFactory implements GeneratedKeysReaderFactory {
		@Override
		public <I> GeneratedKeysReader<I> build(String keyName, Class<I> columnType) {
			return (GeneratedKeysReader<I>) new MySQLGeneratedKeysReader();
		}
	}
}
