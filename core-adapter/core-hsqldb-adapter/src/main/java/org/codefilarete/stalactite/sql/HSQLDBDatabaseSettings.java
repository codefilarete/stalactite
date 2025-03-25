package org.codefilarete.stalactite.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.engine.SQLOperationsFactoriesBuilder;
import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelector;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory.DefaultGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.HSQLDBDialectResolver.HSQLDBDatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.DDLSequenceGenerator;
import org.codefilarete.stalactite.sql.ddl.HSQLDBDDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.DMLGenerator.NoopSorter;
import org.codefilarete.stalactite.sql.statement.HSQLDBWriteOperation;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.WriteOperation.RowCountListener;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.HSQLDBParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.HSQLDBTypeMapping;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.CaseInsensitiveSet;
import org.codefilarete.tool.function.ThrowingBiFunction;

/**
 * Database vendor settings for HSQLDB.
 * 
 * @author Guillaume Mary
 */
public class HSQLDBDatabaseSettings extends DatabaseVendorSettings {
	
	/**
	 * HSQLDB keywords, took at <a href="https://hsqldb.org/doc/guide/lists-app.html">HSQLDB keywords</a> in chapter
	 * "List of SQL Keywords Disallowed as HyperSQL Identifiers".
	 * Because HSQLDB has 2 modes (depending on "SQL NAMES" database option) and below keywords are the one strictly
	 * prohibited.
	 * @see #EXTENDED_KEYWORDS
	 */
	@VisibleForTesting
	static final String[] KEYWORDS = new String[] {
			"ALL", "AND", "ANY", "ARRAY", "AS", "AT",
			"BETWEEN", "BOTH", "BY",
			"CALL", "CASE", "CAST", "COALESCE", "CONVERT", "CORRESPONDING", "COUNT", "CREATE", "CROSS", "CUBE",
			"DEFAULT", "DISTINCT", "DO", "DROP",
			"ELSE", "EVERY", "EXCEPT", "EXISTS",
			"FETCH", "FOR", "FROM", "FULL",
			"GRANT", "GROUP", "GROUPING",
			"HAVING",
			"IN", "INNER", "INTERSECT", "INTO", "IS",
			"JOIN",
			"LEADING", "LEFT", "LIKE",
			"MAX", "MIN",
			"NATURAL", "NOT", "NULLIF",
			"ON", "OR", "ORDER", "OUTER",
			"PRIMARY",
			"REFERENCES", "RIGHT", "ROLLUP",
			"SELECT", "SET", "SOME", "STDDEV_POP", "STDDEV_SAMP", "SUM",
			"TABLE", "THEN", "TO", "TRAILING", "TRIGGER",
			"UNION", "UNIQUE", "USING",
			"VALUES", "VAR_POP", "VAR_SAMP",
			"WHEN", "WHERE", "WITH"
	};
	
	/**
	 * Extended HSQLDB keywords, took at <a href="https://hsqldb.org/doc/guide/lists-app.html">HSQLDB keywords</a> in chapter
	 * "List of SQL Standard Keywords".
	 * Source available at {@link org.hsqldb.Tokens#isKeyword(String)} but not through {@link java.sql.DatabaseMetaData#getSQLKeywords()}
	 * which returns "" (see {@link org.hsqldb.jdbc.JDBCDatabaseMetaData#getSQLKeywords()}).
	 */
	static final String[] EXTENDED_KEYWORDS = new String[] {
			"ABS", "ABSENT", "ACOS", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ANY_VALUE", "ARE", "ARRAY", "ARRAY_AGG", "ARRAY_MAX_CARDINALITY", "AS", "ASENSITIVE", "ASIN", "ASYMMETRIC", "AT", "ATAN", "ATOMIC", "AUTHORIZATION", "AVG",
			"BEGIN", "BEGIN_FRAME", "BEGIN_PARTITION", "BETWEEN", "BIGINT", "BINARY", "BIT_LENGTH", "BLOB", "BOOLEAN", "BOTH", "BY",
			"CALL", "CALLED", "CARDINALITY", "CASCADED", "CASE", "CAST", "CEIL", "CEILING", "CHAR", "CHARACTER", "CHARACTER_LENGTH", "CHAR_LENGTH", "CHECK", "CLOB", "CLOSE", "COALESCE", "COLLATE", "COLLECT", "COLUMN", "COMMIT", "COMPARABLE", "CONDIITON", "CONNECT", "CONSTRAINT", "CONTAINS", "CONVERT", "CORR", "CORRESPONDING", "COS", "COSH", "COUNT", "COVAR_POP", "COVAR_SAMP", "CREATE", "CROSS", "CUBE", "CUME_DIST", "CURRENT", "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_ROW", "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER", "CURSOR", "CYCLE",
			"DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELETE", "DENSE_RANK", "DEREF", "DESCRIBE", "DETERMINISTIC", "DISCONNECT", "DISTINCT", "DO", "DOUBLE", "DROP", "DYNAMIC",
			"EACH", "ELEMENT", "ELSE", "ELSEIF", "EMPTY", "END", "END_EXEC", "END_FRAME", "END_PARTITION", "EQUALS", "ESCAPE", "EVERY", "EXCEPT", "EXEC", "EXECUTE", "EXISTS", "EXIT", "EXP", "EXTERNAL", "EXTRACT",
			"FALSE", "FETCH", "FILTER", "FIRST_VALUE", "FLOAT", "FLOOR", "FOR", "FOREIGN", "FRAME_ROW", "FREE", "FROM", "FULL", "FUNCTION", "FUSION",
			"GET", "GLOBAL", "GRANT", "GROUP", "GROUPING", "GROUPS",
			"HANDLER", "HAVING", "HOLD", "HOUR",
			"IDENTITY", "IF", "IMPORT", "IN", "INDICATOR", "INITIAL", "INNER", "INOUT", "INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERSECT", "INTERSECTION", "INTERVAL", "INTO", "IS", "ITERATE",
			"JOIN", "JSON", "JSON_ARRAY", "JSON_ARRAYAGG", "JSON_EXISTS", "JSON_OBJECT", "JSON_OBJECTAGG", "JSON_QUERY", "JSON_TABLE", "JSON_VALUE",
			"LAG", "LANGUAGE", "LARGE", "LAST_VALUE", "LATERAL", "LEAD", "LEADING", "LEAST", "LEAVE", "LEFT", "LIKE", "LIKE_REGX", "LISTAGG", "LN", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP", "LOG", "LOG10", "LOOP", "LOWER", "LPAD", "LTRIM",
			"MATCH", "MAX", "MAX_CARDINALITY", "MEMBER", "MERGE", "METHOD", "MIN", "MINUTE", "MOD", "MODIFIES", "MODULE", "MONTH", "MULTISET",
			"NATIONAL", "NATURAL", "NCHAR", "NCLOB", "NEW", "NO", "NONE", "NORMALIZE", "NOT", "NTH_VALUE", "NTILE", "NULL", "NULLIF", "NUMERIC",
			"OCCURRENCES_REGEX", "OCTET_LENGTH", "OF", "OFFSET", "OLD", "OMIT", "ON", "ONLY", "OPEN", "OR", "ORDER", "OUT", "OUTER", "OVER", "OVERLAPS", "OVERLAY",
			"PARAMETER", "PARTITION", "PERCENTILE_CONT", "PERCENTILE_DISC", "PERCENT_RANK", "PERIOD", "POSITION", "POSITION_REGEX", "POWER", "PRECEDES", "PRECISION", "PREPARE", "PRIMARY", "PROCEDURE",
			"RANGE", "RANK", "READS", "REAL", "RECURSIVE", "REF", "REFERENCES", "REFERENCING", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY", "RELEASE", "REPEAT", "RESIGNAL", "RESULT", "RETURN", "RETURNS", "REVOKE", "RIGHT", "ROLLBACK", "ROLLUP", "ROW", "ROWS", "ROW_NUMBER", "RPAD", "RTRIM",
			"SAVEPOINT", "SCOPE", "SCROLL", "SEARCH", "SECOND", "SELECT", "SENSITIVE", "SESSION_USER", "SET", "SIGNAL", "SIMILAR", "SIN", "SINH", "SMALLINT", "SOME", "SPECIFIC", "SPECIFICTYPE", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQRT", "STACKED", "START", "STATIC", "STDDEV_POP", "STDDEV_SAMP", "SUBMULTISET", "SUBSTRING", "SUBSTRING_REGEX", "SUCCEEDS", "SUM", "SYMMETRIC", "SYSTEM", "SYSTEM_TIME", "SYSTEM_USER",
			"TABLE", "TABLESAMPLE", "TAN", "TANH", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING", "TRANSLATE", "TRANSLATE_REGEX", "TRANSLATION", "TREAT", "TRIGGER", "TRIM", "TRIM_ARRAY", "TRUE", "TRUNCATE",
			"UESCAPE", "UNDO", "UNION", "UNIQUE", "UNKNOWN", "UNNEST", "UNTIL", "UPDATE", "UPPER", "USER", "USING",
			"VALUE", "VALUES", "VALUE_OF", "VARBINARY", "VARCHAR", "VARYING", "VAR_POP", "VAR_SAMP", "VERSIONING",
			"WHEN", "WHENEVER", "WHERE", "WHILE", "WIDTH_BUCKET", "WINDOW", "WITH", "WITHIN", "WITHOUT",
			"YEAR"
	};
	
	// Technical note: DO NOT declare settings BEFORE KEYWORDS field because it requires it and the JVM makes KEYWORDS null at this early stage (strange)
	public static final HSQLDBDatabaseSettings HSQLDB_2_7 = new HSQLDBDatabaseSettings();
	
	private HSQLDBDatabaseSettings() {
		this(new HSQLDBSQLOperationsFactoriesBuilder(), new HSQLDBParameterBinderRegistry());
	}
	
	private HSQLDBDatabaseSettings(HSQLDBSQLOperationsFactoriesBuilder sqlOperationsFactoriesBuilder, HSQLDBParameterBinderRegistry parameterBinderRegistry) {
		super(new HSQLDBDatabaseSignet(2, 7),
				Collections.unmodifiableSet(new CaseInsensitiveSet(KEYWORDS)),
				'"',
				new HSQLDBTypeMapping(),
				parameterBinderRegistry,
				sqlOperationsFactoriesBuilder,
				new DefaultGeneratedKeysReaderFactory(parameterBinderRegistry),
				1000,
				true);
	}
	
	private static class HSQLDBSQLOperationsFactoriesBuilder implements SQLOperationsFactoriesBuilder {
		
		private final ReadOperationFactory readOperationFactory;
		private final HSQLDBWriteOperationFactory writeOperationFactory;
		
		private HSQLDBSQLOperationsFactoriesBuilder() {
			this.readOperationFactory = new ReadOperationFactory();
			this.writeOperationFactory = new HSQLDBWriteOperationFactory();
		}
		
		private ReadOperationFactory getReadOperationFactory() {
			return readOperationFactory;
		}
		
		private HSQLDBWriteOperationFactory getWriteOperationFactory() {
			return writeOperationFactory;
		}
		
		@Override
		public SQLOperationsFactories build(ParameterBinderIndex<Column, ParameterBinder> parameterBinders, DMLNameProviderFactory dmlNameProviderFactory, SqlTypeRegistry sqlTypeRegistry) {
			DMLGenerator dmlGenerator = new DMLGenerator(parameterBinders, NoopSorter.INSTANCE, dmlNameProviderFactory);
			HSQLDBDDLTableGenerator ddlTableGenerator = new HSQLDBDDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
			DDLSequenceGenerator ddlSequenceGenerator = new DDLSequenceGenerator(dmlNameProviderFactory);
			return new SQLOperationsFactories(writeOperationFactory, readOperationFactory, dmlGenerator, ddlTableGenerator, ddlSequenceGenerator, new HSQLDBDatabaseSequenceSelectorFactory(readOperationFactory));
		}
	}

	@VisibleForTesting
	static class HSQLDBWriteOperationFactory extends WriteOperationFactory {
		
		@Override
		protected <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator,
																	   ConnectionProvider connectionProvider,
																	   ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider,
																	   RowCountListener rowCountListener) {
			return new HSQLDBWriteOperation<ParamType>(sqlGenerator, connectionProvider, rowCountListener) {
				@Override
				protected void prepareStatement(Connection connection) throws SQLException {
					this.preparedStatement = statementProvider.apply(connection, getSQL());
				}
			};
		}
	}
	
	private static class HSQLDBDatabaseSequenceSelectorFactory implements DatabaseSequenceSelectorFactory {
		
		private final ReadOperationFactory readOperationFactory;
		
		private HSQLDBDatabaseSequenceSelectorFactory(ReadOperationFactory readOperationFactory) {
			this.readOperationFactory = readOperationFactory;
		}
		
		@Override
		public DatabaseSequenceSelector create(Sequence databaseSequence, ConnectionProvider connectionProvider) {
			return new DatabaseSequenceSelector(databaseSequence, "CALL NEXT VALUE FOR " + databaseSequence.getName(), readOperationFactory, connectionProvider);
		}
	}
}
