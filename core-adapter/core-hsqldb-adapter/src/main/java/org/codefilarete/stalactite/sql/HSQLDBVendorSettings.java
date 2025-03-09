package org.codefilarete.stalactite.sql;

import java.util.Collections;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.engine.SQLOperationsFactoriesBuilder;
import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelector;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory.DefaultGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.HSQLDBDialect.HSQLDBWriteOperationFactory;
import org.codefilarete.stalactite.sql.ddl.DDLSequenceGenerator;
import org.codefilarete.stalactite.sql.ddl.HSQLDBDDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.DMLGenerator.NoopSorter;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.HSQLDBParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.HSQLDBTypeMapping;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.tool.collection.CaseInsensitiveSet;

/**
 * Database vendor settings for HSQLDB.
 * 
 * @author Guillaume Mary
 */
public class HSQLDBVendorSettings extends DatabaseVendorSettings {
	
	/**
	 * HSQLDB keywords, took at <a href="https://hsqldb.org/doc/guide/lists-app.html">HSQLDB keywords</a>
	 * Programmatically available {@link org.hsqldb.Tokens#isKeyword(String)} but not through {@link java.sql.DatabaseMetaData#getSQLKeywords()}
	 * which returns "" (see {@link org.hsqldb.jdbc.JDBCDatabaseMetaData#getSQLKeywords()}).
	 */
	private static final String[] KEYWORDS = new String[] {
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
	
	public HSQLDBVendorSettings() {
		this(new ReadOperationFactory(), new HSQLDBParameterBinderRegistry());
	}
	
	private HSQLDBVendorSettings(ReadOperationFactory readOperationFactory, HSQLDBParameterBinderRegistry parameterBinderRegistry) {
		super(Collections.unmodifiableSet(new CaseInsensitiveSet(KEYWORDS)),
				'"',
				new HSQLDBTypeMapping(),
				parameterBinderRegistry,
				new HSQLDBSQLOperationsFactoriesBuilder(),
				new DefaultGeneratedKeysReaderFactory(parameterBinderRegistry),
				new HSQLDBDatabaseSequenceSelectorFactory(readOperationFactory),
				100,
				true);
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
	
	private static class HSQLDBSQLOperationsFactoriesBuilder implements SQLOperationsFactoriesBuilder {
		
		@Override
		public SQLOperationsFactories build(ParameterBinderIndex<Column, ParameterBinder> parameterBinders, DMLNameProviderFactory dmlNameProviderFactory, SqlTypeRegistry sqlTypeRegistry) {
			DMLGenerator dmlGenerator = new DMLGenerator(parameterBinders, NoopSorter.INSTANCE, dmlNameProviderFactory);
			HSQLDBDDLTableGenerator ddlTableGenerator = new HSQLDBDDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
			DDLSequenceGenerator ddlSequenceGenerator = new DDLSequenceGenerator(dmlNameProviderFactory);
			return new SQLOperationsFactories(new HSQLDBWriteOperationFactory(), new ReadOperationFactory(), dmlGenerator, ddlTableGenerator, ddlSequenceGenerator);
		}
	}
}
