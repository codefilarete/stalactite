package org.codefilarete.stalactite.sql.derby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.stream.LongStream;

import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.EmbedStatement;
import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.engine.SQLOperationsFactoriesBuilder;
import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelector;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.DMLNameProviderFactory;
import org.codefilarete.stalactite.sql.DatabaseSequenceSelectorFactory;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.derby.DerbyDialectResolver.DerbyDatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.DDLSequenceGenerator;
import org.codefilarete.stalactite.sql.derby.ddl.DerbyDDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.derby.statement.DerbyReadOperation;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.WriteOperation.RowCountListener;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.derby.statement.binder.DerbyParameterBinderRegistry;
import org.codefilarete.stalactite.sql.derby.statement.binder.DerbyTypeMapping;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.CaseInsensitiveSet;
import org.codefilarete.tool.function.ThrowingBiFunction;

import static org.codefilarete.tool.bean.Objects.preventNull;

/**
 * 
 * @author Guillaume Mary
 */
public class DerbyDatabaseSettings extends DatabaseVendorSettings {
	
	/**
	 * Derby keywords, took from <a href="https://db.apache.org/derby/docs/10.14/ref/rrefkeywords29722.html">Derby documentation</a> because those of
	 * it JDBC Drivers are not enough / accurate (see {@link org.apache.derby.impl.jdbc.EmbedDatabaseMetaData#getSQLKeywords()})
	 */
	@VisibleForTesting
	static final String[] KEYWORDS = new String[] {
			"ADD", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "AS", "ASC", "ASSERTION", "AT", "AUTHORIZATION", "AVG",
			"BEGIN", "BETWEEN", "BIGINT", "BIT", "BOOLEAN", "BOTH", "BY",
			"CALL", "CASCADE", "CASCADED", "CASE", "CAST", "CHAR", "CHARACTER", "CHARACTER_LENGTH", "CHECK", "CLOSE", "COALESCE", "COLLATE", "COLLATION", "COLUMN",
				"COMMIT", "CONNECT", "CONNECTION", "CONSTRAINT", "CONSTRAINTS", "CONTINUE", "CONVERT", "CORRESPONDING", "CREATE", "CROSS", "CURRENT",
				"CURRENT_DATE", "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR",
			"DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DESCRIBE", "DIAGNOSTICS", "DISCONNECT", "DISTINCT", "DOUBLE", "DROP",
			"ELSE", "END", "END-EXEC", "ESCAPE", "EXCEPT", "EXCEPTION", "EXEC", "EXECUTE", "EXISTS", "EXPLAIN", "EXTERNAL",
			"FALSE", "FETCH", "FIRST", "FLOAT", "FOR", "FOREIGN", "FOUND", "FROM", "FULL", "FUNCTION",
			"GET", "GETCURRENTCONNECTION", "GLOBAL", "GO", "GOTO", "GRANT", "GROUP",
			"HAVING", "HOUR",
			"IDENTITY", "IMMEDIATE", "IN", "INDICATOR", "INITIALLY", "INNER", "INOUT", "INPUT", "INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERSECT", "INTO", "IS", "ISOLATION",
			"JOIN",
			"KEY",
			"LAST", "LEADING", "LEFT", "LIKE", "LOWER", "LTRIM",
			"MATCH", "MAX", "MIN", "MINUTE",
			"NATIONAL", "NATURAL", "NCHAR", "NVARCHAR", "NEXT", "NO", "NONE", "NOT", "NULL", "NULLIF", "NUMERIC",
			"OF", "ON", "ONLY", "OPEN", "OPTION", "OR", "ORDER", "OUTER", "OUTPUT", "OVERLAPS",
			"PAD", "PARTIAL", "PREPARE", "PRESERVE", "PRIMARY", "PRIOR", "PRIVILEGES", "PROCEDURE", "PUBLIC",
			"READ", "REAL", "REFERENCES", "RELATIVE", "RESTRICT", "REVOKE", "RIGHT", "ROLLBACK", "ROWS", "RTRIM",
			"SCHEMA", "SCROLL", "SECOND", "SELECT", "SESSION_USER", "SET", "SMALLINT", "SOME", "SPACE", "SQL", "SQLCODE", "SQLERROR", "SQLSTATE", "SUBSTR", "SUBSTRING", "SUM", "SYSTEM_USER",
			"TABLE", "TEMPORARY", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRANSACTION", "TRANSLATE", "TRANSLATION", "TRIM", "TRUE",
			"UNION", "UNIQUE", "UNKNOWN", "UPDATE", "UPPER", "USER", "USING",
			"VALUES", "VARCHAR", "VARYING", "VIEW",
			"WHENEVER", "WHERE", "WINDOW", "WITH", "WORK", "WRITE",
			"XML", "XMLEXISTS", "XMLPARSE", "XMLQUERY", "XMLSERIALIZE",
			"YEAR"
	};
	
	// Technical note: DO NOT declare settings BEFORE KEYWORDS field because it requires it and the JVM makes KEYWORDS null at this early stage (strange)
	public static final DerbyDatabaseSettings DERBY_10_14 = new DerbyDatabaseSettings();
	
	private DerbyDatabaseSettings() {
		this(new DerbySQLOperationsFactoriesBuilder(), new DerbyParameterBinderRegistry());
	}
	
	private DerbyDatabaseSettings(DerbySQLOperationsFactoriesBuilder sqlOperationsFactoriesBuilder, DerbyParameterBinderRegistry parameterBinderRegistry) {
		super(new DerbyDatabaseSignet(10, 14),
				Collections.unmodifiableSet(new CaseInsensitiveSet(KEYWORDS)),
				'"',
				new DerbyTypeMapping(),
				parameterBinderRegistry,
				sqlOperationsFactoriesBuilder,
				new DerbyGeneratedKeysReaderFactory(),
				1000,
				true);
	}
	
	private static class DerbySQLOperationsFactoriesBuilder implements SQLOperationsFactoriesBuilder {
		
		private final DerbyReadOperationFactory readOperationFactory;
		private final DerbyWriteOperationFactory writeOperationFactory;
		
		private DerbySQLOperationsFactoriesBuilder() {
			this.readOperationFactory = new DerbyReadOperationFactory();
			this.writeOperationFactory = new DerbyWriteOperationFactory();
		}
		
		private DerbyReadOperationFactory getReadOperationFactory() {
			return readOperationFactory;
		}
		
		private DerbyWriteOperationFactory getWriteOperationFactory() {
			return writeOperationFactory;
		}
		
		@Override
		public SQLOperationsFactories build(ParameterBinderIndex<Column, ParameterBinder> parameterBinders, DMLNameProviderFactory dmlNameProviderFactory, SqlTypeRegistry sqlTypeRegistry) {
			DMLGenerator dmlGenerator = new DMLGenerator(parameterBinders, DMLGenerator.NoopSorter.INSTANCE, dmlNameProviderFactory);
			DerbyDDLTableGenerator ddlTableGenerator = new DerbyDDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
			DDLSequenceGenerator ddlSequenceGenerator = new DDLSequenceGenerator(dmlNameProviderFactory);
			return new SQLOperationsFactories(writeOperationFactory, readOperationFactory, dmlGenerator, ddlTableGenerator, ddlSequenceGenerator, new DerbySequenceSelectorFactory(readOperationFactory));
		}
	}
	
	public static class DerbyReadOperationFactory extends ReadOperationFactory {

		public DerbyReadOperationFactory() {
			this(null);
		}

		public DerbyReadOperationFactory(Integer fetchSize) {
			super(fetchSize);
		}

		@Override
		public <ParamType> ReadOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider, Integer fetchSize) {
			return new DerbyReadOperation<>(sqlGenerator, connectionProvider, preventNull(fetchSize, super.fetchSize));
		}
	}
	
	public static class DerbyWriteOperationFactory extends WriteOperationFactory {
		
		@Override
		protected <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator,
																	   ConnectionProvider connectionProvider,
																	   ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider,
																	   RowCountListener rowCountListener) {
			return new DerbyWriteOperation<ParamType>(sqlGenerator, connectionProvider, rowCountListener) {
				@Override
				protected void prepareStatement(Connection connection) throws SQLException {
					this.preparedStatement = statementProvider.apply(connection, getSQL());
				}
			};
		}
	}
	
	/**
	 * Made package-private to be visible by {@link DerbyGeneratedKeysReader}
	 * @param <ParamType>
	 */
	static class DerbyWriteOperation<ParamType> extends WriteOperation<ParamType> {
		
		/** Updated row count of the last executed batch statement */
		private long updatedRowCount = 0;
		
		public DerbyWriteOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider, RowCountListener rowCountListener) {
			super(sqlGenerator, connectionProvider, rowCountListener);
		}
		
		public long getUpdatedRowCount() {
			return updatedRowCount;
		}
		
		protected long[] doExecuteBatch() throws SQLException {
			long[] rowCounts = super.doExecuteBatch();
			this.updatedRowCount = LongStream.of(rowCounts).sum();
			return rowCounts;
		}
		
		/**
		 * Overridden to use Derby special {@link EmbedConnection#cancelRunningStatement()} method
		 * to avoid exception "ERROR 0A000: Feature not implemented: cancel" (see {@link EmbedStatement#cancel()} implementation).
		 *
		 * @throws SQLException if cancellation fails
		 */
		@Override
		public void cancel() throws SQLException {
			EmbedConnection conn = preparedStatement.getConnection().unwrap(EmbedConnection.class);
			conn.cancelRunningStatement();
		}
	}
	
	private static class DerbySequenceSelectorFactory implements DatabaseSequenceSelectorFactory {
		
		private final ReadOperationFactory readOperationFactory;
		
		private DerbySequenceSelectorFactory(ReadOperationFactory readOperationFactory) {
			this.readOperationFactory = readOperationFactory;
		}
		
		@Override
		public DatabaseSequenceSelector create(org.codefilarete.stalactite.sql.ddl.structure.Sequence databaseSequence, ConnectionProvider connectionProvider) {
			return new DatabaseSequenceSelector(databaseSequence, "values next value for " + databaseSequence.getAbsoluteName(), readOperationFactory, connectionProvider);
		}
	}
	
	@VisibleForTesting
	static class DerbyGeneratedKeysReaderFactory implements GeneratedKeysReaderFactory {
		@Override
		public <I> GeneratedKeysReader<I> build(String keyName, Class<I> columnType) {
			return (GeneratedKeysReader<I>) new DerbyGeneratedKeysReader();
		}
	}
}
