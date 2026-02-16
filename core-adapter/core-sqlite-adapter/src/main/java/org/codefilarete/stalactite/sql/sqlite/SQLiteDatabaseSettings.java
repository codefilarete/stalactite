package org.codefilarete.stalactite.sql.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.stream.LongStream;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.engine.SQLOperationsFactoriesBuilder;
import org.codefilarete.stalactite.mapping.id.sequence.SequenceStoredAsTableSelector;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.DMLNameProviderFactory;
import org.codefilarete.stalactite.sql.DatabaseSequenceSelectorFactory;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.sqlite.SQLiteDialectResolver.SQLiteDatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.DDLSequenceGenerator;
import org.codefilarete.stalactite.sql.sqlite.ddl.SQLiteDDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.WriteOperation.RowCountListener;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.stalactite.sql.sqlite.statement.binder.SQLiteParameterBinderRegistry;
import org.codefilarete.stalactite.sql.sqlite.statement.binder.SQLiteTypeMapping;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.CaseInsensitiveSet;
import org.codefilarete.tool.function.ThrowingBiFunction;

import static org.codefilarete.tool.bean.Objects.preventNull;

/**
 * 
 * @author Guillaume Mary
 */
public class SQLiteDatabaseSettings extends DatabaseVendorSettings {
	
	/**
	 * SQLite keywords, took from <a href="https://sqlite.org/lang_keywords.html">SQLite documentation</a>
	 */
	public static final String[] KEYWORDS = new String[] {
			"ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ALWAYS", "ANALYZE", "AND", "AS", "ASC", "ATTACH", "AUTOINCREMENT",
			"BEFORE", "BEGIN", "BETWEEN", "BY",
			"CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT", "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
			"DATABASE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DO", "DROP",
			"EACH", "ELSE", "END", "ESCAPE", "EXCEPT", "EXCLUDE", "EXCLUSIVE", "EXISTS", "EXPLAIN",
			"FAIL", "FILTER", "FIRST", "FOLLOWING", "FOR", "FOREIGN", "FROM", "FULL",
			"GENERATED", "GLOB", "GROUP", "GROUPS",
			"HAVING",
			"IF", "IGNORE", "IMMEDIATE", "IN", "INDEX", "INDEXED", "INITIALLY", "INNER", "INSERT", "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL",
			"JOIN",
			"KEY",
			"LAST", "LEFT", "LIKE", "LIMIT",
			"MATCH", "MATERIALIZED",
			"NATURAL", "NO", "NOT", "NOTHING", "NOTNULL", "NULL", "NULLS",
			"OF", "OFFSET", "ON", "OR", "ORDER", "OTHERS", "OUTER", "OVER",
			"PARTITION", "PLAN", "PRAGMA", "PRECEDING", "PRIMARY",
			"QUERY",
			"RAISE", "RANGE", "RECURSIVE", "REFERENCES", "REGEXP", "REINDEX", "RELEASE", "RENAME", "REPLACE", "RESTRICT", "RETURNING", "RIGHT", "ROLLBACK", "ROW", "ROWS",
			"SAVEPOINT", "SELECT", "SET",
			"TABLE", "TEMP", "TEMPORARY", "THEN", "TIES", "TO", "TRANSACTION", "TRIGGER",
			"UNBOUNDED", "UNION", "UNIQUE", "UPDATE", "USING",
			"VACUUM", "VALUES", "VIEW", "VIRTUAL",
			"WHEN", "WHERE", "WINDOW", "WITH", "WITHOUT",
	};
	
	public static final SQLiteDatabaseSettings SQLITE_3_45 = new SQLiteDatabaseSettings();
	
	private SQLiteDatabaseSettings() {
		this(new SQLiteOperationsFactoriesBuilder(), new SQLiteParameterBinderRegistry());
	}
	
	private SQLiteDatabaseSettings(SQLiteOperationsFactoriesBuilder sqLiteOperationsFactoriesBuilder, SQLiteParameterBinderRegistry parameterBinderRegistry) {
		super(new SQLiteDatabaseSignet(3, 45),
				Collections.unmodifiableSet(new CaseInsensitiveSet(KEYWORDS)),
				'"',
				new SQLiteTypeMapping(),
				parameterBinderRegistry,
				sqLiteOperationsFactoriesBuilder,
				new SQLiteGeneratedKeysReaderFactory(),
				1000,
				false);
	}
	
	private static class SQLiteOperationsFactoriesBuilder implements SQLOperationsFactoriesBuilder {
		
		private final ReadOperationFactory readOperationFactory;
		private final SQLiteWriteOperationFactory writeOperationFactory;
		
		private SQLiteOperationsFactoriesBuilder() {
			this.readOperationFactory = new ReadOperationFactory();
			this.writeOperationFactory = new SQLiteWriteOperationFactory();
		}
		
		private ReadOperationFactory getReadOperationFactory() {
			return readOperationFactory;
		}
		
		private WriteOperationFactory getWriteOperationFactory() {
			return writeOperationFactory;
		}
		
		@Override
		public SQLOperationsFactories build(ParameterBinderIndex<Column, ParameterBinder> parameterBinders, DMLNameProviderFactory dmlNameProviderFactory, SqlTypeRegistry sqlTypeRegistry) {
			SQLiteDMLGenerator dmlGenerator = new SQLiteDMLGenerator(parameterBinders, DMLGenerator.NoopSorter.INSTANCE, dmlNameProviderFactory);
			SQLiteDDLTableGenerator ddlTableGenerator = new SQLiteDDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
			DDLSequenceGenerator ddlSequenceGenerator = new DDLSequenceGenerator(dmlNameProviderFactory);
			return new SQLOperationsFactories(writeOperationFactory, readOperationFactory, dmlGenerator, ddlTableGenerator, ddlSequenceGenerator,
					new SQLiteSequenceSelectorFactory(readOperationFactory, writeOperationFactory, dmlGenerator));
		}
	}
	
	public static class SQLiteWriteOperationFactory extends WriteOperationFactory {
		
		@Override
		protected <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator,
																	   ConnectionProvider connectionProvider,
																	   ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider,
																	   RowCountListener rowCountListener) {
			return new SQLiteWriteOperation<ParamType>(sqlGenerator, connectionProvider, rowCountListener) {
				@Override
				protected void prepareStatement(Connection connection) throws SQLException {
					this.preparedStatement = statementProvider.apply(connection, getSQL());
				}
			};
		}
	}
	
	/**
	 * Made package-private to be visible by {@link SQLiteGeneratedKeysReader}
	 * @param <ParamType>
	 */
	static class SQLiteWriteOperation<ParamType> extends WriteOperation<ParamType> {
		
		/** Updated row count of the last executed batch statement */
		private long updatedRowCount = 0;
		
		public SQLiteWriteOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider, RowCountListener rowCountListener) {
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
	}
	
	@VisibleForTesting
	static class SQLiteSequenceSelectorFactory implements DatabaseSequenceSelectorFactory {
		
		private final ReadOperationFactory readOperationFactory;
		private final WriteOperationFactory writeOperationFactory;
		private final DMLGenerator dmlGenerator;
		
		private SQLiteSequenceSelectorFactory(ReadOperationFactory readOperationFactory, WriteOperationFactory writeOperationFactory, DMLGenerator dmlGenerator) {
			this.dmlGenerator = dmlGenerator;
			this.readOperationFactory = readOperationFactory;
			this.writeOperationFactory = writeOperationFactory;
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
	static class SQLiteGeneratedKeysReaderFactory implements GeneratedKeysReaderFactory {
		
		@Override
		public <I> GeneratedKeysReader<I> build(String keyName, Class<I> columnType) {
			return (GeneratedKeysReader<I>) new SQLiteGeneratedKeysReader();
		}
	}
}
