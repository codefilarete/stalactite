package org.codefilarete.stalactite.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.stream.LongStream;

import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SQLiteDDLTableGenerator;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.DMLGenerator.NoopSorter;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.WriteOperation.RowCountListener;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.SQLiteTypeMapping;
import org.codefilarete.tool.function.ThrowingBiFunction;

/**
 * @author Guillaume Mary
 */
public class SQLiteDialect extends Dialect {

	public SQLiteDialect() {
		super(new SQLiteTypeMapping());
	}

	/**
	 * Overridden to return dedicated SQLite generated keys reader because SQLite as a special management
	 * <strong>Only supports Integer</strong>
	 */
	@Override
	public <I> GeneratedKeysReader<I> buildGeneratedKeysReader(String keyName, Class<I> columnType) {
		return (GeneratedKeysReader<I>) new SQLiteGeneratedKeysReader();
	}
	
	@Override
	protected DMLGenerator newDmlGenerator(ColumnBinderRegistry columnBinderRegistry) {
		return new SQLiteDMLGenerator(columnBinderRegistry, NoopSorter.INSTANCE, getDmlNameProviderFactory());
	}
	
	@Override
	protected DDLTableGenerator newDdlTableGenerator() {
		return new SQLiteDDLTableGenerator(getSqlTypeRegistry(), getDmlNameProviderFactory());
	}
	
	@Override
	protected ReadOperationFactory newReadOperationFactory() {
		return new SQLiteReadOperationFactory();
	}
	
	@Override
	protected WriteOperationFactory newWriteOperationFactory() {
		return new SQLiteWriteOperationFactory();
	}
	
	public static class SQLiteReadOperationFactory extends ReadOperationFactory {
		
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
}
