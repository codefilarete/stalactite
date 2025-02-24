package org.codefilarete.stalactite.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.stream.LongStream;

import org.codefilarete.stalactite.mapping.id.sequence.SequenceStoredAsTableSelector;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SQLiteDDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.DMLGenerator.NoopSorter;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.WriteOperation.RowCountListener;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.SQLiteParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.SQLiteTypeMapping;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.function.ThrowingBiFunction;

import static org.codefilarete.tool.bean.Objects.preventNull;

/**
 * @author Guillaume Mary
 */
public class SQLiteDialect extends DefaultDialect {
	
	private final SQLiteSequenceSelectorFactory sequenceSelectorFactory = new SQLiteSequenceSelectorFactory();
	
	public SQLiteDialect() {
		super(new SQLiteTypeMapping(), new SQLiteParameterBinderRegistry());
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
	protected DMLGenerator newDmlGenerator() {
		return new SQLiteDMLGenerator(getColumnBinderRegistry(), NoopSorter.INSTANCE, getDmlNameProviderFactory());
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
	
	@Override
	public DatabaseSequenceSelectorFactory getDatabaseSequenceSelectorFactory() {
		return sequenceSelectorFactory;
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
	
	@VisibleForTesting
	class SQLiteSequenceSelectorFactory implements DatabaseSequenceSelectorFactory {

		@Override
		public org.codefilarete.tool.function.Sequence<Long> create(Sequence databaseSequence, ConnectionProvider connectionProvider) {
			return new SequenceStoredAsTableSelector(
					databaseSequence.getSchema(),
					databaseSequence.getName(),
					preventNull(databaseSequence.getInitialValue(), 1),
					preventNull(databaseSequence.getBatchSize(), 1),
					getDmlGenerator(),
					getReadOperationFactory(),
					getWriteOperationFactory(),
					connectionProvider);
		}
	}
}
