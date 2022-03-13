package org.codefilarete.stalactite.sql.statement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.LongSupplier;

import org.codefilarete.stalactite.engine.StaleStateObjectException;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.tool.function.ThrowingBiFunction;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.statement.WriteOperation.RowCountListener;

/**
 * As its name mentions it, this class is a factory for {@link WriteOperation}, introduced to be overridden for database specific behavior.
 * 
 * @author Guillaume Mary
 * @see Dialect#newWriteOperationFactory()
 */
public class WriteOperationFactory {
	
	public static final RowCountListener NOOP_COUNT_CHECKER = new NoopRowCountListener();
	
	/**
	 * Delegates instance creation to {@link #createInstance(SQLStatement, ConnectionProvider, RowCountListener)} with {@link #NOOP_COUNT_CHECKER}
	 * 
	 * @param sqlGenerator the SQL order to be executed, expected to be an INSERT, UPDATE or DELETE one
	 * @param connectionProvider will provide {@link PreparedStatement} for SQL order execution
	 * @param <ParamType> type of parameter contained in given {@link SQLStatement}, therefore you'll get same type for created {@link WriteOperation}
	 * @return a new {@link WriteOperation} with no updated rows count check
	 */
	public <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider) {
		return createInstance(sqlGenerator, connectionProvider, NOOP_COUNT_CHECKER);
	}
	
	/**
	 * Delegates instance creation to {@link #createInstance(SQLStatement, ConnectionProvider, RowCountListener)} with a fixed row count checking.
	 * Invoked for single order execution, see {@link #createInstance(SQLStatement, ConnectionProvider, LongSupplier)} for batched ones.
	 * 
	 * @param sqlGenerator the SQL order to be executed, expected to be an INSERT, UPDATE or DELETE one
	 * @param connectionProvider will provide {@link PreparedStatement} for SQL order execution
	 * @param expectedRowCount expected count of rows to be updated by given SQL statement
	 * @param <ParamType> type of parameter contained in given {@link SQLStatement}, therefore you'll get same type for created {@link WriteOperation}
	 * @return a new {@link WriteOperation} with checking of updated row count to given one
	 */
	public <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider, long expectedRowCount) {
		return createInstance(sqlGenerator, connectionProvider, () -> expectedRowCount);
	}
	
	/**
	 * Delegates instance creation to {@link #createInstance(SQLStatement, ConnectionProvider, RowCountListener)} with a dynamic row count checking.
	 * Invoked for batched SQL statement, see {@link ExpectedBatchedRowCountsSupplier} for an implementation of batched row counter.
	 * 
	 * @param sqlGenerator the SQL order to be executed, expected to be an INSERT, UPDATE or DELETE one
	 * @param connectionProvider will provide {@link PreparedStatement} for SQL order execution
	 * @param expectedRowCount dynamic counter of expected count of rows to be updated by given SQL statement, will be queried for each batch execution
	 * @param <ParamType> type of parameter contained in given {@link SQLStatement}, therefore you'll get same type for created {@link WriteOperation}
	 * @return a new {@link WriteOperation} with checking of updated row count to given one
	 */
	public <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider, LongSupplier expectedRowCount) {
		return createInstance(sqlGenerator, connectionProvider, new JDBCRowCountChecker(expectedRowCount));
	}
	
	/**
	 * Instanciates a {@link WriteOperation} with a generic contract of updated row count, see {@link WriteOperation#WriteOperation(SQLStatement, ConnectionProvider, RowCountListener)}
	 * 
	 * @param sqlGenerator the SQL order to be executed, expected to be an INSERT, UPDATE or DELETE one
	 * @param connectionProvider will provide {@link PreparedStatement} for SQL order execution
	 * @param <ParamType> type of parameter contained in given {@link SQLStatement}, therefore you'll get same type for created {@link WriteOperation}
	 * @return a new {@link WriteOperation#WriteOperation(SQLStatement, ConnectionProvider, RowCountListener)}
	 */
	protected <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider, RowCountListener rowCountListener) {
		return createInstance(sqlGenerator, connectionProvider, Connection::prepareStatement, rowCountListener);
	}
	
	/**
	 * Equivalent to {@link #createInstance(SQLStatement, ConnectionProvider, long)} but for cases where {@link PreparedStatement} needs to be customized
	 * by one of the {@link Connection}.prepareStatement(..) methods
	 *
	 * @param sqlGenerator the SQL order to be executed, expected to be an INSERT, UPDATE or DELETE one
	 * @param connectionProvider will provide {@link Connection} of the {@link PreparedStatement} to be created
	 * @param statementProvider method that gives the {@link PreparedStatement} : its parameters will be the {@link Connection} given by connectionProvider and generated SQL from sqlGenerator,
	 * 		  will be queried for each batch execution
	 * @param expectedRowCount expected count of rows to be updated by given SQL statement
	 * @param <ParamType> type of parameter contained in given {@link SQLStatement}, therefore you'll get same type for created {@link WriteOperation}
	 * @return a new {@link WriteOperation} with checking of updated row count to given one
	 */
	public <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator,
																ConnectionProvider connectionProvider,
																ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider,
																long expectedRowCount) {
		return createInstance(sqlGenerator, connectionProvider, statementProvider, () -> expectedRowCount);
	}
	
	/**
	 * Equivalent to {@link #createInstance(SQLStatement, ConnectionProvider, LongSupplier)} but for cases where {@link PreparedStatement} needs 
	 * to be customized by one of the {@link Connection}.prepareStatement(..) methods
	 *
	 * @param sqlGenerator the SQL order to be executed, expected to be an INSERT, UPDATE or DELETE one
	 * @param connectionProvider will provide {@link Connection} of the {@link PreparedStatement} to be created
	 * @param statementProvider method that gives the {@link PreparedStatement} : its parameters will be {@link Connection} given by connectionProvider and generated SQL from sqlGenerator
	 * @param expectedRowCount dynamic counter of expected count of rows to be updated by given SQL statement
	 * @param <ParamType> type of parameter contained in given {@link SQLStatement}, therefore you'll get same type for created {@link WriteOperation}
	 * @return a new {@link WriteOperation} with checking of updated row count to given one
	 */
	public <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator,
																ConnectionProvider connectionProvider,
																ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider,
																LongSupplier expectedRowCount) {
		return createInstance(sqlGenerator, connectionProvider, statementProvider, new JDBCRowCountChecker(expectedRowCount));
	}
	
	protected <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator,
																   ConnectionProvider connectionProvider,
																   ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider,
																   RowCountListener rowCountListener) {
		return new WriteOperation<ParamType>(sqlGenerator, connectionProvider, rowCountListener) {
			@Override
			protected void prepareStatement(Connection connection) throws SQLException {
				this.preparedStatement = statementProvider.apply(connection, getSQL());
			}
		};
	}
	
	/**
	 * A {@link RowCountListener} that doesn't check updated row count
	 */
	protected static class NoopRowCountListener implements RowCountListener {
		
		protected NoopRowCountListener() {
		}
		
		@Override
		public void onRowCount(long writeCount) {
			
		}
	}
	
	/**
	 * A {@link RowCountListener} that complies with JDBC specification about returned values, and will throw a {@link StaleStateObjectException} if
	 * row count doesn't match given (at construction time) one, or if any #EXECUTE_FAILED happened. 
	 * 
	 * @see Statement#SUCCESS_NO_INFO
	 * @see Statement#EXECUTE_FAILED
	 */
	public static class JDBCRowCountChecker implements RowCountListener {
		
		private final LongSupplier expectedRowCount;
		
		public JDBCRowCountChecker(LongSupplier expectedRowCount) {
			this.expectedRowCount = expectedRowCount;
		}
		
		public JDBCRowCountChecker(long expectedRowCount) {
			this(() -> expectedRowCount);
		}
		
		@Override
		public void onRowCounts(long[] writeCounts) {
			long writeCount = 0;
			int successNoInfoCount = 0;
			int failureCount = 0;
			for (long count : writeCounts) {
				if (count == Statement.SUCCESS_NO_INFO) {
					successNoInfoCount++;
				} else if (count == Statement.EXECUTE_FAILED) {
					failureCount++;
				} else {
					writeCount += count;
				}
			}
			long expectedRowCount = this.expectedRowCount.getAsLong();
			if (successNoInfoCount != writeCounts.length && (failureCount != 0 || expectedRowCount != writeCount)) {
				throw new StaleStateObjectException(expectedRowCount, writeCount);
			} // else :
			// - all rows are SUCESS_NO_INFO
			// - or failureCount = 0 and expectedRowCount == writeCount
			// => nothing to do
		}
		
		@Override
		public void onRowCount(long writeCount) {
			if (expectedRowCount.getAsLong() != writeCount) {
				throw new StaleStateObjectException(expectedRowCount.getAsLong(), writeCount);
			}
		}
	}
	
	/**
	 * Provides expected count for updated rows. This implementation fits batched statement usage because it adapts its returned value according to
	 * packet number currently queried.
	 * Since this implementation as a state of currently queried packet, its instances shouldn't be shared between {@link WriteOperation}s
	 */
	public static class ExpectedBatchedRowCountsSupplier implements LongSupplier {
		
		private final int lastPacketNumber;
		private int packetNumber = 0;
		private final int lastPacketSize;
		private final int packetSize;
		
		public ExpectedBatchedRowCountsSupplier(int entityCount, int batchSize) {
			lastPacketNumber = entityCount / batchSize;
			packetSize = batchSize;
			lastPacketSize = entityCount % batchSize;
		}
		
		@Override
		public long getAsLong() {
			// since we're invoked for each packet, we adapt our answer according to packet number
			return packetNumber++ == lastPacketNumber ? lastPacketSize : packetSize;
		}
	}
	
}
