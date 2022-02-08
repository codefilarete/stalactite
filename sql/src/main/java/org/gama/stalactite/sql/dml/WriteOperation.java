package org.gama.stalactite.sql.dml;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.tool.function.ThrowingExecutable;
import org.gama.stalactite.sql.ConnectionProvider;

/**
 * {@link SQLOperation} dedicated to Inserts, Updates, Deletes ... so theses operations return number of affected rows
 * 
 * @author Guillaume Mary
 */
public class WriteOperation<ParamType> extends SQLOperation<ParamType> {
	
	/** JDBC Batch statement count, for logging */
	private int batchedStatementCount = 0;
	
	/** Batched values, mainly for logging, filled when debug is required */
	private final Map<Integer /* batch count */, Map<ParamType, ?>> batchedValues = new HashMap<>();
	
	private final RowCountListener rowCountListener;
	
	public WriteOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider, RowCountListener rowCountListener) {
		super(sqlGenerator, connectionProvider);
		this.rowCountListener = rowCountListener;
	}
	
	/**
	 * Executes the statement, wraps {@link PreparedStatement#executeLargeUpdate()}.
	 * To be used if you used {@link #setValue(Object, Object)} or {@link #setValues(Map)}
	 *
	 * @see #setValue(Object, Object)
	 * @see #setValues(Map)
	 */
	public void execute() {
		prepareExecute();
		long writeCount;
		try {
			writeCount = doExecuteUpdate();
		} catch (SQLException e) {
			throw new SQLExecutionException(getSQL(), e);
		}
		rowCountListener.onRowCount(writeCount);
	}
	
	/**
	 * Executes the statement, wraps {@link PreparedStatement#executeLargeBatch()}.
	 * To be used if you used {@link #addBatch(Map)}
	 *
	 * @see #addBatch(Map)
	 */
	public void executeBatch() {
		LOGGER.debug("Batching statement {} times", batchedStatementCount);
		long[] updatedRowCounts;
		try {
			getListener().onExecute(getSqlStatement());
			updatedRowCounts = doWithLogManagement(this::doExecuteBatch);
		} finally {
			batchedStatementCount = 0;
		}
		rowCountListener.onRowCounts(updatedRowCounts);
	}
	
	protected long[] doExecuteBatch() throws SQLException {
		return preparedStatement.executeLargeBatch();
	}
	
	protected long doExecuteUpdate() throws SQLException {
		return preparedStatement.executeLargeUpdate();
	}
	
	private <T, E extends SQLException> T doWithLogManagement(ThrowingExecutable<T, E> delegateWithResult) {
		logExecution(() -> {
			HashMap<Integer, Map<ParamType, ?>> valuesClone = new HashMap<>(batchedValues);
			valuesClone.entrySet().forEach(e -> e.setValue(filterLoggable(e.getValue())));
			return valuesClone.toString();
		});
		try {
			return delegateWithResult.execute();
		} catch (SQLException e) {
			throw new SQLExecutionException(getSQL(), e);
		} finally {
			if (LOGGER.isTraceEnabled()) {
				batchedValues.clear();
			}
		}
	}
	
	/**
	 * Add values as a batched statement
	 *
	 * @param values values to be added as batch
	 * @see #executeBatch()
	 */
	public void addBatch(Map<ParamType, ?> values) {
		// Necessary to call setValues() BEFORE ensureStatement() because in case of StringParamedSQL statement is built
		// thanks to values (the expansion of parameters needs the values)
		setValues(values);
		applyValuesToEnsuredStatement();
		batchedStatementCount++;
		if (LOGGER.isTraceEnabled()) {
			// we log values only when debug needed to prevent memory consumption
			batchedValues.put(batchedStatementCount, values);
		}
		try {
			this.preparedStatement.addBatch();
		} catch (SQLException e) {
			throw Exceptions.asRuntimeException(e);
		}
	}
	
	/**
	 * Listener for updated row counts : its methods are called after statement execution, either for batched or not, with number of updated row counts.
	 */
	public interface RowCountListener {
		
		/**
		 * Method invoked after statement execution as batch.
		 * This implementation calls {@link #onRowCount(long)} for each value of the array. 
		 *
		 * @param writeCounts values returned by JDBC {@link PreparedStatement#executeLargeBatch()}
		 */
		default void onRowCounts(long[] writeCounts) {
			for (long writeCount : writeCounts) {
				onRowCount(writeCount);
			}
		}
		
		/**
		 * Method invoked after statement execution (not batched).
		 *
		 * @param writeCount value returned by JDBC {@link PreparedStatement#execute()}
		 */
		void onRowCount(long writeCount);
	}
}
