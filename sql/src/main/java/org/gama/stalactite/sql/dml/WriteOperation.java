package org.gama.stalactite.sql.dml;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.gama.lang.exception.Exceptions;
import org.gama.lang.function.ThrowingExecutable;
import org.gama.stalactite.sql.ConnectionProvider;

/**
 * {@link SQLOperation} dedicated to Inserts, Updates, Deletes ... so theses operations return number of affected rows
 * 
 * @author Guillaume Mary
 */
public class WriteOperation<ParamType> extends SQLOperation<ParamType> {
	
	/** Updated row count of the last executed batch statement */
	private int updatedRowCount = 0;
	
	/** JDBC Batch statement count, for logging */
	private int batchedStatementCount = 0;
	
	/** Batched values, mainly for logging, filled when debug is required */
	private final Map<Integer /* batch count */, Map<ParamType, ?>> batchedValues = new HashMap<>();
	
	public WriteOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider) {
		super(sqlGenerator, connectionProvider);
	}
	
	public int getUpdatedRowCount() {
		return updatedRowCount;
	}
	
	/**
	 * Executes the statement, wraps {@link PreparedStatement#executeUpdate()}.
	 * To be used if you didn't use {@link #addBatch(Map)}
	 *
	 * @return the same as {@link PreparedStatement#executeUpdate()}
	 * @see #setValue(Object, Object)
	 * @see #setValues(Map)
	 */
	public int execute() {
		prepareExecute();
		return executeUpdate();
	}
	
	/**
	 * Executes the statement, wraps {@link PreparedStatement#executeBatch()}.
	 * To be used if you used {@link #addBatch(Map)}
	 *
	 * @return {@link Statement#SUCCESS_NO_INFO} or {@link Statement#EXECUTE_FAILED} if one {@link PreparedStatement#executeBatch()}
	 * @see #addBatch(Map)
	 */
	public int executeBatch() {
		LOGGER.debug("Batching statement {} times", batchedStatementCount);
		try {
			getListener().onExecute(getSqlStatement());
			int[] insertedRowCount = doWithLogManagement(this::doExecuteBatch);
			updatedRowCount = computeUpdatedRowCount(insertedRowCount);
			return updatedRowCount;
		} finally {
			batchedStatementCount = 0;
		}
	}
	
	protected int[] doExecuteBatch() throws SQLException {
		return preparedStatement.executeBatch();
	}
	
	protected int executeUpdate() {
		try {
			return this.doExecuteUpdate();
		} catch (SQLException e) {
			throw new SQLExecutionException(getSQL(), e);
		}
	}
	
	protected int doExecuteUpdate() throws SQLException {
		return preparedStatement.executeUpdate();
	}
	
	/**
	 * Tries to simplify values returned by {@link Statement#executeBatch()}: returns only one int instead of an array.
	 * Main purpose is to return the sum of all updated rows count, but it takes into account drivers that conform
	 * to the {@link Statement#executeBatch()} specifications: it returns {@link Statement#SUCCESS_NO_INFO} if one of
	 * the updates did, same for {@link Statement#EXECUTE_FAILED}.
	 * This operation is only here to make {@link #executeBatch()} looks like {@link #execute()}, added that I don't
	 * see interest for upper layer to have fine grained result as int[]. 
	 * 
	 * @return {@link Statement#SUCCESS_NO_INFO}, {@link Statement#EXECUTE_FAILED}, or the sum of all ints
	 */
	protected int computeUpdatedRowCount(int[] rowCounts) {
		int updatedRowCountSum = 0;
		for (int rowCount : rowCounts) {
			switch (rowCount) {
				// first two cases are for drivers that conform to Statement.executeBatch specification
				case Statement.SUCCESS_NO_INFO:
					return Statement.SUCCESS_NO_INFO;
				case Statement.EXECUTE_FAILED:
					return Statement.EXECUTE_FAILED;
				default:	// 0 or really updated row count
					updatedRowCountSum += rowCount;
			}
		}
		return updatedRowCountSum;
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
}
