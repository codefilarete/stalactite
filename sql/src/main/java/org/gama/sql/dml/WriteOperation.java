package org.gama.sql.dml;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.gama.lang.Retryer;
import org.gama.lang.Retryer.RetryException;
import org.gama.lang.bean.IDelegate;
import org.gama.lang.exception.Exceptions;
import org.gama.sql.IConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SQLOperation} dedicated to Inserts, Updates, Deletes ... so all operations that return number of affected rows
 *
 * @author Guillaume Mary
 */
public class WriteOperation<ParamType> extends SQLOperation<ParamType> {
	
	protected static final Logger LOGGER = LoggerFactory.getLogger(WriteOperation.class);
	
	/** Updated row count of the last executed batch statement */
	private int updatedRowCount = 0;
	
	/** JDBC Batch statement count, for logging */
	private int batchedStatementCount = 0;
	
	/** Instance that helps to retry update statements on error, default is no {@link Retryer#NO_RETRY}, should not be null */
	private final Retryer retryer;
	
	public WriteOperation(SQLStatement<ParamType> sqlGenerator, IConnectionProvider connectionProvider) {
		this(sqlGenerator, connectionProvider, Retryer.NO_RETRY);
	}
	
	public WriteOperation(SQLStatement<ParamType> sqlGenerator, IConnectionProvider connectionProvider, Retryer retryer) {
		super(sqlGenerator, connectionProvider);
		this.retryer = retryer;
	}
	
	public int getUpdatedRowCount() {
		return updatedRowCount;
	}
	
	/**
	 * Executes the statement, wraps {@link PreparedStatement#executeUpdate()}.
	 * To be used if you don't used {@link #addBatch(Map)}
	 *
	 * @return the same as {@link PreparedStatement#executeUpdate()}
	 */
	public int execute() {
		applyValuesToEnsuredStatement();
		return executeUpdate();
	}
	
	/**
	 * Executes the statement, wraps {@link PreparedStatement#executeBatch()}.
	 * To be used if you used {@link #addBatch(Map)}
	 *
	 * @return {@link Statement#SUCCESS_NO_INFO} or {@link Statement#EXECUTE_FAILED} if one {@link PreparedStatement#executeBatch()}
	 */
	public int executeBatch() {
		LOGGER.debug("Batching " + batchedStatementCount + " statements");
		try {
			updatedRowCount = computeUpdatedRowCount(doExecuteBatch());
			return updatedRowCount;
		} finally {
			batchedStatementCount = 0;
		}
	}
	
	private int executeUpdate() {
		LOGGER.debug(getSQL());
		try {
			return doWithRetry(this::doExecuteUpdate);
		} catch (SQLException | RetryException e) {
			throw new RuntimeException("Error during " + getSQL(), e);
		}
	}
	
	private int doExecuteUpdate() throws SQLException {
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
	protected int computeUpdatedRowCount(int[] updatedRowCounts) {
		int updatedRowCountSum = 0;
		for (int updatedRowCount : updatedRowCounts) {
			switch (updatedRowCount) {
				// first two cases are for drivers that conform to Statement.executeBatch specification
				case Statement.SUCCESS_NO_INFO:
					return Statement.SUCCESS_NO_INFO;
				case Statement.EXECUTE_FAILED:
					return Statement.EXECUTE_FAILED;
				default:	// 0 or really updated row count
					updatedRowCountSum += updatedRowCount;
			}
		}
		return updatedRowCountSum;
	} 
	
	private int[] doExecuteBatch() {
		LOGGER.debug(getSQL());
		try {
			return (int[]) doWithRetry((IDelegate<Object, SQLException>) () -> preparedStatement.executeBatch());
		} catch (SQLException | RetryException e) {
			throw new RuntimeException("Error during " + getSQL(), e);
		}
	}
	
	private <T, E extends Exception> T doWithRetry(IDelegate<T, E> delegateWithResult) throws E, RetryException {
		return retryer.execute(delegateWithResult, getSQL());
	}
	
	/**
	 * Add values as a batched statement
	 *
	 * @param values values to be added as batch
	 */
	public void addBatch(Map<ParamType, Object> values) {
		// Necessary to call setValues() BEFORE ensureStatement() because in case of StringParamedSQL statement is built
		// thanks to values (the expansion of parameters needs the values)
		setValues(values);
		applyValuesToEnsuredStatement();
		batchedStatementCount++;
		try {
			this.preparedStatement.addBatch();
		} catch (SQLException e) {
			throw Exceptions.asRuntimeException(e);
		}
	}
	
	private void applyValuesToEnsuredStatement() {
		try {
			ensureStatement();
			this.sqlStatement.applyValues(preparedStatement);
		} catch (Throwable t) {
			throw new RuntimeException("Error while applying values " + this.sqlStatement.values + " on statement " + getSQL(), t);
		}
	}
}
