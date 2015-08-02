package org.gama.sql.dml;

import org.gama.lang.Retryer;
import org.gama.lang.bean.IDelegateWithReturnAndThrows;
import org.gama.lang.exception.Exceptions;
import org.gama.sql.IConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * {@link SQLOperation} dedicated to Inserts, Updates, Deletes ... so all operations that return number of affected rows
 *
 * @author Guillaume Mary
 */
public class WriteOperation<ParamType> extends SQLOperation<ParamType> {
	
	protected static final Logger LOGGER = LoggerFactory.getLogger(WriteOperation.class);
	
	/** JDBC Batch statement count, for logging */
	private int batchedStatementCount = 0;
	
	/** Instance that helps to retry update statements on error, default is no {@link Retryer#NO_RETRY} */
	private final Retryer retryer;
	
	public WriteOperation(SQLStatement<ParamType> sqlGenerator, IConnectionProvider connectionProvider) {
		this(sqlGenerator, connectionProvider, Retryer.NO_RETRY);
	}
	
	public WriteOperation(SQLStatement<ParamType> sqlGenerator, IConnectionProvider connectionProvider, Retryer retryer) {
		super(sqlGenerator, connectionProvider);
		this.retryer = retryer;
	}
	
	/**
	 * Executes the statement, wraps {@link PreparedStatement#executeUpdate()}.
	 * To be used if you don't used {@link #addBatch(Map)}
	 *
	 * @return the same as {@link PreparedStatement#executeUpdate()}
	 */
	public int execute() {
		applyValuesToEnsuredStatement();
		return doExecuteUpdate();
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
			return computeUpdatedRowCount(doExecuteBatch());
		} finally {
			batchedStatementCount = 0;
		}
	}
	
	private int doExecuteUpdate() {
		LOGGER.debug(getSQL());
		return (int) doWithRetry(new IDelegateWithReturnAndThrows() {
			@Override
			public Object execute() throws Throwable {
				return preparedStatement.executeUpdate();
			}
		});
	}
	
	/**
	 * Tries to simplify values returned by {@link Statement#executeBatch()}: returns only one int instead of an array.
	 * Main purpose is to return the sum of all updated rows count, but it takes into account drivers that conform
	 * to the {@link Statement#executeBatch()} specifications: it returns {@link Statement#SUCCESS_NO_INFO} if one of
	 * the updates did, same for {@link Statement#EXECUTE_FAILED}.
	 * This operation is only here to make {@link #executeBatch()} looks like {@link #execute()}, added that I don't
	 * see interest of upper layer to have fine grained result as int[]. 
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
		return (int[]) doWithRetry(new IDelegateWithReturnAndThrows() {
			@Override
			public Object execute() throws Throwable {
				return preparedStatement.executeBatch();
			}
		});
	}
	
	private <T> T doWithRetry(IDelegateWithReturnAndThrows<T> delegateWithResult) {
		try {
			return retryer.execute(delegateWithResult, getSQL());
		} catch (Throwable t) {
			Exceptions.throwAsRuntimeException(t);
			return null; // unreachable
		}
	}
	
	/**
	 * Add values as a batched statement
	 *
	 * @param values values to be added as batch
	 */
	public void addBatch(Map<ParamType, Object> values) {
		// Necessary to call setValues() BEFORE ensureStatement() because in case of ParameterizedSQL statement is built
		// thanks to values (the expansion of parameters needs the values)
		setValues(values);
		applyValuesToEnsuredStatement();
		batchedStatementCount++;
		try {
			this.preparedStatement.addBatch();
		} catch (SQLException e) {
			Exceptions.throwAsRuntimeException(e);
		}
	}
	
	private void applyValuesToEnsuredStatement() {
		try {
			ensureStatement();
			this.sqlStatement.applyValues(preparedStatement);
		} catch (Throwable t) {
			throw new RuntimeException(getSQL() +" / " + this.sqlStatement.values, t);
		}
	}
}
