package org.gama.sql.dml;

import org.gama.lang.Retrier;
import org.gama.lang.bean.IDelegateWithReturnAndThrows;
import org.gama.lang.exception.Exceptions;
import org.gama.lang.exception.MultiCauseException;
import org.gama.sql.IConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * {@link SQLOperation} dedicated to Inserts, Updates, Deletes ... so all operations that return number of affected rows
 * 
 * @author Guillaume Mary
 */
public class WriteOperation<ParamType> extends SQLOperation<ParamType> {
	
	protected static final Logger LOGGER = LoggerFactory.getLogger(WriteOperation.class);
	
	/** JDBC Batch row count, for logging */
	private int batchRowCount = 0;
	
	public WriteOperation(SQLStatement<ParamType> sqlGenerator, IConnectionProvider connectionProvider) {
		super(sqlGenerator, connectionProvider);
	}
	
	/**
	 * Executes the statement, wraps {@link PreparedStatement#executeUpdate()}.
	 * To be used if you don't used {@link #addBatch(Map)}
	 *
	 * @return the number of updated rows in database
	 * @throws SQLException
	 */
	public int execute() {
		applyValuesToEnsuredStatement();
		return doExecuteUpdate();
	}
	
	/**
	 * Executes the statement, wraps {@link PreparedStatement#executeBatch()}.
	 * To be used if you used {@link #addBatch(Map)}
	 *
	 * @return the number of updated rows in database for each call to {@link #addBatch(Map)}
	 */
	public int[] executeBatch() {
		LOGGER.debug("Batching " + batchRowCount + " rows");
		int[] updatedRowCount = doExecuteBatch();
		checkUpdatedRowCount(updatedRowCount);
		batchRowCount = 0;
		return updatedRowCount;
	}
	
	private int doExecuteUpdate() {
		LOGGER.debug(getSQL());
		return (int) doWithRetry(new IDelegateWithReturnAndThrows() {
			@Override
			public Object execute() throws Throwable {
				return preparedStatement.executeUpdate();
			}
		}, 3, 5);
	}
	
	private int[] doExecuteBatch() {
		LOGGER.debug(getSQL());
		return (int[]) doWithRetry(new IDelegateWithReturnAndThrows() {
			@Override
			public Object execute() throws Throwable {
				return preparedStatement.executeBatch();
			}
		}, 3, 5);
	}

	private <T> T doWithRetry(IDelegateWithReturnAndThrows<T> delegateWithResult, int retryMaxCount, int retryDelay) {
		Retrier testInstance = new Retrier(retryMaxCount, retryDelay) {
			@Override
			protected boolean shouldRetry(Throwable t) {
				return isDeadlock(t);
			}
		};
		try {
			return testInstance.execute(delegateWithResult, getSQL());
		} catch (Throwable t) {
			Exceptions.throwAsRuntimeException(t);
			return null; // unreachable
		}
	}

	private boolean isDeadlock(Throwable t) {
		boolean isDeadlock = t != null && t.getMessage() != null && t.getMessage().contains("Deadlock");
		if (!isDeadlock) {
			Exceptions.throwAsRuntimeException(t);
		}
		return isDeadlock;
	}
	
	protected void checkUpdatedRowCount(int[] updatedRowCount) {
		MultiCauseException exception = new MultiCauseException();
		for (int rowCount : updatedRowCount) {
			if (rowCount == 0) {
				exception.addCause(new IllegalStateException("No row updated for " + getSQL()));
			}
		}
		exception.throwIfNotEmpty();
	}
	
	/**
	 * Shortcut for {@link #setValues(Map)} + {@link #addBatch(Map)}
	 * @param values
	 * @throws SQLException
	 */
	public void addBatch(Map<ParamType, Object> values) {
		// Necessary to call setValues() BEFORE ensureStatement() because in case of ParameterizedSQL statement is built
		// thanks to values (the expansion of parameters needs the values)
		setValues(values);
		applyValuesToEnsuredStatement();
		batchRowCount++;
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
		} catch (SQLException e) {
			Exceptions.throwAsRuntimeException(e);
		}
	}
}
