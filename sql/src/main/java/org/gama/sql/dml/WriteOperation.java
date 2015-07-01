package org.gama.sql.dml;

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
	 * @throws SQLException
	 */
	public int[] executeBatch() {
		LOGGER.debug("Batching " + batchRowCount + " rows");
		int[] updatedRowCount = doExecuteBatch();
		checkUpdatedRowCount(updatedRowCount);
		batchRowCount = 0;
		return updatedRowCount;
	}
	
	private int doExecuteUpdate() {
		Throwable t = null;
		do {
			LOGGER.debug(getSQL());
			try {
				return this.preparedStatement.executeUpdate();
			} catch (SQLException e) {
				t = e;
			}
		} while(isDeadlock(t));
		Exceptions.throwAsRuntimeException(t);
		return 0;	// unreachable
	}
	
	private int[] doExecuteBatch() {
		Throwable t = null;
		do {
			LOGGER.debug(getSQL());
			try {
				return this.preparedStatement.executeBatch();
			} catch (SQLException e) {
				t = e;
			}
		} while(isDeadlock(t));
		Exceptions.throwAsRuntimeException(t);
		return null;	// unreachable
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
