package org.gama.sql.dml;

import org.gama.sql.binder.ParameterBinder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author Guillaume Mary
 */
public class PreparedSQLForWriting extends PreparedSQL {
	
	public PreparedSQLForWriting(String sql, Map<Integer, ParameterBinder> parameterBinders) {
		super(sql, parameterBinders);
	}
	
	/**
	 * Executes the statement, wraps {@link PreparedStatement#executeUpdate()}.
	 * To be used if you don't used {@link #addBatch()}
	 * 
	 * @return the number of updated rows in database
	 * @throws SQLException
	 */
	public int executeUpdate() throws SQLException {
		int updatedRowCount = this.preparedStatement.executeUpdate();
		clearValues();
		return updatedRowCount;
	}
	
	/**
	 * Executes the statement, wraps {@link PreparedStatement#executeBatch()}.
	 * To be used if you used {@link #addBatch()}
	 * 
	 * @return the number of updated rows in database for each call to {@link #addBatch()}
	 * @throws SQLException
	 */
	public int[] execute() throws SQLException {
		int[] updatedRowCount = this.preparedStatement.executeBatch();
		clearValues();
		return updatedRowCount;
	}
	
	/**
	 * Add the current values to the statement as a batched one, wraps {@link PreparedStatement#addBatch()}
	 * 
	 * @throws SQLException
	 */
	public void addBatch() throws SQLException {
		this.preparedStatement.addBatch();
		clearValues();
	}
	
	/**
	 * Shortcut for {@link #set(Map)} + {@link #addBatch()}
	 * @param values
	 * @throws SQLException
	 */
	public void addBatch(Map<Integer, Object> values) throws SQLException {
		set(values);
		addBatch();
	}
	
}
