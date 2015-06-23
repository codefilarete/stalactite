package org.gama.sql.dml;

import org.gama.sql.IConnectionProvider;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * {@link SQLOperation} dedicated to Inserts, Updates, Deletes ... so all operations that return number of affected rows
 * 
 * @author Guillaume Mary
 */
public class WriteOperation<ParamType> extends SQLOperation<ParamType> {
	
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
	public int execute() throws SQLException {
		ensureStatement();
		this.sqlStatement.applyValues(preparedStatement);
		return this.preparedStatement.executeUpdate();
	}
	
	/**
	 * Executes the statement, wraps {@link PreparedStatement#executeBatch()}.
	 * To be used if you used {@link #addBatch(Map)}
	 *
	 * @return the number of updated rows in database for each call to {@link #addBatch(Map)}
	 * @throws SQLException
	 */
	public int[] executeBatch() throws SQLException {
		ensureStatement();
		this.sqlStatement.applyValues(preparedStatement);
		return this.preparedStatement.executeBatch();
	}
	
	/**
	 * Shortcut for {@link #setValues(Map)} + {@link #addBatch(Map)}
	 * @param values
	 * @throws SQLException
	 */
	public void addBatch(Map<ParamType, Object> values) throws SQLException {
		// Necessary to call setValues() BEFORE ensureStatement() because in case of ParameterizedSQL statement is built
		// thanks to values (the expansion of parameters needs the values)
		setValues(values);
		ensureStatement();
		this.sqlStatement.applyValues(preparedStatement);
		this.preparedStatement.addBatch();
	}
}
