package org.gama.sql.dml;

import org.gama.sql.IConnectionProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Tries to simplify use of {@link PreparedStatement} in oriented scenarii like:
 * - set values on {@link PreparedStatement}
 * - executeBatch {@link PreparedStatement}
 * 
 * @see WriteOperation
 * @see ReadOperation
 * @param <ParamType> is type of sqlStatement value entries
 * 
 * @author Guillaume Mary
 */
public abstract class SQLOperation<ParamType> implements AutoCloseable {
	
	protected final IConnectionProvider connectionProvider;
	
	protected PreparedStatement preparedStatement;
	
	protected final SQLStatement<ParamType> sqlStatement;
	
	private String sql;
	
	public SQLOperation(SQLStatement<ParamType> sqlStatement, IConnectionProvider connectionProvider) {
		this.sqlStatement = sqlStatement;
		this.connectionProvider = connectionProvider;
	}
	
	/**
	 * Simple wrap to {@link SQLStatement#setValues(Map)}
	 * @param values
	 */
	public void setValues(Map<ParamType, Object> values) {
		// we transfert data to our own structure
		this.sqlStatement.setValues(values);
	}
	
	public void setValue(ParamType index, Object value) {
		this.sqlStatement.setValue(index, value);
	}
	
	/**
	 * Common operation for subclasses. Rebuild PreparedStatement if connection changed. Call {@link #getSQL()} when
	 * necessary.
	 * 
	 * @throws SQLException
	 */
	protected void ensureStatement() throws SQLException {
		Connection connection = this.connectionProvider.getConnection();
		if (this.preparedStatement == null || this.preparedStatement.getConnection() != connection) {
			this.preparedStatement = connection.prepareStatement(getSQL());
		}
	}
	
	/**
	 * Gives the SQL that is used in the {@link PreparedStatement}.
	 * Called by {@link SQLStatement#getSQL()} and store the result.
	 * 
	 * @return the SQL that is used in the {@link PreparedStatement}
	 */
	protected String getSQL() {
		if (this.sql == null) {
			this.sql = sqlStatement.getSQL();
		}
		return this.sql;
	}
	
	/**
	 * Closes internal {@link PreparedStatement}
	 * 
	 * @throws Exception
	 */
	@Override
	public void close() throws Exception {
		this.preparedStatement.close();
	}
}
