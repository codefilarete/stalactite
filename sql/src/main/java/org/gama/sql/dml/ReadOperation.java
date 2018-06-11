package org.gama.sql.dml;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.gama.sql.ConnectionProvider;

/**
 * {@link SQLOperation} dedicated to Selects ... so these operations return a {@link ResultSet}.
 * 
 * @author Guillaume Mary
 */
public class ReadOperation<ParamType> extends SQLOperation<ParamType> {
	
	public ReadOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider) {
		super(sqlGenerator, connectionProvider);
	}
	
	/**
	 * Executes the statement, wraps {@link PreparedStatement#executeQuery()}
	 *
	 * @return the {@link ResultSet} from the database
	 */
	public ResultSet execute() {
		try {
			ensureStatement();
			this.sqlStatement.applyValues(preparedStatement);
			logExecution();
			applyTimeout();
			return this.preparedStatement.executeQuery();
		} catch (SQLException e) {
			throw new SQLExecutionException(getSQL(), e);
		}
	}
}
