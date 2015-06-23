package org.gama.sql.dml;

import org.gama.sql.IConnectionProvider;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@link SQLOperation} dedicated to Selectes ... so all operations that return a ResultSet
 *
 * @author Guillaume Mary
 */
public class ReadOperation<ParamType> extends SQLOperation<ParamType> {
	
	public ReadOperation(SQLStatement<ParamType> sqlGenerator, IConnectionProvider connectionProvider) {
		super(sqlGenerator, connectionProvider);
	}
	
	/**
	 * Executed the statement, wraps {@link PreparedStatement#executeQuery()}
	 *
	 * @return the {@link ResultSet} from the database
	 * @throws SQLException
	 */
	public ResultSet execute() throws SQLException {
		ensureStatement();
		this.sqlStatement.applyValues(preparedStatement);
		return this.preparedStatement.executeQuery();
	}
	
}
