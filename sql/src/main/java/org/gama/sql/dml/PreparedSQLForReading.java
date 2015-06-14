package org.gama.sql.dml;

import org.gama.sql.binder.ParameterBinder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author Guillaume Mary
 */
public class PreparedSQLForReading extends PreparedSQL {
	
	public PreparedSQLForReading(String sql, Map<Integer, ParameterBinder> parameterBinders) {
		super(sql, parameterBinders);
	}
	
	/**
	 * Executed the statement, wraps {@link PreparedStatement#executeQuery()}
	 * 
	 * @return the {@link ResultSet} from the database
	 * @throws SQLException
	 */
	public ResultSet executeRead() throws SQLException {
		ResultSet resultSet = this.preparedStatement.executeQuery();
		clearValues();
		return resultSet;
	}
	
}
