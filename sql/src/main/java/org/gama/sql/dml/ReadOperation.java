package org.gama.sql.dml;

import org.gama.sql.IConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@link SQLOperation} dedicated to Selectes ... so all operations that return a ResultSet
 *
 * @author Guillaume Mary
 */
public class ReadOperation<ParamType> extends SQLOperation<ParamType> {
	
	protected static final Logger LOGGER = LoggerFactory.getLogger(ReadOperation.class);
	
	public ReadOperation(SQLStatement<ParamType> sqlGenerator, IConnectionProvider connectionProvider) {
		super(sqlGenerator, connectionProvider);
	}
	
	/**
	 * Executed the statement, wraps {@link PreparedStatement#executeQuery()}
	 *
	 * @return the {@link ResultSet} from the database
	 * @throws SQLException
	 */
	public ResultSet execute() {
		try {
			ensureStatement();
			this.sqlStatement.applyValues(preparedStatement);
			LOGGER.debug(getSQL());
			return this.preparedStatement.executeQuery();
		} catch (SQLException e) {
			throw new RuntimeException("Error running \"" + getSQL() + "\"", e);
		}
	}
	
}
