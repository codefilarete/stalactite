package org.gama.sql.dml;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.gama.sql.ConnectionProvider;
import org.gama.sql.result.ResultSetIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SQLOperation} dedicated to Selectes ... so all operations that return a ResultSet
 *
 * @author Guillaume Mary
 */
public class ReadOperation<ParamType> extends SQLOperation<ParamType> {
	
	protected static final Logger LOGGER = LoggerFactory.getLogger(ReadOperation.class);
	
	public ReadOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider) {
		super(sqlGenerator, connectionProvider);
	}
	
	/**
	 * Execute the statement, wraps {@link PreparedStatement#executeQuery()}
	 *
	 * @return the {@link ResultSet} from the database
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
	
	/**
	 * Execute the statement and return the transformed result by resultSetIterator
	 *
	 * @return the transformed result by resultSetIterator
	 */
	public <T> List<T> execute(ResultSetIterator<T> resultSetIterator) {
		resultSetIterator.setResultSet(execute());
		return resultSetIterator.convert();
	}
	
}
