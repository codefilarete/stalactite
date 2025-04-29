package org.codefilarete.stalactite.sql.statement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.ConnectionProvider;

/**
 * {@link SQLOperation} dedicated to Selects ... so these operations return a {@link ResultSet}.
 * 
 * @author Guillaume Mary
 */
public class ReadOperation<ParamType> extends SQLOperation<ParamType> {

	/** Fetch size to be used for this operation */
	private final Integer fetchSize;

	/**
	 * Constructor with mandatory parameters
	 * @param sqlStatement the statement that must be executed by this operation
	 * @param connectionProvider JDBC {@link Connection} provider that will be used to get a connection to execute the statement on
	 */
	public ReadOperation(SQLStatement<ParamType> sqlStatement, ConnectionProvider connectionProvider) {
		this(sqlStatement, connectionProvider, null);
	}

	/**
	 * Constructor with mandatory parameters
	 * @param sqlStatement the statement that must be executed by this operation
	 * @param connectionProvider JDBC {@link Connection} provider that will be used to get a connection to execute the statement on
	 * @param fetchSize the optional fetch size to be used for this operation, pass null to use default value of {@link PreparedStatement#setFetchSize(int)}   
	 */
	public ReadOperation(SQLStatement<ParamType> sqlStatement, ConnectionProvider connectionProvider, Integer fetchSize) {
		super(sqlStatement, connectionProvider);
		this.fetchSize = fetchSize;
	}

	/**
	 * Overridden to set the fetch size if needed
	 */
	@Override
	protected void prepareStatement(Connection connection) throws SQLException {
		super.prepareStatement(connection);
		if (fetchSize != null) {
			this.preparedStatement.setFetchSize(fetchSize);
		}
	}
	
	/**
	 * Executes the statement, wraps {@link PreparedStatement#executeQuery()}
	 *
	 * @return the {@link ResultSet} from the database
	 */
	public ResultSet execute() {
		prepareExecute();
		try {
			return this.preparedStatement.executeQuery();
		} catch (SQLException e) {
			throw new SQLExecutionException(getSQL(), e);
		}
	}
}
