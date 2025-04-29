package org.codefilarete.stalactite.sql.statement;

import java.sql.PreparedStatement;

import org.codefilarete.stalactite.sql.ConnectionProvider;

import static org.codefilarete.tool.bean.Objects.preventNull;

/**
 * As its name mention it, this class is a factory for {@link ReadOperation}, introduced to be overridden for database specific behavior
 *
 * @author Guillaume Mary
 */
public class ReadOperationFactory {

	/**
	 * The number of rows that should be fetched from the database when executing
	 * a {@link java.sql.ResultSet}. Acts as a performance optimization parameter
	 * when retrieving data, allowing control over the amount of data fetched
	 * per batch. This value may be overridden during the creation of
	 * {@link ReadOperation} instances.
	 *
	 * @see java.sql.ResultSet#setFetchSize(int)
	 */
	protected final Integer fetchSize;

	public ReadOperationFactory() {
		this(null);
	}
	
	public ReadOperationFactory(Integer fetchSize) {
		this.fetchSize = fetchSize;
	}

	/**
	 * Creates a new instance of {@link ReadOperation}
	 *
	 * @param sqlGenerator the object containing the SQL to execute
	 * @param connectionProvider the {@link java.sql.Connection} provider to ask for {@link java.sql.ResultSet} creation
	 * @param <ParamType> type of placeholders in the SQL (can be String, int, etc.) depending on given SQL
	 * @return a new instance of ReadOperation
	 * @see java.sql.Connection#prepareStatement(String)
	 * @see PreparedStatement#executeQuery()
	 * @see java.sql.ResultSet#setFetchSize(int)
	 */
	public <ParamType> ReadOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator,
															   ConnectionProvider connectionProvider) {
		return createInstance(sqlGenerator, connectionProvider, null);
	}

	/**
	 * Creates a new instance of {@link ReadOperation} with the given fetch size. Made to force the fetch size.
	 *
	 * @param sqlGenerator the object containing the SQL to execute
	 * @param connectionProvider the {@link java.sql.Connection} provider to ask for {@link java.sql.ResultSet} creation
	 * @param fetchSize the optional number of fetched rows by the {@link java.sql.ResultSet} (see {@link java.sql.ResultSet#setFetchSize(int)})
	 * @param <ParamType> type of placeholders in the SQL (can be String, int, etc.) depending on given SQL
	 * @return a new instance of ReadOperation
	 * @see java.sql.Connection#prepareStatement(String)
	 * @see PreparedStatement#executeQuery()
	 * @see java.sql.ResultSet#setFetchSize(int)
	 */
	public <ParamType> ReadOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator,
															   ConnectionProvider connectionProvider,
															   Integer fetchSize) {
		return new ReadOperation<>(sqlGenerator, connectionProvider, preventNull(fetchSize, this.fetchSize));
	}
}
