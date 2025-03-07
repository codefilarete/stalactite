package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A class that uses {@link ResultSet#getObject(String, Class)} to read values from a query.
 * Made for cases when database vendor supports particular types only through getObject method.
 * 
 * @param <T>
 * @author Guillaume Mary
 * @see JdbcTypePreparedStatementWriter
 */
public class JdbcTypeResultSetReader<T> implements ResultSetReader<T> {
	
	private final Class<T> type;
	
	public JdbcTypeResultSetReader(Class<T> type) {
		this.type = type;
	}
	
	@Override
	public T doGet(ResultSet resultSet, String columnName) throws SQLException {
		return resultSet.getObject(columnName, type);
	}
	
	@Override
	public Class<T> getType() {
		return type;
	}
}
