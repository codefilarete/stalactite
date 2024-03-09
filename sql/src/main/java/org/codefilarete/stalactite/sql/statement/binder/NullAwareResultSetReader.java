package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.codefilarete.tool.function.SerializableThrowingBiFunction;

/**
 * Wrapper around another {@link ResultSetReader} to handle null value
 * 
 * @author Guillaume Mary
 */
public class NullAwareResultSetReader<T> implements ResultSetReader<T> {
	
	private final ResultSetReader<T> surrogate;
	
	public NullAwareResultSetReader(SerializableThrowingBiFunction<ResultSet, String, T, SQLException> resultSetGetter) {
		this(ResultSetReader.ofMethodReference(resultSetGetter));
	}
	
	public NullAwareResultSetReader(ResultSetReader<T> surrogate) {
		this.surrogate = surrogate;
	}
	
	@Override
	public T doGet(ResultSet resultSet, String columnName) throws SQLException {
		if (!isNull(columnName, resultSet)) {
			return getNotNull(columnName, resultSet);
		} else {
			return null;
		}
	}
	
	@Override
	public Class<T> getType() {
		return surrogate.getType();
	}
	
	@Override
	public <O> Class<O> getColumnType() {
		return surrogate.getColumnType();
	}
	
	/**
	 * Says if a column is null.
	 *
	 * This implementation is done with getObject(columnName) == null. Not sure that all JDBC driver supports it. Official way is to use
	 * {@link ResultSet#wasNull()} but it needs column to be read before. Thought this is more expensive than testing getObject().
	 * To be changed/overridden according to JDBC driver support or performance observation. 
	 *
	 * @throws SQLException the possible error thrown by {@link ResultSet#getObject(String)}
	 */
	protected boolean isNull(String columnName, ResultSet resultSet) throws SQLException {
		return resultSet.getObject(columnName) == null;
	}
	
	public T getNotNull(String columnName, ResultSet resultSet) {
		return surrogate.get(resultSet, columnName);
	}
	
}
