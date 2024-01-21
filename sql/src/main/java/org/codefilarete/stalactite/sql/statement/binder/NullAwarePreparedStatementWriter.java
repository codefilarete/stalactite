package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.codefilarete.tool.function.SerializableThrowingTriConsumer;

/**
 * Wrapper around another {@link PreparedStatementWriter} to handle null value
 * 
 * @author Guillaume Mary
 */
public class NullAwarePreparedStatementWriter<T> implements PreparedStatementWriter<T> {
	
	private final PreparedStatementWriter<T> surrogate;
	
	public NullAwarePreparedStatementWriter(SerializableThrowingTriConsumer<PreparedStatement, Integer, T, SQLException> preparedStatementSetter) {
		this(PreparedStatementWriter.ofMethodReference(preparedStatementSetter));
	}
	
	public NullAwarePreparedStatementWriter(PreparedStatementWriter<T> surrogate) {
		this.surrogate = surrogate;
	}
	
	@Override
	public void set(PreparedStatement statement, int valueIndex, T value) throws SQLException {
		if (value == null) {
			setNull(valueIndex, statement);
		} else {
			setNotNull(valueIndex, value, statement);
		}
	}
	
	@Override
	public Class<T> getType() {
		return surrogate.getType();
	}
	
	/**
	 * Special method for null values.
	 * This implementation is based on {@link PreparedStatement#setObject(int, Object)} with null as 2nd argument, but not all JDBC drivers
	 * support this way of doing and shall need usage of {@link PreparedStatement#setNull(int, int)}. So this method is here to be overridden
	 * according to support of setObject(int, null).
	 *
	 * @param valueIndex the index where to set null, first argument of setXXX(..)
	 * @param statement the {@link PreparedStatement} on which to set null
	 * @throws SQLException the possible error thrown by {@link PreparedStatement#setObject(int, Object)}
	 */
	protected void setNull(int valueIndex, PreparedStatement statement) throws SQLException {
		// could be : statement.setNull(valueIndex, statement.getParameterMetaData().getParameterType(valueIndex))
		statement.setObject(valueIndex, null);
	}
	
	public void setNotNull(int valueIndex, T value, PreparedStatement statement) throws SQLException {
		surrogate.set(statement, valueIndex, value);
	}
}
