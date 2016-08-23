package org.gama.sql.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A {@link ParameterBinder} that manage null values. Done to prevent classes to do same tests many times.
 * 
 * Subclasses only need to override {@link #getNotNull(String, ResultSet)} and {@link #setNotNull(int, Object, PreparedStatement)} methods,
 * or eventually {@link #isNull(String, ResultSet)} and {@link #setNull(int, PreparedStatement)} if the implementation made by this class
 * is not supported by JDBC driver used.
 *
 * @author Guillaume Mary
 */
public abstract class AbstractParameterBinder<T> implements ParameterBinder<T> {
	
	@Override
	public T get(String columnName, ResultSet resultSet) throws SQLException {
		if (!isNull(columnName, resultSet)) {
			return getNotNull(columnName, resultSet);
		} else {
			return null;
		}
	}
	
	/**
	 * Say if a column is null.
	 * 
	 * This implementation is done with getObject(columnName) == null. Not sure that all JDBC driver support this. Official way is to use
	 * {@link ResultSet#wasNull()} but it needs column to be read before. Thought this is more expensive than testing getObject().
	 * To change/override according to JDBC driver support or performance observation. 
	 * 
	 * @throws SQLException
	 */
	protected boolean isNull(String columnName, ResultSet resultSet) throws SQLException {
		return resultSet.getObject(columnName) == null;
	}
	
	public abstract T getNotNull(String columnName, ResultSet resultSet) throws SQLException;
	
	@Override
	public void set(int valueIndex, T value, PreparedStatement statement) throws SQLException {
		if (value == null) {
			setNull(valueIndex, statement);
		} else {
			setNotNull(valueIndex, value, statement);
		}
	}
	
	/**
	 * Special method for null values.
	 * This implementation is based on {@link PreparedStatement#setObject(int, Object)} with null as 2nd argument, but not all JDBC drivers
	 * don't support this way of doing and need usage of {@link PreparedStatement#setNull(int, int)}. So, to be overriden according to support
	 * of setObject(int, null).
	 * 
	 * @param valueIndex the index where to set null, first argument of setXXX(..)
	 * @param statement the {@link PreparedStatement} on which to set null
	 * @throws SQLException the possible error thrown by {@link PreparedStatement}
	 */
	protected void setNull(int valueIndex, PreparedStatement statement) throws SQLException {
		statement.setObject(valueIndex, null);
	}
	
	public abstract void setNotNull(int valueIndex, T value, PreparedStatement statement) throws SQLException;
}
