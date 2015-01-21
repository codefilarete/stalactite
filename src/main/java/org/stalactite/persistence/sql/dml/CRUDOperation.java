package org.stalactite.persistence.sql.dml;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

import javax.annotation.Nonnull;

import org.stalactite.lang.collection.Arrays;
import org.stalactite.lang.collection.Iterables;
import org.stalactite.lang.collection.Iterables.ForEach;
import org.stalactite.lang.exception.Exceptions;
import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.structure.Table.Column;

/**
 * Class for basic CRUD operations, not for complex select with joins nor update with subselect
 * 
 * @author mary
 */
public abstract class CRUDOperation<R> {
	
	private final String sql;
	
	private PreparedStatement statement;
	
	/**
	 * 
	 * @param sql
	 */
	public CRUDOperation(String sql) {
		this.sql = sql;
	}
	
	public String getSql() {
		return sql;
	}
	
	public PreparedStatement getStatement() {
		return statement;
	}
	
	protected void set(Map<Column, Integer> colToIndexes, Column column, Object value) throws SQLException {
		Integer index = colToIndexes.get(column);
		if (index == null) {
			throw new IllegalArgumentException();
		} else {
			set(index, value);
		}
	}
	
	public void set(int valueIndex, Object value) throws SQLException {
		if (statement == null) {
			throw new IllegalStateException("Statement is not prepared, call prepare(Connection) before set(..)");
		}
		if (value == null) {
			statement.setObject(valueIndex, null);
			// TODO: à implémenter avec un parsing de la requête, cf AbstractQueryImpl#expandParameterList
//		} else if (value instanceof Collection) {
//			Collection listParam = (Collection) value;
//			statement.setParameterList(valueIndex, listParam);
		} else if (value instanceof Double) {
			Double doubleParam = (Double) value;
			statement.setDouble(valueIndex, doubleParam);
		} else if (value instanceof Float) {
			Float doubleParam = (Float) value;
			statement.setFloat(valueIndex, doubleParam);
		} else if (value instanceof Integer) {
			Integer integerParam = (Integer) value;
			statement.setInt(valueIndex, integerParam);
		} else if (value instanceof Long) {
			Long longParam = (Long) value;
			statement.setLong(valueIndex, longParam);
		} else if (value instanceof Date) {
			Date dateParam = (Date) value;
			statement.setTimestamp(valueIndex, new Timestamp(dateParam.getTime()));
		} else if (value instanceof String) {
			String strParam = (String) value;
			statement.setString(valueIndex, strParam);
		} else {
			throw new UnsupportedOperationException("Type "+value.getClass() + " is not implemented");
		}
	}
	
	/**
	 * Apply upsert and where values of PersistentValues to internal PreparedStatement created from Connection.
	 * Don't execute statement as we don't know if it's a select or not so return would be different.
	 * 
	 * @param values values to set to PreparedStatement
	 * @param connection a JDBC connection to create a PreparedStatement
	 * @return the created PreparedStatement from Connection
	 * @throws SQLException
	 */
	public void apply(@Nonnull PersistentValues values, @Nonnull Connection connection) throws SQLException {
		apply(Arrays.asList(values), connection);
	}
	
	/**
	 * Massive version of {@link #apply(PersistentValues, Connection)}. Dedicated to non-select statement since each
	 * PersistentValues is added as batch to the preparedStatement, so use {@link PreparedStatement#executeBatch()} afterward.
	 * 
	 * @param values values to set to PreparedStatement
	 * @param connection a JDBC connection to create a PreparedStatement
	 * @return the created PreparedStatement from Connection
	 * @throws SQLException
	 */
	public void apply(@Nonnull Iterable<PersistentValues> values, @Nonnull Connection connection) throws SQLException {
		prepare(connection);
		try {
			Iterables.visit(values, new ForEach<PersistentValues, Void>() {
				@Override
				public Void visit(PersistentValues values) {
					try {
						applyValues(values);
					} catch (SQLException e) {
						Exceptions.throwAsRuntimeException(e);
					}
					return null;
				}
			});
		} catch (RuntimeException e) {
			// rethrow SQLException that was thrown as RuntimeException in loop
			Throwable t = e.getCause();
			if (t instanceof SQLException) {
				throw (SQLException) t;
			} else {
				throw e;
			}
		}
	}
	
	protected abstract void applyValues(PersistentValues values) throws SQLException;
	
	protected abstract R execute() throws SQLException;
	
	protected void prepare(Connection connection) throws SQLException {
		if (statement == null) {
			statement = connection.prepareStatement(sql);
		}
	}
}
