package org.stalactite.persistence.sql.dml;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

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
public class CRUDStatement {
	
	private final String sql;
	
	private PreparedStatement statement;
	
	/** Column indexes for written columns of inserts and updates */
	private Map<Column, Integer> upsertIndexes;
	
	/** Column indexes for where part of updates, deletes and selects */
	private Map<Column, Integer> whereIndexes;
	
	/**
	 * Constructor for inserts
	 * 
	 * @param upsertIndexes
	 * @param sql
	 */
	public CRUDStatement(Map<Column, Integer> upsertIndexes, String sql) {
		this(upsertIndexes, sql, null);
	}
	
	/**
	 * Constructor for selects and deletes
	 * 
	 * @param sql
	 * @param whereIndexes
	 */
	public CRUDStatement(String sql, Map<Column, Integer> whereIndexes) {
		this(null, sql, whereIndexes);
	}
	
	/**
	 * Constructor for updates
	 * @param upsertIndexes
	 * @param sql
	 * @param whereIndexes
	 */
	public CRUDStatement(Map<Column, Integer> upsertIndexes, String sql, Map<Column, Integer> whereIndexes) {
		this.upsertIndexes = upsertIndexes;
		this.sql = sql;
		this.whereIndexes = whereIndexes;
	}
	
	public String getSql() {
		return sql;
	}
	
	public PreparedStatement getStatement() {
		return statement;
	}
	
	public void setUpsertValue(@Nonnull Column column, Object value) throws SQLException {
		set(upsertIndexes, column, value);
	}
	
	public void setWhereValue(@Nonnull Column column, Object value) throws SQLException {
		set(whereIndexes, column, value);
	}
	
	private void set(Map<Column, Integer> colToIndexes, Column column, Object value) throws SQLException {
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
	public PreparedStatement apply(@Nonnull PersistentValues values, @Nonnull Connection connection) throws SQLException {
		prepare(connection);
		applyValues(values);
		return this.statement;
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
	public PreparedStatement apply(@Nonnull Iterable<PersistentValues> values, @Nonnull Connection connection) throws SQLException {
		prepare(connection);
		try {
			Iterables.visit(values, new ForEach<PersistentValues, Void>() {
				@Override
				public Void visit(PersistentValues values) {
					try {
						applyValues(values);
						CRUDStatement.this.statement.addBatch();
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
		return this.statement;
	}
	
	protected void applyValues(PersistentValues values) throws SQLException {
		for (Entry<Column, Object> colToValues : values.getUpsertValues().entrySet()) {
			setUpsertValue(colToValues.getKey(), colToValues.getValue());
		}
		for (Entry<Column, Object> colToValues : values.getWhereValues().entrySet()) {
			setWhereValue(colToValues.getKey(), colToValues.getValue());
		}
	}
	
	protected void prepare(Connection connection) throws SQLException {
		if (statement == null) {
			statement = connection.prepareStatement(sql);
		}
	}
}
