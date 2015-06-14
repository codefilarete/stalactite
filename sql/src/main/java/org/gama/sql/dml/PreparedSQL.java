package org.gama.sql.dml;

import org.gama.sql.binder.ParameterBinder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Class aimed at easing SQL prepared statement execution: simplification comes from {@link ParameterBinder}s because
 * you don't need to call the good setXXX() method.
 * {@link #prepareStatement(Connection)} must be called before {@link #applyValues()}
 * 
 * @see PreparedSQLForReading
 * @see PreparedSQLForWriting 
 * 
 * @author Guillaume Mary
 */
public abstract class PreparedSQL {
	
	private final String sql;
	
	private final Map<Integer, Object> values = new HashMap<>();
	
	private final Map<Integer, ParameterBinder> parameterBinders;
	
	protected PreparedStatement preparedStatement;
	
	public PreparedSQL(String sql, Map<Integer, ParameterBinder> parameterBinders) {
		this.sql = sql;
		this.parameterBinders = parameterBinders;
	}
	
	/**
	 * Build an internal {@link PreparedStatement} from the {@link Connection} given (if never called or connection
	 * has changed since last call)
	 * Must be called before {@link #applyValues()}
	 * 
	 * @param connection
	 * @throws SQLException
	 */
	public void prepareStatement(Connection connection) throws SQLException {
		if (this.preparedStatement == null || this.preparedStatement.getConnection() != connection) {
			this.preparedStatement = connection.prepareStatement(sql);
		}
	}
	
	public void set(Map<Integer, Object> values) {
		// NB: don't replace instance, putAll since Map can be cleared by clearValues()
		this.values.putAll(values);
	}
	
	public void set(Iterable<Entry<Integer, Object>> values) {
		for (Entry<Integer, Object> valueEntry : values) {
			set(valueEntry.getKey(), valueEntry.getValue());
		}
	}
	
	public void set(int index, Object value) {
		this.values.put(index, value);
	}
	
	public void clearValues() {
		this.values.clear();
	}
	
	/**
	 * Calls all setXXX {@link PreparedStatement} methods according to {@link ParameterBinder}s given in constructor.
	 * Hence {@link #prepareStatement(Connection)} must be called before.
	 * @throws SQLException
	 */
	public void applyValues() throws SQLException {
		for (Entry<Integer, Object> indexToValue : values.entrySet()) {
			Integer index = indexToValue.getKey();
			Object value = indexToValue.getValue();
			ParameterBinder paramBinder = parameterBinders.get(index);
			if (paramBinder == null) {
				throw new IllegalArgumentException("Can't find a "+ParameterBinder.class.getName() + " for index " + index + " of value " + value
						+ " on sql : " + sql);
			}
			paramBinder.set(index, value, preparedStatement);
		}
	}
}
