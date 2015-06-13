package org.gama.sql.dml;

import org.gama.sql.binder.ParameterBinder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Guillaume Mary
 */
public class PreparedSQL {
	
	private final String sql;
	
	private final Map<Integer, Object> values = new HashMap<>();
	
	private final Map<Integer, ParameterBinder> parameterBinders;
	
	private PreparedStatement preparedStatement;
	
	public PreparedSQL(String sql, Map<Integer, ParameterBinder> parameterBinders) {
		this.sql = sql;
		this.parameterBinders = parameterBinders;
	}
	
	public void clearValues() {
		this.values.clear();
	}
	
	public void set(int index, Object o) {
		this.values.put(index, o);
	}
	
	public int executeWrite(Connection connection) throws SQLException {
		prepareStatement(connection);
		applyValues();
		return this.preparedStatement.executeUpdate();
	}
	
	public ResultSet executeRead(Connection connection) throws SQLException {
		prepareStatement(connection);
		applyValues();
		return this.preparedStatement.executeQuery();
	}
	
	private void prepareStatement(Connection connection) throws SQLException {
		if (this.preparedStatement == null || this.preparedStatement.getConnection() != connection) {
			this.preparedStatement = connection.prepareStatement(sql);
		}
	}
	
	private void applyValues() throws SQLException {
		for (Entry<Integer, Object> valueEntry : values.entrySet()) {
			applyValue(valueEntry);
		}
	}

	private void applyValue(Entry<Integer, Object> valueEntry) throws SQLException {
		Object value = valueEntry.getValue();
		Integer index = valueEntry.getKey();
		ParameterBinder paramBinder = parameterBinders.get(index);
		if (paramBinder == null) {
			throw new IllegalArgumentException("Can't find a "+ParameterBinder.class.getName() + " for index " + index + " of value " + value
			+ " on sql : " + sql);
		}
		paramBinder.set(index, value, preparedStatement);
	}
}
