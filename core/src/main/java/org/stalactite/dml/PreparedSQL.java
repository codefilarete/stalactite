package org.stalactite.dml;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.stalactite.persistence.engine.PersistenceContext;
import org.stalactite.persistence.sql.Dialect;
import org.stalactite.persistence.sql.dml.binder.ParameterBinder;

/**
 * @author mary
 */
public class PreparedSQL {
	
	private final String sql;
	
	private final Map<Integer, Object> values = new HashMap<>();
	
	private final Map<Integer, ParameterBinder> nullParameterBinders = new HashMap<>();

	public PreparedSQL(String sql) {
		this.sql = sql;
	}
	
	public void set(int index, Object o) {
		this.values.put(index, o);
	}
	
	public void setNull(int index, Class clazz) {
		set(index, null);
		PersistenceContext currentPersistenceContext = PersistenceContext.getCurrent();
		Dialect currentDialect = currentPersistenceContext.getDialect();
		nullParameterBinders.put(index, currentDialect.getParameterBinderRegistry().getBinder(clazz));
	}
	
	public int executeWrite() throws SQLException {
		PreparedStatement preparedStatement = prepareStatement();
		return preparedStatement.executeUpdate();
	}

	public ResultSet executeRead() throws SQLException {
		PreparedStatement preparedStatement = prepareStatement();
		return preparedStatement.executeQuery();
	}

	private PreparedStatement prepareStatement() throws SQLException {
		PersistenceContext currentPersistenceContext = PersistenceContext.getCurrent();
		PreparedStatement preparedStatement = currentPersistenceContext.getTransactionManager().getCurrentConnection().prepareStatement(sql);
		Dialect currentDialect = currentPersistenceContext.getDialect();
		for (Entry<Integer, Object> valueEntry : values.entrySet()) {
			applyValue(preparedStatement, currentDialect, valueEntry);
		}
		return preparedStatement;
	}

	private void applyValue(PreparedStatement preparedStatement, Dialect currentDialect, Entry<Integer, Object> valueEntry) throws SQLException {
		Object value = valueEntry.getValue();
		Integer index = valueEntry.getKey();
		ParameterBinder paramBinder = null;
		if (value == null) {
			paramBinder = nullParameterBinders.get(index);
			if (paramBinder == null) {
				throw new IllegalArgumentException("Can't set null value on unknown type, please use setNull(..) method");
			}
		} else {
			paramBinder = currentDialect.getParameterBinderRegistry().getBinder(value.getClass());
		}
		paramBinder.set(index, value, preparedStatement);
	}
}
