package org.gama.sql.dml;

import org.gama.sql.binder.ParameterBinder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Class that applies values to {@link PreparedStatement} according to SQL that contains indexed parameters.
 * 
 * @author Guillaume Mary
 */
public class PreparedSQL extends SQLStatement<Integer> {
	
	private final String sql;
	
	public PreparedSQL(String sql, Map<Integer, ParameterBinder> parameterBinders) {
		super(parameterBinders);
		this.sql = sql;
	}
	
	@Override
	public String getSQL() {
		return sql;
	}
	
	protected void doApplyValue(Integer index, Object value, PreparedStatement statement) {
		ParameterBinder paramBinder = getParameterBinder(index);
		if (paramBinder == null) {
			throw new IllegalArgumentException("Can't find a "+ParameterBinder.class.getName() + " for index " + index + " of value " + value
					+ " on sql : " + getSQL());
		}
		try {
			paramBinder.set(index, value, statement);
		} catch (SQLException e) {
			throw new RuntimeException("Error while setting value " + value + " for parameter " + index + " on statement " + getSQL(), e);
		}
	}
}
