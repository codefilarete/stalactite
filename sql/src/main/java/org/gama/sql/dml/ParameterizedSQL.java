package org.gama.sql.dml;

import org.gama.sql.binder.CollectionBinder;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.dml.ExpandableSQL.ExpandableParameter;
import org.gama.sql.dml.SQLParameterParser.ParsedSQL;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Guillaume Mary
 */
public class ParameterizedSQL {
	
	private final String originalSQL;
	
	private final Map<String, Object> values = new HashMap<>();
	
	private final Map<String, ParameterBinder> parameterBinders;
	
	private PreparedStatement preparedStatement;
	
	private final ParsedSQL parsedSQL;
	private ExpandableSQL expandableSQL;
	
	public ParameterizedSQL(String originalSQL, Map<String, ParameterBinder> parameterBinders) {
		this.originalSQL = originalSQL;
		this.parameterBinders = parameterBinders;
		SQLParameterParser sqlParameterParser = new SQLParameterParser(this.originalSQL);
		this.parsedSQL = sqlParameterParser.parse();
	}
	
	public void clearValues() {
		this.values.clear();
	}
	
	public void set(String param, Object o) {
		this.values.put(param, o);
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
		expandableSQL = new ExpandableSQL(parsedSQL, values);
		if (this.preparedStatement == null || this.preparedStatement.getConnection() != connection) {
			this.preparedStatement = connection.prepareStatement(expandableSQL.getPreparedSQL());
		}
	}
	
	private void applyValues() throws SQLException {
		for (ExpandableParameter expandableParameter : expandableSQL.getExpandableParameters()) {
			int index = expandableParameter.getFirstIndex();
			String parameterName = expandableParameter.getParameterName();
			ParameterBinder parameterBinder = parameterBinders.get(parameterName);
			Object value = values.get(parameterName);
			if (parameterBinder == null) {
				throw new IllegalArgumentException("Can't find a "+ParameterBinder.class.getName() + " for index " + index + " of value " + value
						+ " on sql : " + expandableSQL.getPreparedSQL());
			}
			if (value instanceof Iterable) {
				parameterBinder = new CollectionBinder(parameterBinder);
			}
			applyValue(index, value, parameterBinder);
		}
	}
	
	private void applyValue(int index, Object value, ParameterBinder parameterBinder) throws SQLException {
		parameterBinder.set(index, value, preparedStatement);
	}
}
