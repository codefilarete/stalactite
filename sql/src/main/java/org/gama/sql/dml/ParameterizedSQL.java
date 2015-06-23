package org.gama.sql.dml;

import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.dml.ExpandableSQL.ExpandableParameter;
import org.gama.sql.dml.SQLParameterParser.ParsedSQL;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Class that applies values to {@link PreparedStatement} according to SQL that contains named parameters.
 * Supports parameters expansion when passing {@link Iterable} as value of a parameter.
 * 
 * @author Guillaume Mary
 */
public class ParameterizedSQL extends SQLStatement<String> {
	
	private final ParsedSQL parsedSQL;
	private ExpandableSQL expandableSQL;
	
	public ParameterizedSQL(String originalSQL, Map<String, ParameterBinder> parameterBinders) {
		this(new SQLParameterParser(originalSQL).parse(), parameterBinders);
	}
	
	public ParameterizedSQL(ParsedSQL parsedSQL, Map<String, ParameterBinder> parameterBinders) {
		super(parameterBinders);
		this.parsedSQL = parsedSQL;
	}
	
	@Override
	public String getSQL() {
		ensureExpandableSQL(values);
		return expandableSQL.getPreparedSQL();
	}
	
	@Override
	public void applyValues(PreparedStatement statement) {
		if (!values.keySet().containsAll(indexes)) {
			HashSet<String> missingIndexes = new HashSet<>(indexes);
			missingIndexes.removeAll(values.keySet());
			throw new IllegalArgumentException("Missing value for parameters " + missingIndexes + " in " + values + " for \"" + getSQL() + "\"");
		}
		for (Entry<String, Object> indexToValue : values.entrySet()) {
			doApplyValue(indexToValue.getKey(), indexToValue.getValue(), statement);
		}
	}
	
	protected void doApplyValue(String parameterName, Object value, PreparedStatement statement) {
		ExpandableParameter expandableParameter = expandableSQL.getExpandableParameters().get(parameterName);
		ParameterBinder parameterBinder = parameterBinders.get(parameterName);
		if (parameterBinder == null) {
			throw new IllegalArgumentException("Can't find a "+ParameterBinder.class.getName() + " for name " + parameterName + " of value " + value
					+ " on sql : " + expandableSQL.getPreparedSQL());
		}
		int[] markIndexes = expandableParameter.getMarkIndexes();
		if (markIndexes.length == 1 && !(value instanceof Iterable)) {
			int index = markIndexes[0];
			doApplyValue(index, value, parameterBinder, statement);
		} else {
			int firstIndex = markIndexes[0];
			for (Object v : (Iterable) value) {
				doApplyValue(firstIndex++, v, parameterBinder, statement);
			}
		}
	}
	
	private void doApplyValue(int index, Object value, ParameterBinder parameterBinder, PreparedStatement statement) {
		try {
			parameterBinder.set(index, value, statement);
		} catch (SQLException e) {
			throw new RuntimeException("Error while setting value " + value + " for parameter " + index + " on statement " + getSQL(), e);
		}
	}
	
	protected void ensureExpandableSQL(Map<String, Object> values) {
		if (expandableSQL == null) {
			expandableSQL = new ExpandableSQL(this.parsedSQL, ExpandableSQL.sizes(values));
		}
	}
}
