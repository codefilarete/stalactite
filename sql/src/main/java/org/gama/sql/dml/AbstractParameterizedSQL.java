package org.gama.sql.dml;

import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.dml.ExpandableSQL.ExpandableParameter;
import org.gama.sql.dml.SQLParameterParser.ParsedSQL;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Class that applies values to {@link PreparedStatement} according to SQL that contains named parameters.
 * Supports parameters expansion when passing {@link Iterable} as value of a parameter.
 * 
 * @author Guillaume Mary
 */
public abstract class AbstractParameterizedSQL<ParamType> extends SQLStatement<ParamType> {
	
	private final ParsedSQL parsedSQL;
	private ExpandableSQL expandableSQL;
	
	public AbstractParameterizedSQL(String originalSQL, Map<ParamType, ParameterBinder> parameterBinders) {
		this(new SQLParameterParser(originalSQL).parse(), parameterBinders);
	}
	
	public AbstractParameterizedSQL(ParsedSQL parsedSQL, Map<ParamType, ParameterBinder> parameterBinders) {
		super(parameterBinders);
		this.parsedSQL = parsedSQL;
	}
	
	@Override
	public String getSQL() {
		ensureExpandableSQL(values);
		return expandableSQL.getPreparedSQL();
	}
	
	protected void doApplyValue(ParamType parameter, Object value, PreparedStatement statement) {
		ParameterBinder parameterBinder = getParameterBinder(parameter);
		if (parameterBinder == null) {
			throw new IllegalArgumentException("Can't find a "+ParameterBinder.class.getName() + " for name " + parameter + " of value " + value
					+ " on sql : " + expandableSQL.getPreparedSQL());
		}
		ExpandableParameter expandableParameter = expandableSQL.getExpandableParameters().get(getParameterName(parameter));
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
	
	protected void ensureExpandableSQL(Map<ParamType, Object> values) {
		if (expandableSQL == null) {
			expandableSQL = new ExpandableSQL(this.parsedSQL, getValuesSizes(values));
		}
	}
	
	protected abstract Map<String, Integer> getValuesSizes(Map<ParamType, Object> values);
	
	protected abstract String getParameterName(ParamType parameter);
}
