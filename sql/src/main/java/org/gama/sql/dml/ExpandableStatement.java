package org.gama.sql.dml;

import org.gama.sql.binder.ParameterBinder;

import java.sql.PreparedStatement;
import java.util.Map;

/**
 * Equivalent to {@link PreparedSQL} but with ParamType that can be expanded (essentially for Collection parameters)
 * 
 * @author Guillaume Mary
 */
public abstract class ExpandableStatement<ParamType> extends SQLStatement<ParamType> {
	
	private final String sql;
	
	public ExpandableStatement(String sql, Map<ParamType, ParameterBinder> parameterBinders) {
		super(parameterBinders);
		this.sql = sql;
	}
	
	@Override
	public String getSQL() {
		return sql;
	}
	
	@Override
	protected void doApplyValue(ParamType paramType, Object value, PreparedStatement statement) {
		ParameterBinder parameterBinder = getParameterBinder(paramType);
		if (parameterBinder == null) {
			throw new IllegalArgumentException("Can't find a "+ParameterBinder.class.getName() + " for parameter " + getParameterName(paramType) + " of value " + value
					+ " on sql : " + getSQL());
		}
		int[] markIndexes = getIndexes(paramType);
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
	
	protected abstract String getParameterName(ParamType parameter);
	
	protected abstract int[] getIndexes(ParamType paramType);
}
