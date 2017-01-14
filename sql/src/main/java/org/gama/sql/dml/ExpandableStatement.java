package org.gama.sql.dml;

import java.sql.PreparedStatement;
import java.util.Map;

import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.ParameterBinderIndex;

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
	
	public ExpandableStatement(String sql, ParameterBinderIndex<ParamType> parameterBinderProvider) {
		super(parameterBinderProvider);
		this.sql = sql;
	}
	
	@Override
	public String getSQL() {
		return sql;
	}
	
	@Override
	protected void doApplyValue(ParamType paramType, Object value, PreparedStatement statement) {
		ParameterBinder<Object> parameterBinder = getParameterBinder(paramType);
		if (parameterBinder == null) {
			throw new IllegalArgumentException("Can't find binder for parameter " + getParameterName(paramType)
					+ " of type " + (value == null ? "unkown" : value.getClass().getName())
					+ " (value = " + value + ")"
					+ " on sql : " + getSQL());
		}
		int[] markIndexes = getIndexes(paramType);
		if (value instanceof Iterable) {
			// we have several mark indexes : one per value, and one per parameter in query ("id = :id or id = :id)
			// so we loop twice
			for (int i = 0; i < markIndexes.length;) {
				for (Object v : (Iterable) value) {
					int markIndex = markIndexes[i];
					doApplyValue(markIndex, v, parameterBinder, statement);
					i++;
				}
			}
		} else {
			// simple case: value is single, we loop on indexes
			for (int markIndex : markIndexes) {
				doApplyValue(markIndex, value, parameterBinder, statement);
			}
		}
	}
	
	protected abstract String getParameterName(ParamType parameter);
	
	protected abstract int[] getIndexes(ParamType paramType);
}
