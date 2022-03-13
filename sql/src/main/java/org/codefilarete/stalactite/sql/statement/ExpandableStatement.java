package org.codefilarete.stalactite.sql.statement;

import java.sql.PreparedStatement;
import java.util.Map;

import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriterIndex;

/**
 * Equivalent to {@link PreparedSQL} but with ParamType that can be expanded (essentially for Collection parameters)
 * 
 * @author Guillaume Mary
 */
public abstract class ExpandableStatement<ParamType> extends SQLStatement<ParamType> {
	
	private final String sql;
	
	public ExpandableStatement(String sql, Map<? extends ParamType, ? extends PreparedStatementWriter> parameterBinders) {
		super(parameterBinders);
		this.sql = sql;
	}
	
	public ExpandableStatement(String sql, PreparedStatementWriterIndex<? extends ParamType, ? extends PreparedStatementWriter> parameterBinderProvider) {
		super(parameterBinderProvider);
		this.sql = sql;
	}
	
	@Override
	public String getSQL() {
		return sql;
	}
	
	@Override
	protected void doApplyValue(ParamType paramType, Object value, PreparedStatement statement) {
		PreparedStatementWriter<Object> parameterBinder = getParameterBinder(paramType);
		if (parameterBinder == null) {
			throw new BindingException("Can't find binder for parameter \"" + getParameterName(paramType) + "\""
					+ " of type " + (value == null ? "unknown" : value.getClass().getName())
					+ " (value = " + value + ")"
					+ " on sql : " + getSQL());
		}
		int[] markIndexes = getIndexes(paramType);
		if (markIndexes.length > 1 && value instanceof Iterable) {
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
			// cases:
			// - simple case: one mark index and a single value, we loop on indexes
			// - multiple mark indexes and a single value => all indexes will have the same value
			// - one mark index and multiple values (List or any Iterable in fact) => binder should handle this case (ComplexTypeBinder for instance)
			for (int markIndex : markIndexes) {
				doApplyValue(markIndex, value, parameterBinder, statement);
			}
		}
	}
	
	protected abstract String getParameterName(ParamType parameter);
	
	protected abstract int[] getIndexes(ParamType paramType);
}
