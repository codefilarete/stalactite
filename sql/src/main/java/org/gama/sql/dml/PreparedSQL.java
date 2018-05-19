package org.gama.sql.dml;

import java.sql.PreparedStatement;
import java.util.Map;

import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.PreparedStatementWriter;
import org.gama.sql.binder.PreparedStatementWriterIndex;

/**
 * Class that applies values to {@link PreparedStatement} according to SQL that contains indexed parameters.
 * 
 * @author Guillaume Mary
 */
public class PreparedSQL extends SQLStatement<Integer> {
	
	private final String sql;
	
	public PreparedSQL(String sql, Map<Integer, ? extends PreparedStatementWriter> parameterBinders) {
		super(parameterBinders);
		this.sql = sql;
	}
	
	public PreparedSQL(String sql, PreparedStatementWriterIndex<Integer, ? extends PreparedStatementWriter> parameterBinderProvider) {
		super(parameterBinderProvider);
		this.sql = sql;
	}
	
	@Override
	public String getSQL() {
		return sql;
	}
	
	protected void doApplyValue(Integer index, Object value, PreparedStatement statement) {
		PreparedStatementWriter<Object> paramBinder = getParameterBinder(index);
		if (paramBinder == null) {
			throw new BindingException("Can't find a "+ParameterBinder.class.getName() + " for index " + index + " of value " + value
					+ " on sql : " + getSQL());
		}
		doApplyValue(index, value, paramBinder, statement);
	}
}
