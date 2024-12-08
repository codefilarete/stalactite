package org.codefilarete.stalactite.query.builder;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.UnvaluedVariable;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.query.model.Variable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.trace.ModifiableInt;

/**
 * An appender to a {@link PreparedSQL}
 */
public class PreparedSQLAppender implements SQLAppender {
	
	private final SQLAppender surrogate;
	private final ColumnBinderRegistry parameterBinderRegistry;
	private final Map<Integer, ParameterBinder> parameterBinders;
	private final Map<Integer, Object> values;
	private final ModifiableInt paramCounter = new ModifiableInt(1);
	private final DMLNameProvider dmlNameProvider;
	
	public PreparedSQLAppender(SQLAppender sqlAppender, ColumnBinderRegistry parameterBinderRegistry, DMLNameProvider dmlNameProvider) {
		this.surrogate = sqlAppender;
		this.parameterBinderRegistry = parameterBinderRegistry;
		this.dmlNameProvider = dmlNameProvider;
		this.parameterBinders = new HashMap<>();
		this.values = new HashMap<>();
	}
	
	public Map<Integer, Object> getValues() {
		return values;
	}
	
	public Map<Integer, ParameterBinder> getParameterBinders() {
		return parameterBinders;
	}
	
	@Override
	public PreparedSQLAppender cat(String s, String... ss) {
		surrogate.cat(s, ss);
		return this;
	}
	
	/**
	 * Implemented such it adds the value as a {@link PreparedStatement} mark (?) and keeps it for future use in the value list.
	 *
	 * @param column
	 * @param value  the object to be added/printed to the statement
	 * @return this
	 * @param <V> value type
	 */
	@Override
	public <V> PreparedSQLAppender catValue(@Nullable Selectable<V> column, Object value) {
		ParameterBinder<?> parameterBinder;
		if (column == null) {
			parameterBinder = getParameterBinderFromRegistry(value);
		} else if (column instanceof Column) {
			parameterBinder = parameterBinderRegistry.getBinder((Column) column);
		} else {
			parameterBinder = parameterBinderRegistry.getBinder(column.getJavaType());
		}
		return catValue(value, parameterBinder);
	}
	
	@Override
	public PreparedSQLAppender catValue(Object value) {
		return catValue(value, getParameterBinderFromRegistry(value));
	}
	
	private ParameterBinder<?> getParameterBinderFromRegistry(Variable value) {
		ParameterBinder<?> parameterBinder = null;
		if (value instanceof ValuedVariable) {
			parameterBinder = getParameterBinderFromRegistry(((ValuedVariable) value).getValue());
		} else if (value instanceof UnvaluedVariable) {
			parameterBinder = parameterBinderRegistry.getBinder(((UnvaluedVariable) value).getValueType());
		}
		return parameterBinder;
	}	
	private ParameterBinder<?> getParameterBinderFromRegistry(Object value) {
		ParameterBinder<?> parameterBinder;
		if (value instanceof Variable) {
			parameterBinder = getParameterBinderFromRegistry(((Variable) value));
		} else {
			Class<?> binderType = value.getClass().isArray() ? value.getClass().getComponentType() : value.getClass();
			parameterBinder = parameterBinderRegistry.getBinder(binderType);
		}
		return parameterBinder;
	}
	
	private PreparedSQLAppender catValue(Object value, ParameterBinder<?> binderSupplier) {
		if (value instanceof ValuedVariable) {
			Object innerValue = ((ValuedVariable) value).getValue();
			if (innerValue instanceof Selectable) {
				// Columns are simply appended (no binder needed nor index increment)
				surrogate.cat(dmlNameProvider.getName((Selectable) innerValue));
			} else {
				appendPlaceholder(innerValue, binderSupplier);
			}
		} else {
			appendPlaceholder(value, binderSupplier);
		}
		return this;
	}
	
	private void appendPlaceholder(Object value, ParameterBinder<?> binderSupplier) {
		surrogate.cat("?");
		values.put(paramCounter.getValue(), value);
		parameterBinders.put(paramCounter.getValue(), binderSupplier);
		paramCounter.increment();
	}
	
	@Override
	public PreparedSQLAppender catColumn(Column column) {
		// Columns are simply appended (no binder needed nor index increment)
		surrogate.cat(dmlNameProvider.getName(column));
		return this;
	}
	
	@Override
	public SQLAppender removeLastChars(int length) {
		surrogate.removeLastChars(length);
		return this;
	}
	
	@Override
	public String getSQL() {
		return surrogate.getSQL();
	}
}
