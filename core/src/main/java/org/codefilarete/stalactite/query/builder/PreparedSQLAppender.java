package org.codefilarete.stalactite.query.builder;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Placeholder;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.query.model.Variable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.trace.MutableInt;

/**
 * An appender to a {@link PreparedSQL}
 */
public class PreparedSQLAppender implements SQLAppender {
	
	private final SQLAppender delegate;
	private final ColumnBinderRegistry parameterBinderRegistry;
	private final Map<Integer, ParameterBinder<?>> parameterBinders;
	private final Map<Integer, Object> values;
	private final MutableInt paramCounter;
	
	public PreparedSQLAppender(SQLAppender sqlAppender, ColumnBinderRegistry parameterBinderRegistry) {
		this(sqlAppender, parameterBinderRegistry, new HashMap<>(), new HashMap<>(), new MutableInt(1));
	}
	
	private PreparedSQLAppender(SQLAppender delegate,
								ColumnBinderRegistry parameterBinderRegistry,
								Map<Integer, ? extends ParameterBinder<?>> parameterBinders,
								Map<Integer, Object> values,
								MutableInt paramCounter) {
		this.delegate = delegate;
		this.parameterBinderRegistry = parameterBinderRegistry;
		this.parameterBinders = (Map<Integer, ParameterBinder<?>>) parameterBinders;
		this.values = values;
		this.paramCounter = paramCounter;
	}
	
	public Map<Integer, Object> getValues() {
		return values;
	}
	
	public Map<Integer, ParameterBinder<?>> getParameterBinders() {
		return parameterBinders;
	}
	
	@Override
	public PreparedSQLAppender cat(String s, String... ss) {
		delegate.cat(s, ss);
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
		} else if (value instanceof Placeholder) {
			parameterBinder = parameterBinderRegistry.getBinder(((Placeholder) value).getValueType());
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
			if (innerValue instanceof Column) {
				// Columns are simply appended (no binder needed nor index increment)
				delegate.catColumn((Column) innerValue);
			} else {
				appendPlaceholder(innerValue, binderSupplier);
			}
		} else {
			appendPlaceholder(value, binderSupplier);
		}
		return this;
	}
	
	private void appendPlaceholder(Object value, ParameterBinder<?> binderSupplier) {
		delegate.cat("?");
		values.put(paramCounter.getValue(), value);
		parameterBinders.put(paramCounter.getValue(), binderSupplier);
		paramCounter.increment();
	}
	
	@Override
	public PreparedSQLAppender catColumn(Selectable<?> column) {
		// Columns are simply appended (no binder needed nor index increment)
		delegate.catColumn(column);
		return this;
	}
	
	@Override
	public SQLAppender catTable(Fromable table) {
		delegate.catTable(table);
		return this;
	}
	
	@Override
	public SQLAppender removeLastChars(int length) {
		delegate.removeLastChars(length);
		return this;
	}
	
	@Override
	public String getSQL() {
		return delegate.getSQL();
	}
	
	@Override
	public SubSQLAppender newSubPart(DMLNameProvider dmlNameProvider) {
		SQLAppender self = PreparedSQLAppender.this;
		return new DefaultSubSQLAppender(new PreparedSQLAppender(
				this.delegate.newSubPart(dmlNameProvider),
				this.parameterBinderRegistry,
				this.parameterBinders,
				this.values,
				this.paramCounter)) {
			@Override
			public SQLAppender close() {
				// nothing special
				return self;
			}
		};
	}
}
