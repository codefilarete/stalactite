package org.codefilarete.stalactite.query.builder;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.trace.ModifiableInt;

/**
 * An appender to a {@link PreparedSQL}
 */
public class PreparedSQLWrapper implements SQLAppender {
	
	private final SQLAppender surrogate;
	private final ColumnBinderRegistry parameterBinderRegistry;
	private final Map<Integer, ParameterBinder> parameterBinders;
	private final Map<Integer, Object> values;
	private final ModifiableInt paramCounter = new ModifiableInt(1);
	private final DMLNameProvider dmlNameProvider;
	
	public PreparedSQLWrapper(SQLAppender sqlAppender, ColumnBinderRegistry parameterBinderRegistry, DMLNameProvider dmlNameProvider) {
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
	public PreparedSQLWrapper cat(String s, String... ss) {
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
	public <V> PreparedSQLWrapper catValue(@Nullable Selectable<V> column, V value) {
		return catValue(value, () -> {
			if (column == null) {
				return getParameterBinderFromRegistry(value);
			} else if (column instanceof Column) {
				return parameterBinderRegistry.getBinder((Column) column);
			} else {
				return parameterBinderRegistry.getBinder(column.getJavaType());
			}
		});
	}
	
	@Override
	public PreparedSQLWrapper catValue(Object value) {
		return catValue(value, () -> getParameterBinderFromRegistry(value));
	}
	
	private ParameterBinder<?> getParameterBinderFromRegistry(Object value) {
		Class<?> binderType = value.getClass().isArray() ? value.getClass().getComponentType() : value.getClass();
		return parameterBinderRegistry.getBinder(binderType);
	}
	
	private PreparedSQLWrapper catValue(Object value, Supplier<ParameterBinder<?>> binderSupplier) {
		if (value instanceof Selectable) {
			// Columns are simply appended (no binder needed nor index increment)
			surrogate.cat(dmlNameProvider.getName((Selectable) value));
		} else {
			appendParameter(value, binderSupplier);
		}
		return this;
	}
	
	private void appendParameter(Object value, Supplier<ParameterBinder<?>> binderSupplier) {
		surrogate.cat("?");
		values.put(paramCounter.getValue(), value);
		parameterBinders.put(paramCounter.getValue(), binderSupplier.get());
		paramCounter.increment();
	}
	
	@Override
	public PreparedSQLWrapper catColumn(Column column) {
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
