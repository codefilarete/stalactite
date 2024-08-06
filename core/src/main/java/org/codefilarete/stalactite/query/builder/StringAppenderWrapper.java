package org.codefilarete.stalactite.query.builder;

import javax.annotation.Nullable;

import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.StringAppender;

/**
 * A basic appender to a {@link StringAppender}
 */
public class StringAppenderWrapper implements SQLAppender {
	
	private final StringAppender surrogate;
	private final DMLNameProvider dmlNameProvider;
	
	public StringAppenderWrapper(StringAppender stringAppender, DMLNameProvider dmlNameProvider) {
		surrogate = stringAppender;
		this.dmlNameProvider = dmlNameProvider;
	}
	
	@Override
	public StringAppenderWrapper cat(String s, String... ss) {
		surrogate.cat(s).cat(ss);
		return this;
	}
	
	@Override
	public <V> StringAppenderWrapper catValue(@Nullable Selectable<V> column, V value) {
		if (value instanceof CharSequence) {
			// specialized case to escape single quotes
			surrogate.cat("'", value.toString().replace("'", "''"), "'");
		} else if (value instanceof Column) {
			// Columns are simply appended (no binder needed nor index increment)
			surrogate.cat(dmlNameProvider.getName((Column) value));
		} else {
			surrogate.cat(value);
		}
		return this;
	}
	
	@Override
	public StringAppenderWrapper catValue(Object value) {
		if (value instanceof CharSequence) {
			// specialized case to escape single quotes
			surrogate.cat("'", value.toString().replace("'", "''"), "'");
		} else if (value instanceof Column) {
			// Columns are simply appended (no binder needed nor index increment)
			surrogate.cat(dmlNameProvider.getName((Column) value));
		} else {
			surrogate.cat(value);
		}
		return this;
	}
	
	@Override
	public StringAppenderWrapper catColumn(Column column) {
		// Columns are simply appended (no binder needed nor index increment)
		surrogate.cat(dmlNameProvider.getName(column));
		return this;
	}
	
	@Override
	public SQLAppender removeLastChars(int length) {
		surrogate.cutTail(length);
		return this;
	}
	
	@Override
	public String getSQL() {
		return surrogate.toString();
	}
}
