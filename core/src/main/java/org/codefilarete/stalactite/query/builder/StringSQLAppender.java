package org.codefilarete.stalactite.query.builder;

import javax.annotation.Nullable;

import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.StringAppender;

/**
 * A basic SQL appender to a {@link StringAppender}.
 * Values are rawly included in the final {@link StringAppender}, therefore this class must be use with
 * caution due to potential SQL Injection.
 */
public class StringSQLAppender implements SQLAppender {
	
	private final StringAppender delegate;
	private final DMLNameProvider dmlNameProvider;
	
	public StringSQLAppender(StringAppender stringAppender, DMLNameProvider dmlNameProvider) {
		delegate = stringAppender;
		this.dmlNameProvider = dmlNameProvider;
	}
	
	@Override
	public StringSQLAppender cat(String s, String... ss) {
		delegate.cat(s).cat(ss);
		return this;
	}
	
	@Override
	public <V> StringSQLAppender catValue(@Nullable Selectable<V> column, Object value) {
		return catValue(value);
	}
	
	@Override
	public StringSQLAppender catValue(Object value) {
		if (value instanceof ValuedVariable<?>) {
			value = ((ValuedVariable) value).getValue();
		}
		if (value instanceof CharSequence) {
			// specialized case to escape single quotes
			delegate.cat("'", value.toString().replace("'", "''"), "'");
		} else if (value instanceof Column) {
			// Columns are simply appended (no binder needed nor index increment)
			delegate.cat(dmlNameProvider.getName((Column) value));
		} else {
			delegate.cat(value);
		}
		return this;
	}
	
	@Override
	public StringSQLAppender catColumn(Column column) {
		// Columns are simply appended (no binder needed nor index increment)
		delegate.cat(dmlNameProvider.getName(column));
		return this;
	}
	
	@Override
	public SQLAppender removeLastChars(int length) {
		delegate.cutTail(length);
		return this;
	}
	
	@Override
	public String getSQL() {
		return delegate.toString();
	}
}
