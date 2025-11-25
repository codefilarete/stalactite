package org.codefilarete.stalactite.query.builder;

import javax.annotation.Nullable;

import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Placeholder;
import org.codefilarete.stalactite.query.model.QueryStatement.PseudoColumn;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.sql.ddl.DDLAppender;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.StringAppender;

/**
 * A basic SQL appender to a {@link StringAppender}.
 * Values are rawly included in the final {@link StringAppender}, therefore this class must be use with
 * caution due to potential SQL Injection.
 */
public class StringSQLAppender implements SQLAppender {
	
	private final DDLAppender delegate;
	
	public StringSQLAppender(DMLNameProvider dmlNameProvider) {
		this.delegate = new QualifiedNameDDLAppender(dmlNameProvider);
	}
	
	private StringSQLAppender(DDLAppender delegate) {
		this.delegate = delegate;
	}
	
	public DDLAppender getDelegate() {
		return delegate;
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
		} else {
			delegate.cat(value);
		}
		return this;
	}
	
	@Override
	public StringSQLAppender catColumn(Selectable<?> column) {
		// Columns are simply appended (no binder needed nor index increment)
		delegate.cat(column);
		return this;
	}
	
	@Override
	public SQLAppender catTable(Fromable table) {
		delegate.cat(table);
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
	
	@Override
	public SubSQLAppender newSubPart(DMLNameProvider dmlNameProvider) {
		return new DefaultSubSQLAppender(new StringSQLAppender(new QualifiedNameDDLAppender(this.delegate.getAppender(), dmlNameProvider))) {
			@Override
			public SQLAppender close() {
				// nothing special
				return StringSQLAppender.this;
			}
		};
	}
	
	private static class QualifiedNameDDLAppender extends DDLAppender {
		
		public QualifiedNameDDLAppender(DMLNameProvider dmlNameProvider) {
			super(dmlNameProvider);
		}
		
		public QualifiedNameDDLAppender(StringBuilder delegate, DMLNameProvider dmlNameProvider) {
			super(delegate, dmlNameProvider);
		}
		
		@Override
		public StringAppender cat(Object o) {
			if (o instanceof Column || o instanceof PseudoColumn) {
				return super.cat(dmlNameProvider.getName((Selectable<?>) o));
			} else if (o instanceof Selectable) {
				return super.cat(((Selectable<?>) o).getExpression());
			} else if (o instanceof Placeholder) {
				return super.cat(":" + ((Placeholder<?, ?>) o).getName());
			} else {
				return super.cat(o);
			}
		}
	}
}
