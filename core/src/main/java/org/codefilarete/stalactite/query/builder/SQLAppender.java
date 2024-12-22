package org.codefilarete.stalactite.query.builder;

import javax.annotation.Nullable;

import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;

/**
 * The contract for printing a where clause : need to print a String and a value.
 * Then you can print a prepared statement or a valued statement.
 */
public interface SQLAppender {
	
	/**
	 * Appends a {@link String} to the underlying result. Used for keywords, column name, etc
	 *
	 * @param s a basic {@link String}
	 * @return this
	 */
	SQLAppender cat(String s, String... ss);
	
	/**
	 * Called when a value must be "printed" to the underlying result. Implementations will differ on this point depending on the target goal:
	 * values printed in the SQL statement (bad practice because of SQL injection) or prepared statement
	 *
	 * @param column context of the value to be printed
	 * @param value the object to be added/printed to the statement
	 * @return this
	 * @param <V> value type
	 */
	<V> SQLAppender catValue(@Nullable Selectable<V> column, Object value);
	
	/**
	 * Appends value to underlying statement. To be used when {@link #catValue(Selectable, Object)} can't be applied.
	 * 
	 * @param value the value to be printed
	 * @return this
	 */
	SQLAppender catValue(Object value);
	
	/**
	 * Appends column name to underlying SQL statement.
	 *
	 * @param column the column to be printed
	 * @return this
	 */
	SQLAppender catColumn(Selectable<?> column);
	
	SQLAppender catTable(Fromable table);
	
	SQLAppender removeLastChars(int length);
	
	default SQLAppender catIf(boolean condition, String s) {
		if (condition) {
			cat(s);
		}
		return this;
	}
	
	default boolean isEmpty() {
		return getSQL().isEmpty();
	}
	
	String getSQL();
	
	SubSQLAppender newSubPart(DMLNameProvider dmlNameProvider);
	
	interface SubSQLAppender extends SQLAppender {
		
		SubSQLAppender cat(String s, String... ss);
		
		<V> SubSQLAppender catValue(@Nullable Selectable<V> column, Object value);
		
		SubSQLAppender catValue(Object value);
		
		SubSQLAppender catColumn(Selectable<?> column);
		
		SubSQLAppender catTable(Fromable table);
		
		SubSQLAppender removeLastChars(int length);
		
		SQLAppender close();
	}
	
	abstract class DefaultSubSQLAppender implements SubSQLAppender {
		
		private final SQLAppender delegate;
		
		public DefaultSubSQLAppender(SQLAppender delegate) {
			this.delegate = delegate;
		}
		
		public SQLAppender getDelegate() {
			return delegate;
		}
		
		@Override
		public SubSQLAppender cat(String s, String... ss) {
			delegate.cat(s, ss);
			return this;
		}
		
		@Override
		public <V> SubSQLAppender catValue(@Nullable Selectable<V> column, Object value) {
			delegate.catValue(column, value);
			return this;
		}
		
		@Override
		public SubSQLAppender catValue(Object value) {
			delegate.catValue(value);
			return this;
		}
		
		@Override
		public SubSQLAppender catColumn(Selectable<?> column) {
			delegate.catColumn(column);
			return this;
		}
		
		@Override
		public SubSQLAppender catTable(Fromable table) {
			delegate.catTable(table);
			return this;
		}
		
		@Override
		public SubSQLAppender removeLastChars(int length) {
			delegate.removeLastChars(length);
			return this;
		}
		
		@Override
		public SubSQLAppender catIf(boolean condition, String s) {
			delegate.catIf(condition, s);
			return this;
		}
		
		@Override
		public boolean isEmpty() {
			return delegate.isEmpty();
		}
		
		@Override
		public String getSQL() {
			return delegate.getSQL();
		}
		
		@Override
		public SubSQLAppender newSubPart(DMLNameProvider dmlNameProvider) {
			return delegate.newSubPart(dmlNameProvider);
		}
	}
}
