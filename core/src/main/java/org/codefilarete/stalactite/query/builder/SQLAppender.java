package org.codefilarete.stalactite.query.builder;

import javax.annotation.Nullable;

import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;

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
	 * Appends column name to underlying SQL statement. To be used when {@link #catValue(Selectable, Object)} can't be
	 * applied.
	 *
	 * @param column the column to be printed
	 * @return this
	 */
	SQLAppender catColumn(Column column);
	
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
}
