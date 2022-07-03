package org.codefilarete.stalactite.query.builder;

import org.codefilarete.stalactite.sql.ddl.structure.Column;

import javax.annotation.Nullable;

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
	 * @param column context of the value to be printed, can be null if operator doesn't
	 * @param value the object to be added/printed to the statement
	 * @return this
	 */
	SQLAppender catValue(@Nullable Column column, Object value);
	
	default SQLAppender catIf(boolean condition, String s) {
		if (condition) {
			cat(s);
		}
		return this;
	}
	
	String getSQL();
}
