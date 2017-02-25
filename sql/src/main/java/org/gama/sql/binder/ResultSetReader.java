package org.gama.sql.binder;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * An interface that allows to use {@link ResultSet#getXXX(...)} method to be used as method reference.
 * 
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface ResultSetReader<O> {
	
	/**
	 * Read column <t>columnName</t> returned by <t>resultSet</t>
	 *
	 * @param resultSet the {@link ResultSet} to read
	 * @param columnName the column to be read from the given {@link ResultSet}
	 * @return content of <t>columnName</t>, typed according to <t>column</t>
	 * @throws SQLException the exception thrown be the underlying access to the {@link ResultSet}
	 */
	O get(ResultSet resultSet, String columnName) throws SQLException;
}
