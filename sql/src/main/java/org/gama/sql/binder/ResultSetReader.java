package org.gama.sql.binder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

/**
 * An interface that allows to use {@link ResultSet#getXXX(...)} method to be used as method reference.
 * 
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface ResultSetReader<I> {
	
	/**
	 * Reads column <t>columnName</t> returned by <t>resultSet</t>
	 *
	 * @param resultSet the {@link ResultSet} to read
	 * @param columnName the column to be read from the given {@link ResultSet}
	 * @return content of <t>columnName</t>, typed according to <t>column</t>
	 * @throws SQLException the exception thrown be the underlying access to the {@link ResultSet}
	 */
	I get(ResultSet resultSet, String columnName) throws SQLException;
	
	/**
	 * Builds a new {@link ResultSetReader} from this one by appliyng a converter on the output object
	 * 
	 * @param converter the {@link Function} that turns output value to the final type
	 * @param <O> final type
	 * @return a new {@link ResultSetReader} based on this one plus a converting {@link Function}
	 * @see PreparedStatementWriter#preApply(Function)
	 */
	default <O> ResultSetReader<O> thenApply(Function<I, O> converter) {
		return (rs, columnName) -> converter.apply(this.get(rs, columnName));
	}
}
