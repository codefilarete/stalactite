package org.gama.stalactite.sql.binder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

import org.gama.lang.Strings;
import org.gama.stalactite.sql.dml.SQLStatement.BindingException;

/**
 * An interface that allows to use {@link ResultSet#getXXX(...)} method to be used as method reference.
 * 
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface ResultSetReader<I> {
	
	/**
	 * Reads column <t>columnName</t> returned by <t>resultSet</t>.
	 * This implementation wraps any exception in a {@link BindingException} and in particular wraps {@link ClassCastException} for better message
	 * handling.
	 * Subclasses are expected to implement {@link #doGet(ResultSet, String)}
	 *
	 * @param resultSet the {@link ResultSet} to read
	 * @param columnName the column to be read from the given {@link ResultSet}
	 * @return content of <t>columnName</t>, typed according to <t>column</t>
	 */
	default I get(ResultSet resultSet, String columnName) {
		try {
			return doGet(resultSet, columnName);
		} catch (SQLException e) {
			throw new BindingException("Error while reading column '" + columnName + "'", e);
		} catch (ClassCastException e) {
			String[] classNames = e.getMessage().split(" cannot be cast to ");
			CharSequence ellipsedValue = Strings.ellipsis(String.valueOf(DefaultResultSetReaders.OBJECT_READER.get(resultSet, columnName)), 15);
			throw new BindingException("Error while reading column '" + columnName + "' : trying to read '" + ellipsedValue + "' as " + classNames[1]
					+ " but was " + classNames[0], e);
		}
	}
	
	/**
	 * Method expected to be overriden for really reading {@link ResultSet} value if one want to benefit from exception handling by {@link #get(ResultSet, String)}
	 *
	 * @param resultSet the {@link ResultSet} to read
	 * @param columnName the column to be read from the given {@link ResultSet}
	 * @return content of <t>columnName</t>, typed according to <t>column</t>
	 * @throws SQLException the exception thrown be the underlying access to the {@link ResultSet}
	 */
	I doGet(ResultSet resultSet, String columnName) throws SQLException;
	
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
