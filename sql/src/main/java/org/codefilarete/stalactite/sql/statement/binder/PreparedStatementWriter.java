package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Function;

/**
 * An interface that allows to use {@link PreparedStatement#setXXX(...)} methods to be used as method reference.
 * See {@link DefaultPreparedStatementWriters} for some available ones.
 * See {@link ResultSetReader} for its equivalence for reading {@link java.sql.ResultSet}, or
 * {@link ParameterBinder} to accomplish both.
 * 
 * @author Guillaume Mary
 * @see ResultSetReader
 * @see DefaultPreparedStatementWriters
 */
@FunctionalInterface
public interface PreparedStatementWriter<I> {
	
	/**
	 * Applies <code>value</code> at position <code>valueIndex</code> on <code>statement</code>.
	 *
	 * @param preparedStatement PreparedStatement to be used
	 * @param valueIndex parameter index to be set, value for first parameter of methods <code>Statement.setXXX(..)</code>
	 * @param value value to be passed as second argument of methods <code>Statement.setXXX(..)</code>
	 * @throws SQLException the exception thrown be the underlying access to the {@link PreparedStatement}
	 */
	void set(PreparedStatement preparedStatement, int valueIndex, I value) throws SQLException;
	
	/**
	 * Builds a new {@link PreparedStatementWriter} from this one by applying a converter on the input object
	 * 
	 * @param converter the {@link Function} that turns input value to the colum type
	 * @param <O> type of input accepted by resulting {@link PreparedStatementWriter} 
	 * @return a new {@link PreparedStatementWriter} based on this one plus a converting {@link Function}
	 * @see ResultSetReader#thenApply(Function)
	 */
	default <O> PreparedStatementWriter<O> preApply(Function<O, I> converter) {
		return (ps, valueIndex, value) -> this.set(ps, valueIndex, converter.apply(value));
	}
}
