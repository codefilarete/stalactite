package org.codefilarete.stalactite.sql.statement.binder;

/**
 * Parent type for {@link ResultSetReader} and {@link PreparedStatementWriter}.
 * Created for technical reasons : both classes should have {@link #getColumnType()} independently to fulfill some
 * features, but while mixing them in {@link ParameterBinder}, the method of both branches is in conflict because of
 * being a "default" implementation in them. Hence, having an upper-common class solves this, this class is the solution.
 * 
 * @param <I> type targeted by the binder
 * @author Guillaume Mary
 */
public interface JdbcBinder<I> {
	
	/**
	 * Gives the type of the parameter in {@link java.sql.ResultSet} or {@link java.sql.PreparedStatement} this class targets
	 * @return never null
	 */
	Class<I> getType();
	
	/**
	 * Gives the type of column this class targets, which is {@link #getType()} by default. But which is no more it when
	 * wrapping binders for type conversion to deal with column that doesn't match Java type (String <-> Date,
	 * Number <-> String, etc.).
	 * Must be implemented in such case to make database schema aligned with binder and avoid exception at configuration
	 * time if the existing column doesn't match this binder type.
	 * 
	 * 
	 * @return #getType
	 */
	default <O> Class<O> getColumnType() {
		return (Class<O>) getType();
	}
}
