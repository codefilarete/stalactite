package org.stalactite.persistence.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.stalactite.lang.collection.ReadOnlyIterator;
import org.stalactite.lang.exception.Exceptions;

/**
 * ResultSet iterator with convert method
 * 
 * @author mary
 */
public abstract class ResultSetIterator<T> extends ReadOnlyIterator<T> {
	
	protected final ResultSet rs;

	/**
	 * Constructor for ResultSetIterator.
	 * @param rs a ResultSet to be wrapped in an Iterator.
	 */
	public ResultSetIterator(ResultSet rs) {
		this.rs = rs;
	}

	/**
	 * Returns true if there are more rows in the ResultSet.
	 * @return boolean true if there are more rows
	 * @throws RuntimeException if an SQLException occurs.
	 */
	@Override
	public boolean hasNext() {
		try {
			return !rs.isLast();
		} catch (SQLException e) {
			Exceptions.throwAsRuntimeException(e);
			return false;
		}
	}
	
	@Override
	protected T getNext() {
		try {
			rs.next();
			return convert(rs);
		} catch (SQLException e) {
			Exceptions.throwAsRuntimeException(e);
			return null;
		}
	}
	
	protected abstract T convert(ResultSet rs) throws SQLException;
}
