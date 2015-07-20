package org.gama.sql.result;

import org.gama.lang.bean.IConverter;
import org.gama.lang.collection.ReadOnlyIterator;
import org.gama.lang.exception.Exceptions;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * ResultSet iterator with convert method
 * 
 * @author Guillaume Mary
 */
public abstract class ResultSetIterator<T> extends ReadOnlyIterator<T> implements IConverter<ResultSet, T> {
	
	protected final ResultSet rs;
	private boolean nextCalled = false;
	
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
			return !nextCalled && rs.next();
		} catch (SQLException e) {
			Exceptions.throwAsRuntimeException(e);
			// unreachable
			return false;
		} finally {
			nextCalled = true;
		}
	}
	
	@Override
	public T next() {
		try {
			return convert(rs);
		} finally {
			nextCalled = false;
		}
	}
	
	public abstract T convert(ResultSet rs);
}
