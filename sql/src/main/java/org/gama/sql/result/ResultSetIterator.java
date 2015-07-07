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
			// NB: isLast() doesn't manage empty ResultSet
			boolean isEmpty = !rs.isBeforeFirst() && rs.getRow() == 0;
			return !isEmpty && !rs.isLast();
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
	
	public abstract T convert(ResultSet rs);
}
