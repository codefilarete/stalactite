package org.gama.sql.result;

import org.gama.lang.bean.IConverter;
import org.gama.lang.collection.ReadOnlyIterator;
import org.gama.lang.exception.Exceptions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * ResultSet iterator with convert method
 * 
 * @author Guillaume Mary
 */
public abstract class ResultSetIterator<T> extends ReadOnlyIterator<T> implements IConverter<ResultSet, T> {
	
	public static <T> List<T> convert(ResultSet resultSet, final IConverter<ResultSet, T> converter) {
		return new ResultSetIterator<T>(resultSet) {
			@Override
			public T convert(ResultSet rs) {
				return converter.convert(rs);
			}
		}.convert();
	}
	
	
	private ResultSet resultSet;
	private boolean nextCalled = false;
	
	/**
	 * Constructor for ResultSetIterator.
	 */
	public ResultSetIterator() {
	}
	
	/**
	 * Constructor for ResultSetIterator.
	 * @param resultSet a ResultSet to be wrapped in an Iterator.
	 */
	public ResultSetIterator(ResultSet resultSet) {
		this.resultSet = resultSet;
	}
	
	public void setResultSet(ResultSet resultSet) {
		this.resultSet = resultSet;
	}
	
	/**
	 * Returns true if there are more rows in the ResultSet.
	 * @return boolean true if there are more rows
	 * @throws RuntimeException if an SQLException occurs.
	 */
	@Override
	public boolean hasNext() {
		try {
			return !nextCalled && resultSet.next();
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
			return convert(resultSet);
		} finally {
			nextCalled = false;
		}
	}
	
	/**
	 * Convert all rows of the ResultSet
	 * @return a List of objects transformed by {@link #convert(ResultSet)}
	 */
	public List<T> convert() {
		List<T> result = new ArrayList<>();
		while(hasNext()) {
			result.add(convert(resultSet));
		}
		return result;
	}
	
	/**
	 * Convert current row of the ResultSet
	 * @param rs the ResultSet given at constructor or {@link #setResultSet}
	 * @return
	 */
	public abstract T convert(ResultSet rs);
	
}
