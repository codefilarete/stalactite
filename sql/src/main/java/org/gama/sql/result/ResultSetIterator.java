package org.gama.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.gama.lang.bean.IConverter;
import org.gama.lang.collection.ReadOnlyIterator;
import org.gama.lang.exception.Exceptions;

/**
 * ResultSet iterator with convert method
 * 
 * @author Guillaume Mary
 */
public abstract class ResultSetIterator<T> extends ReadOnlyIterator<T> implements IConverter<ResultSet, T, SQLException> {
	
	public static <T> List<T> convert(ResultSet resultSet, IConverter<ResultSet, T, SQLException> converter) {
		return new ResultSetIterator<T>(resultSet) {
			@Override
			public T convert(ResultSet rs) throws SQLException {
				return converter.convert(rs);
			}
		}.convert();
	}
	
	/** The read {@link ResultSet} */
	private ResultSet resultSet;
	/** Mimic the {@link ResultSet#getRow()} method which is not supported by all databases */
	private int rowNumber;
	/**
	 * Flag that indicates if the next method was called.
	 * Used for hasNext() since {@link ResultSet} has no sure way of saying if its read is started or not
	 */
	private boolean nextCalled = false;
	
	/**
	 * Constructor for ResultSetIterator.
	 */
	public ResultSetIterator() {
		resetRowNumber();
	}
	
	/**
	 * Constructor for ResultSetIterator.
	 * @param resultSet a ResultSet to be wrapped in an Iterator.
	 */
	public ResultSetIterator(ResultSet resultSet) {
		this();
		this.resultSet = resultSet;
	}
	
	public void setResultSet(ResultSet resultSet) {
		this.resultSet = resultSet;
		resetRowNumber();
	}
	
	public void resetRowNumber() {
		this.rowNumber = 1;
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
			throw Exceptions.asRuntimeException(e);
		} finally {
			nextCalled = true;
		}
	}
	
	@Override
	public T next() {
		try {
			return read(resultSet);
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
			result.add(read(resultSet));
		}
		return result;
	}
	
	/**
	 * Mainly made for handling {@link SQLException} thrown by {@link #convert(ResultSet)}
	 * 
	 * @param rs the read {@link ResultSet}
	 * @return the result made by {@link #convert(ResultSet)}
	 */
	protected T read(ResultSet rs) {
		T result;
		try {
			result = convert(rs);
			this.rowNumber++;
		} catch (SQLException e) {
			throw new RuntimeException("Error while reading ResultSet on line " + rowNumber, e);
		}
		return result;
	}
	
	/**
	 * Convert current row of the ResultSet
	 * @param rs the ResultSet given at constructor or {@link #setResultSet}
	 * @return the result of the conversion
	 */
	public abstract T convert(ResultSet rs) throws SQLException;
	
}
