package org.gama.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.gama.lang.bean.IConverter;
import org.gama.lang.collection.ReadOnlyIterator;
import org.gama.lang.exception.Exceptions;

/**
 * Turns a {@link ResultSet} into an {@link java.util.Iterator} with row conversion support.
 * 
 * @author Guillaume Mary
 */
public abstract class ResultSetIterator<T> extends ReadOnlyIterator<T> implements IConverter<ResultSet, T, SQLException> {
	
	/** The read {@link ResultSet} */
	private ResultSet resultSet;
	
	/**
	 * Mimics {@link ResultSet#getRow()} method which is not supported by all databases
	 * @see #reset()
	 */
	private int rowNumber = 1;
	
	/**
	 * Flag that indicates if the next method was called.
	 * Used for hasNext() since {@link ResultSet} has no sure way of saying if its read is started or not
	 * @see #reset() 
	 */
	private boolean nextCalled = false;
	
	/**
	 * Default constructor
	 * 
	 * @see #setResultSet(ResultSet) 
	 */
	public ResultSetIterator() {
	}
	
	/**
	 * Constructor with a given {@link ResultSet}
	 * 
	 * @param resultSet a ResultSet to be wrapped in an Iterator.
	 */
	public ResultSetIterator(ResultSet resultSet) {
		this();
		this.resultSet = resultSet;
	}
	
	/**
	 * Change the given {@link ResultSet} at contruction time. Aimed at reusing this iterator.
	 * 
	 * @param resultSet
	 */
	public void setResultSet(ResultSet resultSet) {
		this.resultSet = resultSet;
		reset();
	}
	
	public ResultSet getResultSet() {
		return resultSet;
	}
	
	public int getRowNumber() {
		return rowNumber;
	}
	
	/**
	 * Called when {@link ResultSet} is set. Doesn't rewind the {@link ResultSet} to the beginning.
	 */
	protected void reset() {
		this.nextCalled = false;
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
			this.rowNumber++;
			return convert(resultSet);
		} catch (SQLException e) {
			throw new RuntimeException("Error while reading ResultSet on line " + getRowNumber(), e);
		} finally {
			nextCalled = false;
		}
	}
	
	/**
	 * Convert current row of the ResultSet
	 * 
	 * @param resultSet the ResultSet given at constructor or {@link #setResultSet}
	 * @return the result of the conversion
	 */
	@Override	// only for Javadoc
	public abstract T convert(ResultSet resultSet) throws SQLException;
	
}
