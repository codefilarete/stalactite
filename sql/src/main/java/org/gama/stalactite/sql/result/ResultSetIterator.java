package org.gama.stalactite.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.codefilarete.tool.function.ThrowingConverter;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.ReadOnlyIterator;
import org.codefilarete.tool.exception.Exceptions;

/**
 * Turns a {@link ResultSet} into an {@link java.util.Iterator} with row conversion support.
 * You can either give the {@link ResultSet} at construct time or set it through {@link #setResultSet(ResultSet)} to reuse
 * this instance.
 * <br/>
 * Then you can iterate over your {@link ResultSetIterator} within a loop such as this one :
 * <pre>
 *     ResultSetIterator<String> resultSetIterator = new ResultSetIterator<String>(resultSet) {
 *         &#64;Override
 *         public String convert(ResultSet rs) throws SQLException {
 *             return rs.getString("name");
 *         }
 *     };
 *     while (resultSetIterator.hasNext()) {
 *         String next = resultSetIterator.next();
 *     }
 * </pre>
 * <strong>Due to the nature of {@link ResultSet} we can't know if a next row exists without reading it, hence {@link #hasNext()} must be called
 * before each access to {@link #next()}, else you'll get {@link NoSuchElementException} on {@link #next()} call.
 * </strong>
 * 
 * @author Guillaume Mary
 * @see RowIterator
 * @see #convert()
 * @see #convert(ResultSet)
 */
public abstract class ResultSetIterator<T> extends ReadOnlyIterator<T> implements ThrowingConverter<ResultSet, T, SQLException> {
	
	/** The read {@link ResultSet} */
	private ResultSet resultSet;
	
	/**
	 * Mimics {@link ResultSet#getRow()} method which is not supported by all databases
	 * @see #reset()
	 */
	private int rowNumber = 1;
	
	/**
	 * Flag that indicates if the {@link ResultSet#next()} method of the underlying instance was called.
	 * Used for hasNext() since {@link ResultSet} has no sure way of saying if its iteration is started or not
	 * @see #reset() 
	 */
	private boolean nextCalled = false;
	
	private boolean hasNext = false;
	
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
	 * Called when {@link ResultSet} is set. Doesn't rewind the {@link ResultSet} to the begining.
	 */
	private void reset() {
		this.nextCalled = false;
		this.hasNext = false;
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
			// Warn: operator order is very important to prevent next() to be called unecessarly
			return (hasNext = !nextCalled && resultSet.next());
		} catch (SQLException e) {
			throw Exceptions.asRuntimeException(e);
		} finally {
			nextCalled = true;
		}
	}
	
	@Override
	public T next() {
		if (!hasNext) {
			// this is necessary to be compliant with Iterator#next(..) contract
			throw new NoSuchElementException();
		}
		try {
			this.rowNumber++;
			return convert(resultSet);
		} catch (SQLException e) {
			throw new RuntimeException("Error while reading ResultSet on row " + --rowNumber, e);
		} finally {
			nextCalled = false;
			hasNext = false;
		}
	}
	
	/**
	 * Collects all rows of the {@link ResultSet} set by {@link #setResultSet(ResultSet)} to a {@link List}.
	 * This basic implementation implies that the size of the list equals the {@link ResultSet} row count.
	 * 
	 * @return a non null {@link List} containing all converted rows of the {@link ResultSet}
	 */
	public List<T> convert() {
		return Iterables.collectToList(() -> this, Function.identity());
	}
	
	/**
	 * Converts current row of the ResultSet
	 * 
	 * @param resultSet the ResultSet given at constructor or {@link #setResultSet}
	 * @return the result of the conversion
	 */
	@Override	// only for Javadoc
	public abstract T convert(ResultSet resultSet) throws SQLException;
	
}
