package org.stalactite.persistence.mapping;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.stalactite.lang.collection.ReadOnlyIterator;
import org.stalactite.lang.exception.Exceptions;

/**
 * @author mary
 */
public interface IResultSetTransformer<E> {
	
	public E transform(ResultSet resultSet) throws SQLException;
	
	public void convertColumnsToProperties(ResultSet resultSet, E rowBean) throws SQLException, IllegalAccessException;
	
	public static class ResultSetIterator extends ReadOnlyIterator<ResultSet> {
		
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
		protected ResultSet getNext() {
			try {
				rs.next();
				return rs;
			} catch (SQLException e) {
				Exceptions.throwAsRuntimeException(e);
				return null;
			}
		}
	}
}
