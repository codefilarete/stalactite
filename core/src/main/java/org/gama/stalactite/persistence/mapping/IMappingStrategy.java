package org.gama.stalactite.persistence.mapping;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.structure.Column;

/**
 * A very general contract for mapping a type to a database table. Not expected to be used as this (for instance it lacks deletion contract)
 * 
 * @author Guillaume Mary
 */
public interface IMappingStrategy<T> {
	
	/**
	 * Returns columns that must be inserted
	 *
	 * @param t the instance to be inserted,
	 * 					may be null when this method is called to manage relationship
	 * @return a mapping between columns that must be put in the SQL insert order and there values
	 */
	Map<Column, Object> getInsertValues(T t);
	
	/**
	 * Returns columns that must be updated because of change between 2 instances.
	 * 
	 * @param modified the modified instance,
	 * 					may be null when this method is called to manage relationship
	 * @param unmodified the not modified instance or "previous state" (coming from database for instance),
	 * 					may be null to generate a rough update statement (allColumn doesn't matters in this case) 
	 * @param allColumns indicates if allColumns must be returned, even if they are not all modified (necessary for JDBC batch optimization)
	 * @return a mapping between columns that must be put in the SQL update order and there values,
	 * 			thus a distinction between columns to be updated and columns necessary to the where clause must be done, this is don through {@link UpwhereColumn},
	 * 			so returned value may contains duplicates regarding {@link Column} (they can be in update & where part, especially for optimist lock columns)	
	 */
	Map<UpwhereColumn, Object> getUpdateValues(T modified, T unmodified, boolean allColumns);
	
	T transform(Row row);
	
	/**
	 * Wrapper for {@link Column} placed in an update statement so it can distinguish if it's for the Update or Where part 
	 */
	class UpwhereColumn {
		
		/**
		 * Collects all columns from a set of {@link UpwhereColumn}. Helper method.
		 * 
		 * @param set a non null set
		 * @return all columns of the set
		 */
		public static Set<Column> getUpdateColumns(@Nonnull Set<UpwhereColumn> set) {
			Set<Column> updateColumns = new HashSet<>();
			for (UpwhereColumn upwhereColumn : set) {
				if (upwhereColumn.update) {
					updateColumns.add(upwhereColumn.column);
				}
			}
			return updateColumns;
		}
		
		/**
		 * Collects all columns to be updated (not for the where clause) from a map of {@link UpwhereColumn}. Helper method.
		 * 
		 * @param map a non null map
		 * @return all columns to be updated
		 */
		public static Map<Column, Object> getUpdateColumns(@Nonnull Map<UpwhereColumn, Object> map) {
			Map<Column, Object> updateColumns = new HashMap<>();
			for (Entry<UpwhereColumn, Object> entry : map.entrySet()) {
				UpwhereColumn upwhereColumn = entry.getKey();
				if (upwhereColumn.update) {
					updateColumns.put(upwhereColumn.column, entry.getValue());
				}
			}
			return updateColumns;
		}
		
		/**
		 * Collects all columns for the where clause (not for the update clause) from a map of {@link UpwhereColumn}. Helper method.
		 *
		 * @param map a non null map
		 * @return all columns for the where clause
		 */
		public static Map<Column, Object> getWhereColumns(@Nonnull Map<UpwhereColumn, Object> map) {
			Map<Column, Object> updateColumns = new HashMap<>();
			for (Entry<UpwhereColumn, Object> entry : map.entrySet()) {
				UpwhereColumn upwhereColumn = entry.getKey();
				if (!upwhereColumn.update) {
					updateColumns.put(upwhereColumn.column, entry.getValue());
				}
			}
			return updateColumns;
		}
		
		private final Column column;
		private final boolean update;
		
		public UpwhereColumn(Column column, boolean update) {
			this.column = column;
			this.update = update;
		}
		
		public Column getColumn() {
			return column;
		}
		
		/**
		 * Implemented to ditinguish columns of the update clause from those of the where part, because the main purpose of this class is to be put
		 * in a {@link Map}.
		 * 
		 * @param obj another object
		 * @return true if the other object has the same column and has the same target : update or where part
		 */
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof UpwhereColumn) {
				UpwhereColumn other = (UpwhereColumn) obj;
				return this.column.equals(other.column) && this.update == other.update;
			} else {
				return false;
			}
		}
		
		/**
		 * Based on hash code of the column
		 * @return column hash code
		 */
		@Override
		public int hashCode() {
			return this.column.hashCode();
		}
		
		/**
		 * Overriden to print "U" or "W" according to the purpose of this instance. Simplifies traces abd eventual debug.
		 * @return the column's absolute name, suffixed by "U" or "W" according to the purpose of this instance 
		 */
		@Override
		public String toString() {
			return column.getAbsoluteName() + (update ? " (U)" : " (W)");
		}
	}
}
