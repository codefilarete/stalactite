package org.gama.stalactite.persistence.mapping;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.ValueAccessPoint;
import org.gama.stalactite.persistence.mapping.ToBeanRowTransformer.TransformerListener;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;

/**
 * A very general contract for mapping a type to a database table. Not expected to be used as this (for instance it lacks deletion contract)
 * 
 * @author Guillaume Mary
 */
public interface IMappingStrategy<C, T extends Table> {
	
	/**
	 * Returns columns that must be inserted
	 *
	 * @param c the instance to be inserted,
	 * 					may be null when this method is called to manage relationship
	 * @return a mapping between columns that must be put in the SQL insert order and there values
	 */
	@Nonnull
	Map<Column<T, Object>, Object> getInsertValues(C c);
	
	/**
	 * Returns columns that must be updated because of change between 2 instances.
	 * 
	 * @param modified the modified instance,
	 * 					may be null when this method is called to manage relationship
	 * @param unmodified the non modified instance or "previous state" (coming from database for instance),
	 * 					may be null to generate a rough update statement (allColumn doesn't matter in this case) 
	 * @param allColumns indicates if allColumns must be returned, even if they are not all modified (necessary for JDBC batch optimization)
	 * @return a mapping between columns that must be put in the SQL update order and there values,
	 * 			thus a distinction between columns to be updated and columns necessary to the where clause must be done, this is don through {@link UpwhereColumn},
	 * 			so returned value may contains duplicates regarding {@link Column} (they can be in update & where part, especially for optimist lock columns)	
	 */
	@Nonnull
	Map<UpwhereColumn<T>, Object> getUpdateValues(C modified, C unmodified, boolean allColumns);
	
	/**
	 * Transforms the given row into a new bean. It is not "alias proof" so it is not expected to be used in a select which columns haven't their
	 * name in the select clause, over all with alias.
	 * 
	 * @param row a row coming from a select clause of this entity
	 * @return a new bean which properties have been filled by row values
	 */
	C transform(Row row);
	
	/**
	 * Adds a column for insert. This column is not expected to be already mapped to a bean property of the &lt;T&gt; class
	 * Here are some use case examples :
	 * - getting the creation timestamp of a bean without the need to map it to a bean property,
	 * - completing a table with a value that is not expected by the bean but by the table 
	 * 
	 * It is not expected to use it to tamper with the value of a mapped property, even this is not forbidden and may work, it is not guaranteed to
	 * be a feature.
	 * 
	 * This method might have no purpose for many classes implementing {@link IMappingStrategy}.
	 * 
	 * @param column the column to update
	 * @param valueProvider a {@link Function} for providing column value, incoming instance will be passed to it as input
	 * @param <O> Java type of the column value 
	 */
	default <O> void addSilentColumnInserter(Column<T, O> column, Function<C, O> valueProvider) {
		// does nothing by default
	}
	
	/**
	 * Adds a column for update. This column is not expected to be already mapped to a bean property of the &lt;T&gt; class
	 * Here are some use case examples :
	 * - timestamping a bean without the need to map the timestamp to a bean property,
	 * - completing a table with a value that is not expected by the bean but by the table 
	 * 
	 * It is not expected to use it to tamper with the value of a mapped property, even this is not forbidden and may work, it is not guaranteed to
	 * be a feature.
	 * 
	 * This method might have no purpose for many classes implementing {@link IMappingStrategy}.
	 * 
	 * @param column the column to update
	 * @param valueProvider a {@link Function} for providing column value, incoming instance will be passed to it as input
	 * @param <O> Java type of the column value 
	 */
	default <O> void addSilentColumnUpdater(Column<T, O> column, Function<C, O> valueProvider) {
		// does nothing by default
	}
	
	void addPropertySetByConstructor(ValueAccessPoint accessor);
	
	Map<IReversibleAccessor<C, Object>, Column<T, Object>> getPropertyToColumn();
	
	AbstractTransformer<C> copyTransformerWithAliases(ColumnedRow columnedRow);
	
	/**
	 * Adds a tranformer listener, optional operation
	 * @param listener the listener to be notify of transformation
	 */
	default void addTransformerListener(TransformerListener<C> listener) {
		// does nothing by default
	}
	
	/**
	 * Wrapper for {@link Column} placed in an update statement so it can distinguish if it's for the Update or Where part 
	 */
	class UpwhereColumn<T extends Table> {
		
		/**
		 * Collects all columns from a set of {@link UpwhereColumn}. Helper method.
		 * 
		 * @param set a non null set
		 * @return all columns of the set
		 */
		public static <T extends Table> Set<Column<T, Object>> getUpdateColumns(@Nonnull Set<UpwhereColumn<T>> set) {
			Set<Column<T, ?>> updateColumns = new HashSet<>();
			for (UpwhereColumn<T> upwhereColumn : set) {
				if (upwhereColumn.update) {
					updateColumns.add(upwhereColumn.column);
				}
			}
			return (Set) updateColumns;
		}
		
		/**
		 * Collects all columns to be updated (not for the where clause) from a map of {@link UpwhereColumn}. Helper method.
		 * 
		 * @param map a non null map
		 * @return all columns to be updated
		 */
		public static <T extends Table> Map<Column<T, Object>, Object> getUpdateColumns(@Nonnull Map<UpwhereColumn<T>, Object> map) {
			Map<Column<T, Object>, Object> updateColumns = new HashMap<>();
			for (Entry<UpwhereColumn<T>, Object> entry : map.entrySet()) {
				UpwhereColumn<T> upwhereColumn = entry.getKey();
				if (upwhereColumn.update) {
					updateColumns.put((Column<T, Object>) upwhereColumn.column, entry.getValue());
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
		public static <T extends Table> Map<Column<T, Object>, Object> getWhereColumns(@Nonnull Map<UpwhereColumn<T>, ?> map) {
			Map<Column<T, Object>, Object> updateColumns = new HashMap<>();
			for (Entry<UpwhereColumn<T>, ?> entry : map.entrySet()) {
				UpwhereColumn<T> upwhereColumn = entry.getKey();
				if (!upwhereColumn.update) {
					updateColumns.put((Column<T, Object>) upwhereColumn.column, entry.getValue());
				}
			}
			return updateColumns;
		}
		
		private final Column<T, ?> column;
		private final boolean update;
		
		public UpwhereColumn(Column<T, ?> column, boolean update) {
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
