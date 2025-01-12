package org.codefilarete.stalactite.mapping;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Converter;

/**
 * A very general contract for mapping a type to a database table. Not expected to be used as this (for instance it lacks deletion contract)
 * 
 * @author Guillaume Mary
 */
public interface Mapping<C, T extends Table<T>> {
	
	/**
	 * Returns columns that must be inserted
	 *
	 * @param c the instance to be inserted,
	 * 					may be null when this method is called to manage relationship
	 * @return a mapping between columns that must be put in the SQL insert order and there values
	 */
	Map<Column<T, ?>, Object> getInsertValues(C c);
	
	/**
	 * Returns columns that must be updated because of change between 2 instances.
	 * 
	 * @param modified the modified instance,
	 * 					may be null when this method is called to manage relationship
	 * @param unmodified the non modified instance or "previous state" (coming from database for instance),
	 * 					may be null to generate a rough update statement (allColumn doesn't matter in this case) 
	 * @param allColumns indicates if allColumns must be returned, even if they are not all modified (necessary for JDBC batch optimization)
	 * @return a mapping between columns that must be put in the SQL update order and there values,
	 * 			thus a distinction between columns to be updated and columns necessary to the where clause must be done, this is done through {@link UpwhereColumn},
	 * 			so returned value may contain duplicates regarding {@link Column} (they can be in update & where part, especially for optimist lock columns)	
	 */
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
	 * This method might have no purpose for many classes implementing {@link Mapping}.
	 * 
	 * @param valueProvider the column value provider
	 */
	default void addShadowColumnInsert(ShadowColumnValueProvider<C, T> valueProvider) {
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
	 * This method might have no purpose for many classes implementing {@link Mapping}.
	 *
	 * @param valueProvider the column value provider
	 */
	default void addShadowColumnUpdate(ShadowColumnValueProvider<C, T> valueProvider) {
		// does nothing by default
	}
	
	/**
	 * Adds a column to select. This column is not expected to be already mapped to a bean property of the &lt;T&gt; class.
	 *
	 * This method might have no purpose for many classes implementing {@link Mapping}.
	 *
	 * @param column the column to be read
	 * @param <O> Java type of the column value 
	 */
	default <O> void addShadowColumnSelect(Column<T, O> column) {
		// does nothing by default
	}
	
	void addPropertySetByConstructor(ValueAccessPoint<C> accessor);
	
	Map<ReversibleAccessor<C, Object>, Column<T, Object>> getPropertyToColumn();
	
	Map<ReversibleAccessor<C, Object>, Column<T, Object>> getReadonlyPropertyToColumn();
	
	default ValueAccessPointMap<C, Converter<Object, Object>> getReadConverters() {
		return new ValueAccessPointMap<>();
	}
	
	default ValueAccessPointMap<C, Converter<Object, Object>> getWriteConverters() {
		return new ValueAccessPointMap<>();
	}
	
	default Set<Column<T, Object>> getWritableColumns() {
		return new KeepOrderSet<>(getPropertyToColumn().values());
	}
	
	default Set<Column<T, Object>> getReadonlyColumns() {
		return new KeepOrderSet<>(getReadonlyPropertyToColumn().values());
	}
	
	RowTransformer<C> copyTransformerWithAliases(ColumnedRow columnedRow);
	
	/**
	 * Adds a transformer listener, optional operation
	 * @param listener the listener to be notified of transformation
	 */
	default void addTransformerListener(TransformerListener<C> listener) {
		// does nothing by default
	}
	
	/**
	 * Wrapper for {@link Column} placed in an update statement so it can distinguish if it's for the Update or Where part 
	 */
	class UpwhereColumn<T extends Table<T>> {
		
		/**
		 * Collects all columns from a set of {@link UpwhereColumn}. Helper method.
		 * 
		 * @param set a non null set
		 * @return all columns of the set
		 */
		public static <T extends Table<T>> Set<Column<T, Object>> getUpdateColumns(Set<? extends UpwhereColumn<T>> set) {
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
		public static <T extends Table<T>> Map<Column<T, Object>, Object> getUpdateColumns(Map<? extends UpwhereColumn<T>, Object> map) {
			Map<Column<T, Object>, Object> updateColumns = new HashMap<>();
			for (Entry<? extends UpwhereColumn<T>, Object> entry : map.entrySet()) {
				UpwhereColumn<T> upwhereColumn = entry.getKey();
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
		public static <T extends Table<T>> Map<Column<T, Object>, Object> getWhereColumns(Map<? extends UpwhereColumn<T>, Object> map) {
			Map<Column<T, Object>, Object> updateColumns = new HashMap<>();
			for (Entry<? extends UpwhereColumn<T>, Object> entry : map.entrySet()) {
				UpwhereColumn<T> upwhereColumn = entry.getKey();
				if (!upwhereColumn.update) {
					updateColumns.put(upwhereColumn.column, entry.getValue());
				}
			}
			return updateColumns;
		}
		
		private final Column<T, Object> column;
		private final boolean update;
		
		public UpwhereColumn(Column<T, ?> column, boolean update) {
			this.column = (Column<T, Object>) column;
			this.update = update;
		}
		
		public Column getColumn() {
			return column;
		}
		
		public boolean isUpdate() {
			return update;
		}
		
		/**
		 * Implemented to distinguish columns of the update clause from those of the where part, because the main purpose of this class is to be put
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
		 * Overridden to print "U" or "W" according to the purpose of this instance. Simplifies trace and eventual debug.
		 * @return the column's absolute name, suffixed by "U" or "W" according to the purpose of this instance 
		 */
		@Override
		public String toString() {
			return column.getAbsoluteName() + (update ? " (U)" : " (W)");
		}
	}
	
	/**
	 * Contract to provide values of some "unofficial" {@link Column}s out of a mapping strategy at insert and update
	 * time : those columns are not expected to be one of those mapped by properties but can be discriminator, list
	 * index, etc...
	 * <br/>
	 * An instance may reject to provide a value in some circumstances by overriding {@link #accept(Object)} (which
	 * returns true by default).
	 *
	 * Reader may be interested in getting {@link Column} value in select phase, then he may use
	 * {@link Mapping#addShadowColumnSelect(Column)}
	 *
	 * @param <C> bean type to read value from
	 * @param <T> table type
	 * @see #giveValue(Object)
	 * @see #accept(Object)
	 */
	interface ShadowColumnValueProvider<C, T extends Table<T>> {
		
		/**
		 * Made to determine if current column provider must be included to insert / update statement
		 * @param entity entity being persisted
		 * @return true if {@link #giveValue(Object)} must be call
		 */
		default boolean accept(C entity) {
			return true;
		}
		
		/**
		 * Gives the columns to be appended to the insert or update SQL statement.
		 * 
		 * @return columns to be appended to the insert or update SQL statement.
		 */
		Set<Column<T, ?>> getColumns();
		
		/**
		 * Expected to give values to be set in insert or update SQL Statement for given entity
		 * @param bean the entity that is being persisted
		 * @return values per {@link Column} (must be same column instances that were given to {@link #getColumns()})
		 */
		Map<Column<T, ?>, Object> giveValue(C bean);
	}
}
