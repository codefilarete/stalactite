package org.codefilarete.stalactite.sql.order;

import java.util.Set;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * A fluent way of writing a SQL insert clause by leveraging {@link Column} : values can only be set through it.
 * 
 * @author Guillaume Mary
 * @see InsertCommandBuilder
 */
public class Insert<T extends Table<T>> {
	
	/** Target of the values to insert */
	private final T targetTable;
	/** Target columns of the insert */
	private final Set<InsertColumn<T, ?>> row = new KeepOrderSet<>();
	
	public Insert(T targetTable) {
		this.targetTable = targetTable;
	}
	
	public T getTargetTable() {
		return targetTable;
	}
	
	/**
	 * Adds a column to set and its value. Overwrites any previous value put for that column.
	 * 
	 * @param column any column
	 * @param value value to be inserted
	 * @param <C> value type
	 * @return this
	 */
	public <C> Insert<T> set(Column<? extends T, C> column, C value) {
		this.row.add(new InsertColumn<>(column, value));
		return this;
	}
	
	/**
	 * Gives all columns targeted by this insert
	 * 
	 * @return a non null {@link Set}
	 */
	public Set<InsertColumn<T, ?>> getRow() {
		return row;
	}
	
	
	/**
	 * {@link Column} and its value to be inserted
	 */
	public static class InsertColumn<T extends Table<T>, V> {
		
		private final Column<T, V> column;
		private final V value;
		
		public InsertColumn(Column<? extends T, ? extends V> column, V value) {
			this.column = (Column<T, V>) column;
			this.value = value;
		}
		
		public Column<T, V> getColumn() {
			return column;
		}
		
		public V getValue() {
			return value;
		}
	}
}
