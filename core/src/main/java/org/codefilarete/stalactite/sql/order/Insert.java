package org.codefilarete.stalactite.sql.order;

import java.util.Set;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.order.Update.UpdateColumn;
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
	private final Set<UpdateColumn<T, ?>> row = new KeepOrderSet<>();
	
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
		this.row.add(new UpdateColumn<>(column, value));
		return this;
	}
	
	/**
	 * Gives all columns targeted by this insert
	 * 
	 * @return a non null {@link Set}
	 */
	public Set<UpdateColumn<T, ?>> getRow() {
		return row;
	}
}
