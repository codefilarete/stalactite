package org.gama.stalactite.command.model;

import java.util.LinkedHashSet;
import java.util.Set;

import org.gama.stalactite.command.model.Update.UpdateColumn;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * A simple representation of a SQL insert clause, and a way to build it easily/fluently
 * 
 * @author Guillaume Mary
 * @see org.gama.stalactite.command.builder.InsertCommandBuilder
 */
public class Insert<T extends Table> {
	
	/** Target of the values to insert */
	private final T targetTable;
	/** Target columns of the insert */
	private final Set<UpdateColumn<T>> columns = new LinkedHashSet<>();
	
	public Insert(T targetTable) {
		this.targetTable = targetTable;
	}
	
	public T getTargetTable() {
		return targetTable;
	}
	
	/**
	 * Adds a target column. Overwrites any previous value put for that column.
	 * 
	 * @param column a non null column
	 * @return this
	 */
	public Insert<T> set(Column<T, ?> column) {
		this.columns.add(new UpdateColumn<>((Column<T, Object>) column));
		return this;
	}
	
	/**
	 * Adds a target column and its value. Overwrites any previous value put for that column.
	 * 
	 * @param column a non null column
	 * @param value value to be inserted
	 * @param <C> colun and value type
	 * @return this
	 */
	public <C> Insert<T> set(Column<T, C> column, C value) {
		this.columns.add(new UpdateColumn<>((Column<T, Object>) column, value));
		return this;
	}
	
	/**
	 * Gives all columns that are target of the insert
	 * 
	 * @return a non null {@link Set}
	 */
	public Set<UpdateColumn<T>> getColumns() {
		return columns;
	}
}
