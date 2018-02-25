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
public class Insert {
	
	/** Target of the values to insert */
	private final Table targetTable;
	/** Target columns of the insert */
	private final Set<UpdateColumn> columns = new LinkedHashSet<>();
	
	public Insert(Table targetTable) {
		this.targetTable = targetTable;
	}
	
	public Table getTargetTable() {
		return targetTable;
	}
	
	/**
	 * Adds a target column. Overwrites any previous value put for that column.
	 * 
	 * @param column a non null column
	 * @return this
	 */
	public Insert set(Column column) {
		this.columns.add(new UpdateColumn(column));
		return this;
	}
	
	/**
	 * Adds a target column and its value. Overwrites any previous value put for that column.
	 * 
	 * @param column a non null column
	 * @param value value to be inserted
	 * @param <T> colun and value type
	 * @return this
	 */
	public <T> Insert set(Column<T> column, T value) {
		this.columns.add(new UpdateColumn(column, value));
		return this;
	}
	
	/**
	 * Gives all columns that are target of the insert
	 * 
	 * @return a non null {@link Set}
	 */
	public Set<UpdateColumn> getColumns() {
		return columns;
	}
}
