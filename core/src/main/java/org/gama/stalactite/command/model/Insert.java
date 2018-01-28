package org.gama.stalactite.command.model;

import java.util.LinkedHashSet;
import java.util.Set;

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
	private final Set<Column> columns = new LinkedHashSet<>();
	
	public Insert(Table targetTable) {
		this.targetTable = targetTable;
	}
	
	public Table getTargetTable() {
		return targetTable;
	}
	
	/**
	 * Adds a target column. If already added it has no consequence.
	 * 
	 * @param column a non null column
	 * @return this
	 */
	public Insert set(Column column) {
		this.columns.add(column);
		return this;
	}
	
	/**
	 * Gives all columns that are target of the insert
	 * 
	 * @return a non null {@link Set}
	 */
	public Set<Column> getColumns() {
		return columns;
	}
}
