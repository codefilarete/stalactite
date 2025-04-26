package org.codefilarete.stalactite.sql.order;

import java.util.Set;

import org.codefilarete.stalactite.query.model.Criteria;
import org.codefilarete.stalactite.query.model.Where;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * A fluent way of writing a SQL update clause by leveraging {@link Column} : update and where clauses are only made of it.
 * It handles multi table update by allowing to add columns coming from different tables, but then it is up to caller to join them correctly as
 * he would do for standard SQL, because no check is done by this class nor by {@link UpdateCommandBuilder}.
 * 
 * @author Guillaume Mary
 * @see UpdateCommandBuilder
 */
public class Update<T extends Table<T>> {
	
	/** Main table of columns to update */
	private final T targetTable;
	
	/** Target columns of the update */
	private final Set<Column<T, ?>> columnsToUpdate;
	
	private final Set<StatementVariable<?, T>> row = new KeepOrderSet<>();
	
	private final Where<?> criteria;
	
	public Update(T targetTable) {
		this(targetTable, targetTable.getColumns());
	}
	
	public Update(T targetTable, Where<?> where) {
		this(targetTable, targetTable.getColumns(), where);
	}
	
	public Update(T targetTable, Set<? extends Column<T, ?>> columnsToUpdate) {
		this(targetTable, columnsToUpdate, new Where<>());
	}
	
	public Update(T targetTable, Set<? extends Column<T, ?>> columnsToUpdate, Where<?> where) {
		this.targetTable = targetTable;
		this.columnsToUpdate = (Set<Column<T, ?>>) new KeepOrderSet<>(columnsToUpdate);
		this.criteria = where;
	}
	
	public T getTargetTable() {
		return targetTable;
	}
	
	public Set<Column<T, ?>> getColumnsToUpdate() {
		return columnsToUpdate;
	}
	
	public Criteria<?> getCriteria() {
		return criteria;
	}
	
	/**
	 * Adds a column to update with its value.
	 * 
	 * @param column any column
	 * @param value value for given column
	 * @param <C> value type
	 * @return this
	 */
	public <C> Update<T> set(Column<? extends T, C> column, C value) {
		this.row.add(new ColumnVariable<>(column, value));
		return this;
	}
	
	/**
	 * Adds a target column which value is took from another column
	 *
	 * @param column1 any column
	 * @param column2 any column
	 * @return this
	 */
	public <C> Update<T> set(Column<? extends T, C> column1, Column<?, C> column2) {
		this.row.add(new ColumnVariable<>(column1, column2));
		return this;
	}
	
	/**
	 * Sets the value of a named parameter.
	 * 
	 * @param paramName placeholder name
	 * @param value the value of the placeholder
	 * @return this
	 * @param <C> value type, expected to be compatible with the placeholder one
	 */
	public <C> Update<T> set(String paramName, C value) {
		this.row.add(new PlaceholderVariable<>(paramName, value));
		return this;
	}
	
	/**
	 * Gives all columns that are target by the update
	 * @return a non null {@link Set}
	 */
	public Set<StatementVariable<?, T>> getRow() {
		return row;
	}
}
