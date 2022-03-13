package org.codefilarete.stalactite.sql.order;

import java.util.LinkedHashSet;
import java.util.Set;

import org.codefilarete.stalactite.sql.order.InsertCommandBuilder.InsertStatement;
import org.codefilarete.stalactite.sql.order.Update.UpdateColumn;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * A fluent way of writing a SQL insert clause by leveraging {@link Column} : values can only be set through it.
 * 
 * @author Guillaume Mary
 * @see InsertCommandBuilder
 */
public class Insert<T extends Table> {
	
	/** Target of the values to insert */
	private final T targetTable;
	/** Target columns of the insert */
	private final Set<UpdateColumn> columns = new LinkedHashSet<>();
	
	public Insert(T targetTable) {
		this.targetTable = targetTable;
	}
	
	public T getTargetTable() {
		return targetTable;
	}
	
	/**
	 * Adds a column to set a value for, without predefined value. Then value can be set through {@link InsertStatement#setValue(Column, Object)} if
	 * {@link InsertCommandBuilder#toStatement(ColumnBinderRegistry)} is used to build the SQL order.
	 *
	 * Overwrites any previous value put for that column.
	 * 
	 * @param column any column
	 * @return this
	 */
	public Insert<T> set(Column<T, ?> column) {
		this.columns.add(new UpdateColumn((Column<Table, Object>) column));
		return this;
	}
	
	/**
	 * Adds a column to set and its value. Overwrites any previous value put for that column.
	 * 
	 * @param column any column
	 * @param value value to be inserted
	 * @param <C> value type
	 * @return this
	 */
	public <C> Insert<T> set(Column<T, C> column, C value) {
		this.columns.add(new UpdateColumn((Column<Table, Object>) column, value));
		return this;
	}
	
	/**
	 * Gives all columns targeted by this insert
	 * 
	 * @return a non null {@link Set}
	 */
	public Set<UpdateColumn> getColumns() {
		return columns;
	}
}
