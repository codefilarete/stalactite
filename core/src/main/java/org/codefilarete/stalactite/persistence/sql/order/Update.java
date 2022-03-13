package org.codefilarete.stalactite.persistence.sql.order;

import java.util.LinkedHashSet;
import java.util.Set;

import org.codefilarete.stalactite.persistence.sql.order.UpdateCommandBuilder.UpdateStatement;
import org.codefilarete.stalactite.persistence.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Table;
import org.codefilarete.stalactite.query.model.AbstractRelationalOperator;
import org.codefilarete.stalactite.query.model.Criteria;
import org.codefilarete.stalactite.query.model.CriteriaChain;

/**
 * A fluent way of writing a SQL update clause by leveraging {@link Column} : update and where clauses are only made of it.
 * It handles multi table update by allowing to add columns coming from different tables, but then it is up to caller to join them correctly as
 * he would do for standard SQL, because no check is done by this class nor by {@link UpdateCommandBuilder}.
 * 
 * @author Guillaume Mary
 * @see UpdateCommandBuilder
 */
public class Update {
	
	/** Main table of columns to update */
	private final Table targetTable;
	
	/** Target columns of the update */
	private final Set<UpdateColumn> columns = new LinkedHashSet<>();
	
	private final Criteria<?> criteriaSurrogate = new Criteria<>();
	
	public Update(Table targetTable) {
		this.targetTable = targetTable;
	}
	
	public Table getTargetTable() {
		return targetTable;
	}
	
	public Criteria getCriteria() {
		return criteriaSurrogate;
	}
	
	/**
	 * Adds a column to update without predefined value. Then value can be set through {@link UpdateStatement#setValue(Column, Object)} if
	 * {@link UpdateCommandBuilder#toStatement(ColumnBinderRegistry)} is used to build the SQL order.
	 * 
	 * Overwrites any previous value put for that column.
	 * 
	 * @param column any column
	 * @return this
	 */
	public Update set(Column<Table, ?> column) {
		this.columns.add(new UpdateColumn(column));
		return this;
	}
	
	/**
	 * Adds a column to update with its value.
	 * 
	 * @param column any column
	 * @param value value for given column
	 * @param <C> value type
	 * @return this
	 */
	public <C> Update set(Column<Table, C> column, C value) {
		this.columns.add(new UpdateColumn(column, value));
		return this;
	}
	
	/**
	 * Adds a target column which value is took from another column
	 *
	 * @param column1 any column
	 * @param column2 any column
	 * @return this
	 */
	public <C> Update set(Column<Table, C> column1, Column<Table, C> column2) {
		this.columns.add(new UpdateColumn((Column<Table, Object>) column1, column2));
		return this;
	}
	
	/**
	 * Gives all columns that are target of the update
	 * @return a non null {@link Set}
	 */
	public Set<UpdateColumn> getColumns() {
		return columns;
	}
	
	/**
	 * Adds a criteria to this update.
	 * 
	 * @param column a column target of the condition
	 * @param condition the condition
	 * @return this
	 */
	public CriteriaChain where(Column column, String condition) {
		return criteriaSurrogate.and(column, condition);
	}
	
	/**
	 * Adds a criteria to this update.
	 *
	 * @param column a column target of the condition
	 * @param condition the condition
	 * @return this
	 */
	public CriteriaChain where(Column column, AbstractRelationalOperator condition) {
		return criteriaSurrogate.and(column, condition);
	}
	
	/**
	 * {@link Column} and its value to be updated
	 */
	public static class UpdateColumn {
		
		public static final Object PLACEHOLDER = new Object();
		
		private final Column<Table, Object> column;
		private final Object value;
		
		public UpdateColumn(Column<Table, ?> column) {
			this((Column<Table, Object>) column, PLACEHOLDER);
		}
		
		public <C> UpdateColumn(Column<Table, C> column, C value) {
			this.column = (Column<Table, Object>) column;
			this.value = value;
		}
		
		public Column<Table, Object> getColumn() {
			return column;
		}
		
		public Object getValue() {
			return value;
		}
	}
}
