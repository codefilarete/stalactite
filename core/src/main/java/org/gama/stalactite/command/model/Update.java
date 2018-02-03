package org.gama.stalactite.command.model;

import java.util.LinkedHashSet;
import java.util.Set;

import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.Criteria;
import org.gama.stalactite.query.model.CriteriaChain;
import org.gama.stalactite.query.model.Operand;

/**
 * A simple representation of a SQL update clause, and a way to build it easily/fluently
 * 
 * @author Guillaume Mary
 * @see org.gama.stalactite.command.builder.UpdateCommandBuilder
 */
public class Update {
	
	/** Target of the values to insert */
	private final Table targetTable;
	/** Target columns of the insert */
	private final Set<UpdateColumn> columns = new LinkedHashSet<>();
	
	private final Criteria criteriaSurrogate = new Criteria();
	
	public Update(Table targetTable) {
		this.targetTable = targetTable;
	}
	
	public Table getTargetTable() {
		return targetTable;
	}
	
	public Criteria<?> getCriteria() {
		return criteriaSurrogate;
	}
	
	/**
	 * Adds a target column. If already added it has no consequence.
	 * 
	 * @param column a non null column
	 * @return this
	 */
	public Update set(Column column) {
		this.columns.add(new UpdateColumn(column));
		return this;
	}
	
	public <T> Update set(Column<T> column, T value) {
		this.columns.add(new UpdateColumn(column, value));
		return this;
	}
	
	/**
	 * Adds a target column which value is took from another column
	 *
	 * @param column1 a non null column
	 * @param column2 a non null column
	 * @return this
	 */
	public <T> Update set(Column<T> column1, Column<T> column2) {
		this.columns.add(new UpdateColumn(column1, column2));
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
	public CriteriaChain where(Column column, Operand condition) {
		return criteriaSurrogate.and(column, condition);
	}
	
	public static class UpdateColumn {
		
		public static final Object PLACEHOLDER = new Object();
		
		private final Column column;
		private final Object value;
		
		public UpdateColumn(Column column) {
			this(column, PLACEHOLDER);
		}
		
		public UpdateColumn(Column column, Object value) {
			this.column = column;
			this.value = value;
		}
		
		public Column getColumn() {
			return column;
		}
		
		public Object getValue() {
			return value;
		}
	}
}
