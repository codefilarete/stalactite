package org.codefilarete.stalactite.persistence.sql.order;

import java.util.LinkedHashSet;
import java.util.Set;

import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.codefilarete.stalactite.query.model.AbstractRelationalOperator;
import org.codefilarete.stalactite.query.model.Criteria;
import org.codefilarete.stalactite.query.model.CriteriaChain;

/**
 * A simple representation of a SQL update clause, and a way to build it easily/fluently
 * 
 * @author Guillaume Mary
 * @see UpdateCommandBuilder
 */
public class Update<T extends Table> {
	
	/** Target of the values to insert */
	private final T targetTable;
	/** Target columns of the insert */
	private final Set<UpdateColumn> columns = new LinkedHashSet<>();
	
	private final Criteria criteriaSurrogate = new Criteria();
	
	public Update(T targetTable) {
		this.targetTable = targetTable;
	}
	
	public T getTargetTable() {
		return targetTable;
	}
	
	public Criteria getCriteria() {
		return criteriaSurrogate;
	}
	
	/**
	 * Adds a target column. If already added it has no consequence.
	 * 
	 * @param column a non null column
	 * @return this
	 */
	public Update<T> set(Column<T, ?> column) {
		this.columns.add(new UpdateColumn<>((Column<T, Object>) column));
		return this;
	}
	
	public <C> Update<T> set(Column<T, C> column, C value) {
		this.columns.add(new UpdateColumn<>((Column<T, Object>) column, value));
		return this;
	}
	
	/**
	 * Adds a target column which value is took from another column
	 *
	 * @param column1 a non null column
	 * @param column2 a non null column
	 * @return this
	 */
	public <C> Update<T> set(Column<T, C> column1, Column<T, C> column2) {
		this.columns.add(new UpdateColumn<>((Column<T, Object>) column1, column2));
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
	
	public static class UpdateColumn<T extends Table> {
		
		public static final Object PLACEHOLDER = new Object();
		
		private final Column<T, Object> column;
		private final Object value;
		
		public UpdateColumn(Column<T, Object> column) {
			this(column, PLACEHOLDER);
		}
		
		public UpdateColumn(Column<T, Object> column, Object value) {
			this.column = column;
			this.value = value;
		}
		
		public Column<T, Object> getColumn() {
			return column;
		}
		
		public Object getValue() {
			return value;
		}
	}
}
