package org.codefilarete.stalactite.sql.order;

import java.util.Set;

import org.codefilarete.stalactite.query.model.Where;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * A fluent way of writing a SQL delete clause by leveraging {@link Column} : conditions can only be set through it.
 * It handles multi table deletion by allowing to add columns coming from different tables, but it is up to caller to join them correctly as
 * he would do for standard SQL, because no check is done by this class nor by {@link DeleteCommandBuilder}
 * 
 * @author Guillaume Mary
 * @see DeleteCommandBuilder
 */
public class Delete<T extends Table<T>> {
	
	/** Main table of values to delete */
	private final T targetTable;
	
	private final Set<PlaceholderVariable<?, T>> row = new KeepOrderSet<>();
	
	private final Where<?> criteria;
	
	public Delete(T targetTable) {
		this(targetTable, new Where<>());
	}
	
	public Delete(T targetTable, Where<?> where) {
		this.targetTable = targetTable;
		this.criteria = where;
	}
	
	public T getTargetTable() {
		return targetTable;
	}
	
	public Set<PlaceholderVariable<?, T>> getRow() {
		return row;
	}
	
	public Where<?> getCriteria() {
		return criteria;
	}
	
	/**
	 * Sets the value of a named parameter.
	 *
	 * @param paramName placeholder name
	 * @param value the value of the placeholder
	 * @return this
	 * @param <O> value type, expected to be compatible with the placeholder one
	 */
	public <O> Delete<T> set(String paramName, O value) {
		this.row.add(new PlaceholderVariable<>(paramName, value));
		return this;
	}
}
