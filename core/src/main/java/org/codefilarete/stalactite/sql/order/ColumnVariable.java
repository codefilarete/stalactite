package org.codefilarete.stalactite.sql.order;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.order.UpdateCommandBuilder.UpdateStatement;

/**
 * Column to be set by an update statement.
 * 
 * @param <V>
 * @param <T>
 * @author Guillaume Mary
 */
public class ColumnVariable<V, T extends Table<T>> extends StatementVariable<V, T> {
	
	private final Column<T, V> column;
	private final V value;
	
	public ColumnVariable(Column<? extends T, ? extends V> column, V value) {
		this.column = (Column<T, V>) column;
		this.value = value;
	}
	
	public Column<T, V> getColumn() {
		return column;
	}
	
	public V getValue() {
		return value;
	}
	
	public void applyValueTo(UpdateStatement<T> updateStatement) {
		updateStatement.setValue(column, value);
	}
}