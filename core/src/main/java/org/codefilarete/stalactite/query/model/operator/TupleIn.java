package org.codefilarete.stalactite.query.model.operator;

import java.util.List;

import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.sql.ddl.structure.Column;

/**
 * Represents a "in" that accepts tuples (not supported by all databases)
 *
 * @author Guillaume Mary
 */
public class TupleIn extends ConditionalOperator<Object[], List<Object[]>> {
	
	private final Column[] columns;
	
	private List<Object[]> values;
	
	public TupleIn(Column[] columns, List<Object[]> values) {
		this.columns = columns;
		this.values = values;
	}
	
	public Column[] getColumns() {
		return columns;
	}
	
	public List<Object[]> getValue() {
		return values;
	}
	
	@Override
	public void setValue(List<Object[]> value) {
		this.values = value;
	}
	
	@Override
	public boolean isNull() {
		return false;
	}
}
