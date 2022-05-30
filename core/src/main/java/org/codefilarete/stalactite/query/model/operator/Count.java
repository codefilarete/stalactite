package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.query.model.UnitaryOperator;
import org.codefilarete.stalactite.query.model.Selectable;

/**
 * Represents a count operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Count extends UnitaryOperator implements Selectable {
	
	public Count(Column value) {
		super(value);
	}
	
	@Override
	public String getExpression() {
		return "count";
	}
}
