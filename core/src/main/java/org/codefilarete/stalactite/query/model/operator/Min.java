package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.UnitaryOperator;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.query.model.Selectable;

/**
 * Represents a min operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Min extends UnitaryOperator implements Selectable {
	
	public Min(Column value) {
		super(value);
	}
	
	@Override
	public String getExpression() {
		return "min";
	}
}
