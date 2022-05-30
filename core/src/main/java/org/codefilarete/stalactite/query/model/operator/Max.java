package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.query.model.UnitaryOperator;
import org.codefilarete.stalactite.query.model.Selectable;

/**
 * Represents a max operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Max extends UnitaryOperator implements Selectable {
	
	public Max(Column value) {
		super(value);
	}
	
	@Override
	public String getExpression() {
		return "max";
	}
}
