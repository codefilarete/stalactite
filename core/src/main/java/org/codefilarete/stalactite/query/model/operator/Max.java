package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.query.model.UnitaryOperator;

/**
 * Represents a max operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Max extends UnitaryOperator {
	
	public Max(Column value) {
		super(value);
	}
}
