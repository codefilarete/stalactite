package org.gama.stalactite.query.model.operand;

import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.model.UnitaryOperator;

/**
 * Represents a count operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Count extends UnitaryOperator {
	
	public Count(Column value) {
		super(value);
	}
}
