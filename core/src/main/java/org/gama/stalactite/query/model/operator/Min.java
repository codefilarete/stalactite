package org.gama.stalactite.query.model.operator;

import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.model.UnitaryOperator;

/**
 * Represents a min operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Min extends UnitaryOperator {
	
	public Min(Column value) {
		super(value);
	}
}
