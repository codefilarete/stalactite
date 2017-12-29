package org.gama.stalactite.query.model.operand;

import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.model.Operand;

/**
 * Represents a min operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Min extends Operand {
	
	public Min(Column value) {
		super(value);
	}
}
