package org.gama.stalactite.query.model.operand;

import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.model.Operand;

/**
 * Represents a max operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Max extends Operand {
	
	public Max(Column value) {
		super(value);
	}
}
