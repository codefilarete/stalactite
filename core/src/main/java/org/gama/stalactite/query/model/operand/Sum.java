package org.gama.stalactite.query.model.operand;

import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.model.Operand;

/**
 * Represents a sum operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Sum extends Operand {
	
	public Sum(Column value) {
		super(value);
	}
}
