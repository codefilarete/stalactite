package org.gama.stalactite.query.model.operand;

import org.gama.stalactite.query.model.Operand;

/**
 * Represents a "is null" comparison
 * 
 * @author Guillaume Mary
 */
public class IsNull extends Operand {
	
	public IsNull() {
		super(null);
	}
	
	@Override
	public void setValue(Object value) {
		// setting a value on this as no effect because it has no sense
	}
}
