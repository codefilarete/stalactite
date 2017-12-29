package org.gama.stalactite.query.model.operand;

import org.gama.stalactite.query.model.Operand;

/**
 * Represents a greater (< or <=) comparison
 * 
 * @author Guillaume Mary
 */
public class Greater extends Operand {
	
	private boolean equals;
	
	public Greater(Object value) {
		this(value, false);
	}
	
	public Greater(Object value, boolean equals) {
		super(value);
		setEquals(equals);
	}
	
	public void setEquals(boolean equals) {
		this.equals = equals;
	}
	
	public boolean isEquals() {
		return equals;
	}
}
