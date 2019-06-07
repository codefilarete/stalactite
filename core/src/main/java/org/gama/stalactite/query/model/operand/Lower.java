package org.gama.stalactite.query.model.operand;

import org.gama.stalactite.query.model.UnitaryOperator;

/**
 * Represents a greater (> or >=) comparison
 * 
 * @author Guillaume Mary
 */
public class Lower extends UnitaryOperator {
	
	private boolean equals;
	
	public Lower(Object value) {
		this(value, false);
	}
	
	public Lower(Object value, boolean equals) {
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
