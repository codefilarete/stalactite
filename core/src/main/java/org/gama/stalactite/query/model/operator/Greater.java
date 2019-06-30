package org.gama.stalactite.query.model.operator;

import org.gama.stalactite.query.model.UnitaryOperator;

/**
 * Represents a greater (< or <=) comparison
 * 
 * @author Guillaume Mary
 */
public class Greater<O> extends UnitaryOperator<O> {
	
	private boolean equals;
	
	public Greater(O value) {
		this(value, false);
	}
	
	public Greater(O value, boolean equals) {
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
