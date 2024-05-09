package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.UnitaryOperator;

/**
 * Represents a greater (> or >=) comparison
 * 
 * @author Guillaume Mary
 */
public class Lower<O> extends UnitaryOperator<O> {
	
	private boolean equals;
	
	public Lower() {
		this.equals = false;
	}
	
	public Lower(O value) {
		this(value, false);
	}
	
	public Lower(O value, boolean equals) {
		super(value);
		this.equals = equals;
	}
	
	public void setEquals(boolean equals) {
		this.equals = equals;
	}
	
	public Lower<O> equals() {
		setEquals(true);
		return this;
	}
	
	public boolean isEquals() {
		return equals;
	}
	
}
