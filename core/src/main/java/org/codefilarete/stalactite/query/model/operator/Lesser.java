package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.UnitaryOperator;

/**
 * Represents a greater (> or >=) comparison
 * 
 * @author Guillaume Mary
 */
public class Lesser<O> extends UnitaryOperator<O> {
	
	private boolean equals;
	
	public Lesser() {
		this.equals = false;
	}
	
	public Lesser(O value) {
		this(value, false);
	}
	
	public Lesser(O value, boolean equals) {
		super(value);
		this.equals = equals;
	}
	
	public void setEquals(boolean equals) {
		this.equals = equals;
	}
	
	public Lesser<O> equals() {
		setEquals(true);
		return this;
	}
	
	public boolean isEquals() {
		return equals;
	}
	
}
