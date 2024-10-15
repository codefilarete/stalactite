package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.UnitaryOperator;

/**
 * Represents an equals comparison
 * 
 * @author Guillaume Mary
 */
public class Equals<O> extends UnitaryOperator<O> {
	
	public Equals() {
	}
	
	public Equals(O value) {
		super(value);
	}
	
	public EqualsIgnoreCase<O> ignoringCase() {
		return new EqualsIgnoreCase<>(this);
	}
}
