package org.gama.stalactite.query.model.operator;

import org.gama.stalactite.query.model.UnitaryOperator;

/**
 * Represents an equals comparison
 * 
 * @author Guillaume Mary
 */
public class Equals<O> extends UnitaryOperator<O> {
	
	public Equals(O value) {
		super(value);
	}
}
