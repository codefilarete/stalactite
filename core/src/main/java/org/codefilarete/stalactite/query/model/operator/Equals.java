package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.UnitaryOperator;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.query.model.Variable;

/**
 * Represents an equals comparison
 * 
 * @author Guillaume Mary
 */
public class Equals<O> extends UnitaryOperator<O> {
	
	public Equals() {
	}
	
	public Equals(Variable<O> value) {
		super(value);
	}
	
	public Equals(O value) {
		super(new ValuedVariable<>(value));
	}
	
	public EqualsIgnoreCase<O> ignoringCase() {
		return new EqualsIgnoreCase<>(this);
	}
}
