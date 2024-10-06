package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.UnitaryOperator;
import org.codefilarete.stalactite.query.model.ValueWrapper;
import org.codefilarete.stalactite.query.model.ValueWrapper.SQLFunctionWrapper;

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
	
	public Equals(ValueWrapper<O> value) {
		super(value);
	}
	
	public Equals(SQLFunction<O> value) {
		this(new SQLFunctionWrapper<>(value));
	}
}
