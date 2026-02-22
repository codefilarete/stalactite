package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.UnitaryOperator;
import org.codefilarete.stalactite.query.api.Variable;

/**
 * Represents a "is null" comparison
 * 
 * @author Guillaume Mary
 */
public class IsNull<O> extends UnitaryOperator<O> {
	
	public IsNull() {
		super((O) null);
	}
	
	/**
	 * Overridden to have no effect since SQL isNll() takes no argument
	 * @param value any object
	 */
	@Override
	public void setValue(Variable<O> value) {
		// setting a value on this as no effect because it has no sense
	}
	
	@Override
	public final boolean isNull() {
		return true;
	}
}
