package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.UnitaryOperator;
import org.codefilarete.stalactite.query.model.ValueWrapper;

/**
 * Represents a "is null" comparison
 * 
 * @author Guillaume Mary
 */
public class IsNull extends UnitaryOperator<Object> {
	
	public IsNull() {
		super(null);
	}
	
	/**
	 * Overridden to have no effect since SQL isNll() takes no argument
	 * @param value any object
	 */
	@Override
	public void setValue(Object value) {
		// setting a value on this as no effect because it has no sense
	}
	
	/**
	 * Overridden to have no effect since SQL isNll() takes no argument
	 * @param value any object
	 */
	@Override
	public void setValueWrapper(ValueWrapper<Object> value) {
		// setting a value on this as no effect because it has no sense
	}
	
	@Override
	public final boolean isNull() {
		return true;
	}
}
