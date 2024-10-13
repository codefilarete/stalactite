package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.Selectable;

/**
 * Contract for SQL operators that require an operation on the column / value that they compare to their value 
 *
 * @param <V> value type
 * @author Guillaume Mary
 */
public abstract class BiOperandOperator<V> extends ConditionalOperator<V, V> {
	
	/** Value of the operator */
	private V value;
	
	public BiOperandOperator() {
	}
	
	public BiOperandOperator(V value) {
		this.value = value;
	}
	
	public V getValue() {
		return value;
	}
	
	@Override
	public void setValue(V value) {
		this.value = value;
	}
	
	@Override
	public boolean isNull() {
		return this.value == null;
	}
	
	public abstract Object[] asRawCriterion(Selectable<V> selectable);
}
