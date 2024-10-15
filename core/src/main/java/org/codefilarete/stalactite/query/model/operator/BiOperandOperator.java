package org.codefilarete.stalactite.query.model.operator;

import java.util.List;

import org.codefilarete.stalactite.query.model.ConditionalOperator;

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
	
	/**
	 * Expected to compute a set of Object representing current operator. Returned objects can be a combination of
	 * {@link org.codefilarete.stalactite.query.model.Selectable} or {@link SQLFunction} for example.
	 * This method is invoked at rendering time, thus the {@link #getValue()} can be used by implementation (value is up-to-date)
	 * 
	 * @param leftOperand the left object used as operand (usually a {@link org.codefilarete.stalactite.query.model.Selectable})
	 * @return a new array of SQL-printable objects
	 */
	public abstract List<Object> asRawCriterion(Object leftOperand);
}
