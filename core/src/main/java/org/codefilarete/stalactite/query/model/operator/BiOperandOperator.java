package org.codefilarete.stalactite.query.model.operator;

import java.util.List;

import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.query.model.Variable;

/**
 * Contract for SQL operators that require an operation on the column / value that they compare to their value 
 *
 * @param <V> value type
 * @author Guillaume Mary
 */
public abstract class BiOperandOperator<T, V> extends ConditionalOperator<T, V> {
	
	/** Value of the operator */
	private Variable<V> value;
	
	public BiOperandOperator() {
	}
	
	public BiOperandOperator(Variable<V> value) {
		this.value = value;
	}
	
	public BiOperandOperator(V value) {
		this(new ValuedVariable<>(value));
	}
	
	public Variable<V> getValue() {
		return value;
	}
	
	@Override
	public void setValue(Variable<V> value) {
		this.value = value;
	}
	
	@Override
	public boolean isNull() {
		return this.value instanceof ValuedVariable && ((ValuedVariable) this.value).getValue() == null;
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
