package org.codefilarete.stalactite.query.model.operator;

import java.util.List;

import org.codefilarete.tool.collection.Arrays;

/**
 * Represents a "equals" with ignore case comparison
 *
 * @author Guillaume Mary
 */
public class EqualsIgnoreCase<O> extends BiOperandOperator<O> {
	
	public EqualsIgnoreCase() {
	}
	
	public EqualsIgnoreCase(O value) {
		super(value);
	}
	
	public EqualsIgnoreCase(Equals<O> other) {
		super(other.getValue());
		setNot(other.isNot());
	}
	
	@Override
	public List<Object> asRawCriterion(Object leftOperand) {
		return Arrays.asList(
				new LowerCase<>(leftOperand),
				new Equals<>(new LowerCase<>(getValue()))
						.not(isNot()));
	}
}