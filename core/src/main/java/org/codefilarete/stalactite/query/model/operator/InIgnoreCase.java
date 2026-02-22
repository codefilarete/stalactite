package org.codefilarete.stalactite.query.model.operator;

import java.util.List;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.query.model.Placeholder;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.query.api.Variable;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;

/**
 * Represents a "in" with ignore case comparison
 *
 * @author Guillaume Mary
 */
public class InIgnoreCase extends BiOperandOperator<String, Iterable<String>> {
	
	public InIgnoreCase() {
	}
	
	public InIgnoreCase(Iterable<String> value) {
		super(value);
	}
	
	public InIgnoreCase(String[] value) {
		this(Arrays.asList(value));
	}
	
	public InIgnoreCase(In<String> other) {
		super(other.getValue());
		setNot(other.isNot());
	}
	
	@Override
	public List<Object> asRawCriterion(Object leftOperand) {
		Variable<Iterable<String>> value = getValue();
		if (value instanceof ValuedVariable) {
			Iterable<String> rawValue = ((ValuedVariable<Iterable<String>>) value).getValue();
			return Arrays.asList(
					new LowerCase<>(leftOperand),
					new In<>(Iterables.stream(rawValue).map(LowerCase::new).collect(Collectors.toList()))
							.not(isNot())
			);
		} else if (value instanceof Placeholder) {
			return Arrays.asList(
					new LowerCase<>(leftOperand),
					new In<>(value)
							.not(isNot())
			);
		} else {
			throw new UnsupportedOperationException("Unsupported value type: " + (value == null ? null : value.getClass()));
		}
	}
}
