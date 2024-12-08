package org.codefilarete.stalactite.query.model.operator;

import java.util.List;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.query.model.UnvaluedVariable;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.query.model.Variable;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;

/**
 * Represents a "in" with ignore case comparison
 *
 * @author Guillaume Mary
 */
public class InIgnoreCase extends BiOperandOperator<Iterable<CharSequence>> {
	
	public InIgnoreCase() {
	}
	
	public InIgnoreCase(Iterable<CharSequence> value) {
		super(value);
	}
	
	public InIgnoreCase(CharSequence[] value) {
		this(Arrays.asList(value));
	}
	
	public InIgnoreCase(In<CharSequence> other) {
		super(other.getValue());
		setNot(other.isNot());
	}
	
	@Override
	public List<Object> asRawCriterion(Object leftOperand) {
		Variable<Iterable<CharSequence>> value = getValue();
		if (value instanceof ValuedVariable) {
			Iterable<CharSequence> rawValue = ((ValuedVariable<Iterable<CharSequence>>) value).getValue();
			return Arrays.asList(
					new LowerCase<>(leftOperand),
					new In<>(Iterables.stream(rawValue).map(LowerCase::new).collect(Collectors.toList()))
							.not(isNot())
			);
		} else if (value instanceof UnvaluedVariable) {
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
