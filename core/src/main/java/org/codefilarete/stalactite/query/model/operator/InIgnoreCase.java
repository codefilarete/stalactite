package org.codefilarete.stalactite.query.model.operator;

import java.util.List;
import java.util.stream.Collectors;

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
		return Arrays.asList(
				new LowerCase<>(leftOperand),
				new In<>(Iterables.stream(getValue()).map(LowerCase::new).collect(Collectors.toList()))
						.not(isNot())
		);
	}
}
