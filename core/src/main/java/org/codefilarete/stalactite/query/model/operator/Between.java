package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.builder.WhereSQLBuilderFactory.WhereSQLBuilder;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.query.api.Variable;
import org.codefilarete.stalactite.query.model.operator.Between.Interval;

/**
 * Represents a between operator.
 * Values will be stored as {@link Interval}.
 * 
 * @author Guillaume Mary
 */
public class Between<O> extends ConditionalOperator<O, Interval<O>> {
	
	private Variable<Interval<O>> value;
	
	public Between() {
	}
	
	public Between(Variable<Interval<O>> value) {
		this.value = value;
	}
	
	public Between(Interval<O> value) {
		this(new ValuedVariable<>(value));
	}
	
	public Between(O value1, O value2) {
		this(new Interval<>(value1, value2));
	}
	
	/**
	 * Returns boundaries of this instance, null when both {@link Interval} boundaries are null (done as such to simplify a bit "is null"
	 * code in {@link WhereSQLBuilder})
	 * 
	 * @return null if value boundaries are both null.
	 */
	public Variable<Interval<O>> getValue() {
		return value;
	}
	
	@Override
	public void setValue(Variable<Interval<O>> value) {
		this.value = value;
	}
	
	@Override
	public boolean isNull() {
		return this.value instanceof ValuedVariable
				&& (((ValuedVariable<Interval<O>>) this.value).getValue() == null || ((ValuedVariable<Interval<O>>) this.value).getValue().isEmpty());
	}
	
	/**
	 * A small class to store between values
	 */
	public static class Interval<O> {
		
		private final O value1;
		private final O value2;
		
		public Interval(O value1, O value2) {
			this.value1 = value1;
			this.value2 = value2;
		}
		
		public O getValue1() {
			return value1;
		}
		
		public O getValue2() {
			return value2;
		}
		
		public boolean isEmpty() {
			return getValue1() == null && getValue2() == null;
		}
	}
}
