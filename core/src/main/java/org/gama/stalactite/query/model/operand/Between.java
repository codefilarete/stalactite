package org.gama.stalactite.query.model.operand;

import org.gama.stalactite.query.model.Operand;

/**
 * Represents a between operand.
 * Values will be stored as {@link Interval}.
 * 
 * @author Guillaume Mary
 */
public class Between extends Operand {
	
	public Between(Interval value) {
		super(value);
	}
	
	public Between(Object value1, Object value2) {
		this(new Interval(value1, value2));
	}
	
	/**
	 * Overriden to return null when both {@link Interval} boundaries are null.
	 * Done to help managing "is null" case in {@link org.gama.stalactite.query.builder.WhereBuilder} (else we have to add a if there)
	 * 
	 * @return null if value boundaries are both null.
	 */
	@Override
	public Interval getValue() {
		Interval interval = (Interval) super.getValue();
		return (interval == null || interval.isEmpty()) ? null : interval;
	}
	
	/**
	 * A small class to store between values
	 */
	public static class Interval {
		
		private final Object value1;
		private final Object value2;
		
		public Interval(Object value1, Object value2) {
			this.value1 = value1;
			this.value2 = value2;
		}
		
		public Object getValue1() {
			return value1;
		}
		
		public Object getValue2() {
			return value2;
		}
		
		public boolean isEmpty() {
			return getValue1() == null && getValue2() == null;
		}
	}
}
