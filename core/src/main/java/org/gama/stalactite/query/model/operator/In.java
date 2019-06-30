package org.gama.stalactite.query.model.operator;

import org.gama.lang.collection.Arrays;
import org.gama.stalactite.query.model.AbstractRelationalOperator;

/**
 * Represents a "in" comparison
 * 
 * @author Guillaume Mary
 */
public class In<O> extends AbstractRelationalOperator<O> {
	
	private final Iterable<O> value;
	
	public In(Iterable<O> value) {
		this.value = value;
	}
	
	public In(O[] value) {
		this(Arrays.asList(value));
	}
	
	public Iterable<O> getValue() {
		return value;
	}
	
	@Override
	public boolean isNull() {
		return getValue() == null;
	}
}
