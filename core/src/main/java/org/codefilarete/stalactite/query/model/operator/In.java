package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.stalactite.query.model.ConditionalOperator;

/**
 * Represents a "in" comparison
 * 
 * @author Guillaume Mary
 */
public class In<O> extends ConditionalOperator<O> {
	
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
