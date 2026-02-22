package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.query.api.Variable;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.stalactite.query.model.ConditionalOperator;

/**
 * Represents a "in" comparison
 * 
 * @author Guillaume Mary
 */
public class In<O> extends ConditionalOperator<O, Iterable<O>> {
	
	private Variable<Iterable<O>> value;
	
	public In() {
	}
	
	public In(Variable<Iterable<O>> value) {
		this.value = value;
	}
	
	public In(Iterable<O> value) {
		this(new ValuedVariable<>(value));
	}
	
	public In(O[] value) {
		this(Arrays.asList(value));
	}
	
	public Variable<Iterable<O>> getValue() {
		return value;
	}
	
	@Override
	public void setValue(Variable<Iterable<O>> value) {
		this.value = value;
	}
	
	@Override
	public boolean isNull() {
		return this.value instanceof ValuedVariable && ((ValuedVariable<Iterable<O>>) this.value).getValue() == null;
	}
	
	public InIgnoreCase ignoringCase() {
		return new InIgnoreCase((In<String>) this);
	}
}
