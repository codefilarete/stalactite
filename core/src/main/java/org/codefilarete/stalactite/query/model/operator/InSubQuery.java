package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryProvider;
import org.codefilarete.stalactite.query.model.QueryStatement;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.query.model.Variable;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Represents an "IN" condition with a subquery as its value.
 * 
 * @param <O>
 * @author Guillaume Mary
 */
public class InSubQuery<O> extends ConditionalOperator<O, QueryProvider<?>> {
	
	private ValuedVariable<QueryProvider<?>> value;
	
	public InSubQuery() {
	}
	
	public InSubQuery(ValuedVariable<QueryProvider<?>> value) {
		this.value = value;
	}
	
	public InSubQuery(QueryProvider<?> value) {
		this(new ValuedVariable<>((value)));
	}
	
	public InSubQuery(QueryStatement value) {
		this(() -> value);
	}
	
	public ValuedVariable<QueryProvider<?>> getValue() {
		return value;
	}
	
	public QueryStatement getQuery() {
		return nullable(value.getValue()).map(QueryProvider::getQuery).get();
	}
	
	@Override
	public void setValue(Variable<QueryProvider<?>> value) {
		this.value = (ValuedVariable<QueryProvider<?>>) value;
	}
	
	@Override
	public boolean isNull() {
		// we consider this class as non being able to be transformed into the "is null" SQL operator.
		return false;
	}
}

