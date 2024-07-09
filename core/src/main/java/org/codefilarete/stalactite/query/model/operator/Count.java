package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;

/**
 * Represents a count operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Count extends SQLFunction<Long> {
	
	public Count(Selectable<?>... values) {
		super("count", Long.class, (Object[]) values);
	}
	
	public Count(Iterable<? extends Selectable<?>> values) {
		super("count", Long.class, values);
	}
}
