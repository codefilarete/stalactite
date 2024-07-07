package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;

/**
 * Represents a count operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Count extends SQLFunction<Long> {
	
	public Count(Selectable<?> value) {
		super("count", Long.class, value);
	}
}
