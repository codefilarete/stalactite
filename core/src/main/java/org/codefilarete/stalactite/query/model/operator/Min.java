package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.api.Selectable;

/**
 * Represents a min operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Min<N extends Number> extends SQLFunction<Selectable<N>, Long> {
	
	public Min(Selectable<N> value) {
		super("min", Long.class, value);
	}
}
