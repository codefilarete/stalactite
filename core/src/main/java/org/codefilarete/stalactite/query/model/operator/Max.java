package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.api.Selectable;

/**
 * Represents a max operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Max<N extends Number> extends SQLFunction<Selectable<N>, Long> {
	
	public Max(Selectable<N> value) {
		super("max", Long.class, value);
	}
}
