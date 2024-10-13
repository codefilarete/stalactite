package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;

/**
 * Represents a sum operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Sum<N extends Number> extends SQLFunction<Selectable<N>, Long> {
	
	public Sum(Selectable<N> value) {
		super("sum", Long.class, value);
	}
}
