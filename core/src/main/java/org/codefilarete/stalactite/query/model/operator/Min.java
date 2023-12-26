package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;

/**
 * Represents a min operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Min<N extends Number> extends SQLFunction<N> {
	
	public Min(Selectable<N> value) {
		super("min", value.getJavaType(), value);
	}
}
