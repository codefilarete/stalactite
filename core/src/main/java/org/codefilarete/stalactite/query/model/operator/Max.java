package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;

/**
 * Represents a max operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Max<N extends Number> extends SQLFunction<N> {
	
	public Max(Selectable<N> value) {
		super("max", value.getJavaType(), value);
	}
}
