package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;

/**
 * Represents a sum operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Sum<N extends Number> extends SQLFunction<N> {
	
	public Sum(Selectable<N> value) {
		super("sum", value.getJavaType(), value);
	}
}
