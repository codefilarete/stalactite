package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;

/**
 * Represents a count operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Count<N> extends SQLFunction<N> {
	
	public Count(Selectable<N> value) {
		super("count", value.getJavaType(), value);
	}
}
