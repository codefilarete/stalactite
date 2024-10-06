package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;

/**
 * Implementation of <code>upper</code> SQL function
 * 
 * @param <V> value type
 * @author Guillaume Mary
 */
public class UpperCase<V extends CharSequence> extends SQLFunction<V> {
	
	public UpperCase(Selectable<V> colum) {
		super("upper", colum.getJavaType(), colum);
	}
	
	public UpperCase(V colum) {
		super("upper", (Class<V>) colum.getClass(), colum);
	}
	
	public UpperCase(SQLFunction<V> value) {
		super("upper", value);
	}
}
