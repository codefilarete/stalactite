package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;

/**
 * Implementation of <code>trim</code> SQL function
 * 
 * @param <V> value type
 * @author Guillaume Mary
 */
public class Trim<V extends CharSequence> extends SQLFunction<V> {
	
	public Trim(Selectable<V> colum) {
		super("trim", colum.getJavaType(), colum);
	}
	
	public Trim(V colum) {
		super("trim", (Class<V>) colum.getClass(), colum);
	}
	
	public Trim(SQLFunction<V> value) {
		super("trim", value);
	}
}
