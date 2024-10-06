package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;

/**
 * Implementation of <code>lower</code> SQL function
 * 
 * @param <V> value type
 * @author Guillaume Mary
 */
public class LowerCase<V extends CharSequence> extends SQLFunction<V> {
	
	public LowerCase(Selectable<V> colum) {
		super("lower", colum.getJavaType(), colum);
	}
	
	public LowerCase(V colum) {
		super("lower", (Class<V>) colum.getClass(), colum);
	}
	
	public LowerCase(SQLFunction<V> value) {
		super("lower", value);
	}
}
