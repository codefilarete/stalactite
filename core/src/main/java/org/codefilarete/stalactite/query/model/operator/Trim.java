package org.codefilarete.stalactite.query.model.operator;

/**
 * Implementation of <code>trim</code> SQL function
 * 
 * @param <V> value type
 * @author Guillaume Mary
 */
public class Trim<V> extends SQLFunction<V, CharSequence> {
	
	public Trim(V colum) {
		super("trim", CharSequence.class, colum);
	}
}
