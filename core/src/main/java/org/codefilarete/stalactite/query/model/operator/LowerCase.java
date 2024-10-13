package org.codefilarete.stalactite.query.model.operator;

/**
 * Implementation of <code>lower</code> SQL function
 * 
 * @param <V> value type
 * @author Guillaume Mary
 */
public class LowerCase<V> extends SQLFunction<V, CharSequence> {
	
	public LowerCase() {
		super("lower", CharSequence.class);
	}
	
	public LowerCase(V value) {
		super("lower", CharSequence.class, value);
	}
}
