package org.codefilarete.stalactite.query.model.operator;

/**
 * Implementation of <code>upper</code> SQL function
 * 
 * @param <V> value type
 * @author Guillaume Mary
 */
public class UpperCase<V> extends SQLFunction<V, CharSequence> {
	
	public UpperCase(V value) {
		super("upper", CharSequence.class, value);
	}
}
