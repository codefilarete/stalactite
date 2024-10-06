package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;

/**
 * Implementation of <code>substring</code> SQL function
 *
 * @author Guillaume Mary
 */
public class Substring<V extends CharSequence> extends SQLFunction<V> {
	
	public Substring(Selectable<V> colum, int from) {
		super("substring", colum.getJavaType(), colum, from);
	}
	
	public Substring(Selectable<V> colum, int from, int to) {
		super("substring", colum.getJavaType(), colum, from, to);
	}
	
	public Substring(V colum, int from) {
		super("substring", (Class<V>) colum.getClass(), colum, from);
	}
	
	public Substring(V colum, int from, int to) {
		super("substring", (Class<V>) colum.getClass(), colum, from, to);
	}
	
	public Substring(SQLFunction<V> function, int from) {
		super("substring", function, from);
	}
	
	public Substring(SQLFunction<V> function, int from, int to) {
		super("substring", function, from , to);
	}
}
