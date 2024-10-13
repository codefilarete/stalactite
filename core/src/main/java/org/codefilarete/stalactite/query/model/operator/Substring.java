package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.tool.collection.Arrays;

/**
 * Implementation of <code>substring</code> SQL function
 *
 * @author Guillaume Mary
 */
public class Substring<V extends CharSequence> extends SQLFunction<Iterable<?>, V> {
	
	public Substring(Selectable<V> colum, int from) {
		super("substring", colum.getJavaType(), Arrays.asList(colum, from));
	}
	
	public Substring(Selectable<V> colum, int from, int to) {
		super("substring", colum.getJavaType(), Arrays.asList(colum, from, to));
	}
	
	public Substring(V colum, int from) {
		super("substring", (Class<V>) colum.getClass(), Arrays.asList(colum, from));
	}
	
	public Substring(V colum, int from, int to) {
		super("substring", (Class<V>) colum.getClass(), Arrays.asList(colum, from, to));
	}
	
	public Substring(SQLFunction<?, V> function, int from) {
		super("substring", function.getType(), Arrays.asList(function, from));
	}
	
	public Substring(SQLFunction<?, V> function, int from, int to) {
		super("substring", function.getType(), Arrays.asList(function, from , to));
	}
}
