package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.tool.collection.Arrays;

/**
 * Implementation of coalesce SQL function (returns first non-null arguments)
 * 
 * @param <V> value type
 * @author Guillaume Mary
 */
public class Coalesce<V> extends SQLFunction<V> {
	
	public Coalesce(Selectable<V> column, Object... arguments) {
		super("coalesce", column.getJavaType(), Arrays.cat(new Object[] { column }, arguments));
	}
	
	public Coalesce(Class<V> javaType, Object... arguments) {
		super("coalesce", javaType, arguments);
	}
}
