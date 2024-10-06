package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.tool.collection.Iterables;

/**
 * Implementation of cast SQL function
 * 
 * @author Guillaume Mary
 */
public class Cast<V> extends SQLFunction<V> {
	
	private final Integer typeSize;
	
	/**
	 * @param expression statement to be cast to given type
	 * @param castType Java type used to find SQL cast type through {@link org.codefilarete.stalactite.sql.Dialect} type mapping
	 */
	public Cast(String expression, Class<V> castType) {
		super("cast", castType, expression);
		this.typeSize = null;
	}
	
	/**
	 * @param casted column to be cast to given type
	 * @param castType Java type used to find SQL cast type through {@link org.codefilarete.stalactite.sql.Dialect} type mapping
	 */
	public Cast(Selectable<?> casted, Class<V> castType) {
		this(casted, castType, null);
	}
	
	/**
	 * @param casted column to be cast to given type
	 * @param castType Java type used to find SQL cast type through {@link org.codefilarete.stalactite.sql.Dialect} type mapping
	 * @param typeSize to be used in case of type requiring a size
	 */
	public Cast(Selectable<?> casted, Class<V> castType, Integer typeSize) {
		super("cast", castType, casted);
		this.typeSize = typeSize;
	}
	
	public Object getCastTarget() {
		return Iterables.first(getArguments());
	}
	
	public Integer getTypeSize() {
		return typeSize;
	}
}
