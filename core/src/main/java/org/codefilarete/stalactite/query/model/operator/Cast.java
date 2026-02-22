package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.ddl.Size;

/**
 * Implementation of cast SQL function
 * 
 * @author Guillaume Mary
 */
public class Cast<V, O> extends SQLFunction<V, O> {
	
	private final Size typeSize;
	
	/**
	 * @param expression statement to be cast to given type
	 * @param castType Java type used to find SQL cast type through {@link org.codefilarete.stalactite.sql.Dialect} type mapping
	 */
	public Cast(String expression, Class<O> castType) {
		super("cast", castType, (V) expression);
		this.typeSize = null;
	}
	
	/**
	 * @param casted column to be cast to given type
	 * @param castType Java type used to find SQL cast type through {@link org.codefilarete.stalactite.sql.Dialect} type mapping
	 */
	public Cast(Selectable<?> casted, Class<O> castType) {
		this(casted, castType, null);
	}
	
	/**
	 * @param casted column to be cast to given type
	 * @param castType Java type used to find SQL cast type through {@link org.codefilarete.stalactite.sql.Dialect} type mapping
	 * @param typeSize to be used in case of type requiring a size
	 */
	public Cast(Selectable<?> casted, Class<O> castType, Size typeSize) {
		super("cast", castType, (V) casted);
		this.typeSize = typeSize;
	}
	
	public Size getTypeSize() {
		return typeSize;
	}
}
