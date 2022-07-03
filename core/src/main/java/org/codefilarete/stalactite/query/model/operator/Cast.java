package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;

/**
 * @author Guillaume Mary
 */
public class Cast<V> extends SQLFunction<V> {
	
	private final Integer typeSize;
	
	public Cast(String expression, Class<V> castType) {
		super("cast", castType, expression);
		this.typeSize = null;
	}
	
	public Cast(Selectable<V> casted, Class<V> castType) {
		this(casted, castType, null);
	}
	
	public Cast(Selectable<V> casted, Class<V> castType, Integer typeSize) {
		super("cast", castType, casted);
		this.typeSize = typeSize;
	}
	
	public Object getCastTarget() {
		return getArguments()[0];
	}
	
	public Integer getTypeSize() {
		return typeSize;
	}
}
