package org.gama.stalactite.persistence.mapping;

import org.gama.lang.Reflections;

import java.util.Map;

/**
 * Class for transforming columns into a Map
 * 
 * @author Guillaume Mary
 */
public abstract class ToMapRowTransformer<T extends Map> extends AbstractTransformer<T> {
	
	public ToMapRowTransformer(Class<T> clazz) {
		super(Reflections.getDefaultConstructor(clazz));
	}
	
}
