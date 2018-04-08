package org.gama.stalactite.persistence.mapping;

import java.lang.reflect.Constructor;
import java.util.Map;

import org.gama.lang.Reflections;

/**
 * Class for transforming columns into a Map
 * 
 * @author Guillaume Mary
 */
public abstract class ToMapRowTransformer<T extends Map> extends AbstractTransformer<T> {
	
	public ToMapRowTransformer(Class<T> clazz) {
		this(Reflections.getDefaultConstructor(clazz));
	}
	
	public ToMapRowTransformer(Constructor<T> constructor) {
		super(constructor);
	}
}
