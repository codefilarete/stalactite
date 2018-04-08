package org.gama.stalactite.persistence.mapping;

import java.lang.reflect.Constructor;
import java.util.Collection;

import org.gama.lang.Reflections;

/**
 * Class for transforming columns into a Collection.
 * 
 * @author Guillaume Mary
 */
public abstract class ToCollectionRowTransformer<T extends Collection> extends AbstractTransformer<T> {
	
	public ToCollectionRowTransformer(Class<T> clazz) {
		this(Reflections.getDefaultConstructor(clazz));
	}
	
	public ToCollectionRowTransformer(Constructor<T> constructor) {
		super(constructor);
	}
	
}
