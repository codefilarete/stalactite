package org.gama.stalactite.persistence.mapping;

import org.gama.lang.Reflections;

import java.util.Collection;

/**
 * Class for transforming columns into a Collection.
 * 
 * @author Guillaume Mary
 */
public abstract class ToCollectionRowTransformer<T extends Collection> extends AbstractTransformer<T> {
	
	public ToCollectionRowTransformer(Class<T> clazz) {
		super(Reflections.getDefaultConstructor(clazz));
	}
	
}
