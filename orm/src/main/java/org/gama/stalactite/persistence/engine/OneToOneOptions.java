package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderOneToOneOptions;

/**
 * @author Guillaume Mary
 */
public interface OneToOneOptions<T, I>
		extends CascadeOptions<IFluentMappingBuilderOneToOneOptions<T, I>> {
	
	/** Marks the relation as mandatory. Hence joins will be inner ones and a checking for non null value will be done before insert and update */
	IFluentMappingBuilderOneToOneOptions<T, I> mandatory();
}
