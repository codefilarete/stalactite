package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderOneToOneOptions;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;

/**
 * @author Guillaume Mary
 */
public interface OneToOneOptions<T extends Identified, I extends StatefullIdentifier>
		extends CascadeOption<IFluentMappingBuilderOneToOneOptions<T, I>> {
	
	/** Mark the relation as mandatory. Hence joins will be inner ones and a checking for non null value will be done before insert and update */
	IFluentMappingBuilderOneToOneOptions<T, I> mandatory();
}
