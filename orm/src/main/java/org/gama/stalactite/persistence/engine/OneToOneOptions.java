package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderOneToOneOptions;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;

/**
 * @author Guillaume Mary
 */
public interface OneToOneOptions<T extends Identified, I extends StatefullIdentifier>
		extends CascadeOption<IFluentMappingBuilderOneToOneOptions<T, I>> {
}
