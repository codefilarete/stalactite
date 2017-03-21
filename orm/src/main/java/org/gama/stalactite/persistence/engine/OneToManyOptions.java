package org.gama.stalactite.persistence.engine;

import java.util.function.BiConsumer;

import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderOneToManyOptions;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;

/**
 * @author Guillaume Mary
 */
public interface OneToManyOptions<T extends Identified, I extends StatefullIdentifier, O extends Identified>
	extends CascadeOption<IFluentMappingBuilderOneToManyOptions<T, I, O>> {
	
	IFluentMappingBuilderOneToManyOptions<T, I, O> mappedBy(BiConsumer<O, T> reverseLink);
	
	/**
	 * Ask for deletion of removed entities from the collection (kind of orphan removal) during UPDATE cascade. Default is false (conservative)
	 */
	IFluentMappingBuilderOneToManyOptions<T, I, O> deleteRemoved();
	
}