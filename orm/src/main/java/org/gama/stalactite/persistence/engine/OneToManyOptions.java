package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderOneToManyOptions;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.structure.Column;

/**
 * @author Guillaume Mary
 */
public interface OneToManyOptions<T extends Identified, I extends StatefullIdentifier, O extends Identified>
	extends CascadeOption<IFluentMappingBuilderOneToManyOptions<T, I, O>> {
	
	/**
	 * Defines the bidirectional relationship.
	 * @param reverseLink opposite owner of the relation
	 * @return the global mapping configurer
	 */
	IFluentMappingBuilderOneToManyOptions<T, I, O> mappedBy(SerializableBiConsumer<O, T> reverseLink);
	
	/**
	 * Defines reverse side owner.
	 * Note that defining it this way will not allow relation to be fixed in memory (after select in database), prefer {@link #mappedBy(SerializableBiConsumer)}.
	 * Use this method to define unidirectional relationship.
	 * @param reverseLink opposite owner of the relation
	 * @return the global mapping configurer
	 */
	IFluentMappingBuilderOneToManyOptions<T, I, O> mappedBy(Column<T> reverseLink);
	
	/**
	 * Asks for deletion of removed entities from the collection (kind of orphan removal) during UPDATE cascade. Default is false (conservative)
	 * @return the global mapping configurer
	 */
	IFluentMappingBuilderOneToManyOptions<T, I, O> deleteRemoved();
	
}
