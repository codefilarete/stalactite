package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderOneToManyOptions;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface OneToManyOptions<T extends Identified, I extends StatefullIdentifier, O extends Identified>
	extends CascadeOption<IFluentMappingBuilderOneToManyOptions<T, I, O>> {
	
	/**
	 * Defines the bidirectional relationship.
	 * No need to additionally call {@link #mappedBy(SerializableFunction)} or {@link #mappedBy(Column)}.
	 * 
	 * If the relationship is already defined throught {@link #mappedBy(Column)} or {@link #mappedBy(SerializableFunction)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link CascadeManyConfigurer}.
	 * 
	 * @param reverseLink opposite owner of the relation (setter)
	 * @return the global mapping configurer
	 */
	IFluentMappingBuilderOneToManyOptions<T, I, O> mappedBy(SerializableBiConsumer<O, T> reverseLink);
	
	/**
	 * Defines the bidirectional relationship.
	 * No need to additionally call {@link #mappedBy(SerializableBiConsumer)} or {@link #mappedBy(Column)}.
	 *
	 * If the relationship is already defined throught {@link #mappedBy(Column)} or {@link #mappedBy(SerializableBiConsumer)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link CascadeManyConfigurer}.
	 * 
	 * @param reverseLink opposite owner of the relation (getter)
	 * @return the global mapping configurer
	 */
	IFluentMappingBuilderOneToManyOptions<T, I, O> mappedBy(SerializableFunction<O, T> reverseLink);
	
	/**
	 * Defines reverse side owner.
	 * Note that defining it this way will not allow relation to be fixed in memory (after select in database), prefer {@link #mappedBy(SerializableBiConsumer)}.
	 * Use this method to define unidirectional relationship.
	 *
	 * If the relationship is already defined throught {@link #mappedBy(SerializableFunction)} or {@link #mappedBy(SerializableBiConsumer)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link CascadeManyConfigurer}.
	 * 
	 * @param reverseLink opposite owner of the relation
	 * @return the global mapping configurer
	 */
	IFluentMappingBuilderOneToManyOptions<T, I, O> mappedBy(Column<Table, T> reverseLink);
	
	/**
	 * Asks for deletion of removed entities from the collection (kind of orphan removal) during UPDATE cascade. Default is false (conservative)
	 * @return the global mapping configurer
	 */
	IFluentMappingBuilderOneToManyOptions<T, I, O> deleteRemoved();
	
}
