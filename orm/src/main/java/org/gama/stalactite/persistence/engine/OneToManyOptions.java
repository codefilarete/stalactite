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
	 * Specifies relationship maintenance mode.
	 * Relevent only when an association table exists between source entities and target entities.
	 * By default cascade declared by {@link #cascade(CascadeType, CascadeType...)} will be applied to association table records, but in some
	 * circumstances, such as target entities not direcly managed by source entities (see below), one may need to declare a fine grained cascade on
	 * association records only.
	 *
	 * May be usefull when an aggregate (as such in Domain Driven Design concept) manages an entity that another wants to be linked thought an association
	 * table. The latter should not insert, delete, nor update target table but need to manage the assocciation table records. This method is there
	 * to declare how to behave.
	 *
	 * @param maintenanceMode any {@link RelationshipMaintenanceMode}
	 * @return the global mapping configurer
	 */
	IFluentMappingBuilderOneToManyOptions<T, I, O> relationMode(RelationshipMaintenanceMode maintenanceMode);

	enum RelationshipMaintenanceMode {
		/**
		 * Will cascade any insert, update or delete order to target entities and any association record if present
		 * (case of relation not owned by target entities). 
		 * Will not delete orphan, see {@link #ALL_ORPHAN_REMOVAL} for such case.
		 */
		ALL,
		/** Same as {@link #ALL} but will delete target entities removed from the association (orphans). */
		ALL_ORPHAN_REMOVAL,
		/**
		 * Relevent only when an association table exists between source entities and target ones.
		 * Sets target entities as readonly, so only association record will be maintained.
		 * Usefull when an aggregate (Domain Driven Design term) wants to be linked to an entity of another aggregate without modifying it: this mode
		 * will only manage assocciation table records.
		 */
		ASSOCIATION_ONLY,
		/**
		 * Declares relationship as readonly: no insert, update nor delete will be performed on target entities (nor association reacords if it exist)
		 */
		READ_ONLY
	}
}
