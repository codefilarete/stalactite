package org.codefilarete.stalactite.engine.runtime.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.codefilarete.tool.Experimental;

/**
 * Annotation used on test methods to document and categorize the mapping scenario or use case being verified.
 * Allows specifying characteristics such as root entity polymorphism and the relations involved
 * (type, ownership, fetching strategy, and target polymorphism).
 */
@Experimental
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface UseCase {
	
	/**
	 * Defines the polymorphism strategy of the root entity under test.
	 *
	 * @return the polymorphism strategy, defaults to {@link Polymorphism#NON_POLYMORPHIC}
	 */
	Polymorphism polymorphism() default Polymorphism.NON_POLYMORPHIC;
	
	/**
	 * Describes the relations configured and tested in this use case.
	 *
	 * @return an array of relation configurations
	 */
	Relation[] relation() default {};
	
	/**
	 * Describes an association or relation between entities in the test case.
	 */
	@interface Relation {
		
		/**
		 * The cardinality and directionality type of this relation.
		 *
		 * @return the relation type
		 */
		Type type();
		
		/**
		 * Specifies which side holds the foreign key or association ownership.
		 *
		 * @return the relation ownership, defaults to {@link RelationOwnership#BY_SOURCE}
		 */
		RelationOwnership ownership() default RelationOwnership.BY_SOURCE;
		
		/**
		 * Specifies the strategy used to fetch the related entities.
		 *
		 * @return the fetch strategy, defaults to {@link Fetch#JOIN}
		 */
		Fetch fetch() default Fetch.JOIN;
		
		/**
		 * Specifies the polymorphism strategy for the target entity of the relation.
		 *
		 * @return the target polymorphism, defaults to {@link Polymorphism#NON_POLYMORPHIC}
		 */
		Polymorphism polymorphism() default Polymorphism.NON_POLYMORPHIC;
		
		/**
		 * Relation cardinality types.
		 */
		enum Type {
			/**
			 * The relation is a one-to-one relation, meaning that the source entity has a single target entity
			 */
			ONE_TO_ONE,
			/**
			 * The relation is a one-to-many relation, meaning that the source entity has a collection of target entities
			 */
			ONE_TO_MANY,
			/**
			 * The relation is a many-to-one relation, meaning that the source entity has a single target entity and vice versa
			 */
			MANY_TO_ONE,
			/**
			 * The relation is a many-to-many relation, meaning that the source entity has a collection of target entities and vice versa
			 */
			MANY_TO_MANY
		}
		
		/**
		 * Fetching strategies for entity relations.
		 */
		enum Fetch {
			/**
			 * The relation is fetched in a single query with the source entity
			 */
			JOIN,
			/**
			 * The relation is fetched in a separate query after the source entity has been fetched
			 */
			SEPARATE_QUERY
		}
		
		/**
		 * Entity or table responsible for maintaining the foreign key / association.
		 */
		enum RelationOwnership {
			/**
			 * The relation is owned by the source entity, meaning that the source entity is responsible for the relation
			 */
			BY_SOURCE,
			/**
			 * The relation is owned by the target entity, meaning that the target entity is responsible for the relation
			 */
			BY_TARGET,
			/**
			 * The relation is owned by an association table, meaning that the association table is responsible for the relation
			 */
			BY_ASSOCIATION_TABLE
		}
	}
	
	/**
	 * Inheritance / polymorphism mapping strategies.
	 */
	enum Polymorphism {
		/**
		 * The relation is not polymorphic, meaning that the target entity is of a single type
		 */
		NON_POLYMORPHIC,
		/**
		 * The relation has a table-per-class polymorphism
		 */
		TABLE_PER_CLASS,
		/**
		 * The relation has a single-table polymorphism
		 */
		SINGLE_TABLE,
		/**
		 * The relation has a join polymorphism
		 */
		JOIN_TABLE
	}
}
