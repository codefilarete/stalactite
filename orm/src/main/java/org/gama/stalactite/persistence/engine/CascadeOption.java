package org.gama.stalactite.persistence.engine;

/**
 * Parent options for one-to-one and one-to-many relationship configuration
 * 
 * @author Guillaume Mary
 */
public interface CascadeOption<R> {
	
	/**
	 * Specifies relationship maintenance mode.
	 * 
	 * @param relationshipMode any {@link RelationshipMode}
	 * @return the global mapping configurer
	 */
	R cascading(RelationshipMode relationshipMode);
	
	enum RelationshipMode {
		/**
		 * Will cascade any insert, update or delete order to target entities and any association record if present
		 * (case of relation not owned by target entities). 
		 * Will not delete orphan, see {@link #ALL_ORPHAN_REMOVAL} for such case.
		 */
		ALL,
		/** Same as {@link #ALL} but will delete target entities removed from the association (orphans). */
		ALL_ORPHAN_REMOVAL,
		/**
		 * Relevent only for one-to-many relationship with association table (between source entities and target ones).<br>
		 * <strong>If used on any other case (one-to-one or one-to-many owned by target), an exception will be thrown at configuration time.</strong>
		 * <p/>
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
