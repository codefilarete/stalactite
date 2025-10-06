package org.codefilarete.stalactite.dsl.property;

/**
 * Parent options for one-to-one and one-to-many relation configuration
 * 
 * @author Guillaume Mary
 */
public interface CascadeOptions {
	
	/**
	 * Specifies relation maintenance mode.
	 * Default is {@link RelationMode#ALL}
	 * 
	 * @param relationMode any {@link RelationMode}
	 * @return the global mapping configurer
	 */
	CascadeOptions cascading(RelationMode relationMode);
	
	enum RelationMode {
		
		/**
		 * Will cascade any insert, update or delete order to target entities and any association record if present
		 * (case of relation not owned by target entities). 
		 * Will not delete orphan, see {@link #ALL_ORPHAN_REMOVAL} for such case.
		 * It is the default value.
		 */
		ALL,
		/**
		 * Same as {@link #ALL} but will delete target entities removed from the association (orphans).
		 */
		ALL_ORPHAN_REMOVAL,
		/**
		 * Relevant only for relations with association table (between source entities and target ones) line one-to-many and many-to-many.
		 * <strong>If used on cases without association table (one-to-one or one-to-many owned by target), an exception will be thrown at configuration time.</strong>
		 * <p/>
		 * Sets target entities as readonly, so only association record will be maintained.
		 * Useful when an aggregate (Domain Driven Design term) wants to be linked to an entity of another aggregate without modifying it: this mode
		 * will only manage association table records.
		 */
		ASSOCIATION_ONLY,
		/**
		 * Declares relation as readonly: no insert, update nor delete will be performed on target entities (nor association records if it exists)
		 */
		READ_ONLY
	}
}
