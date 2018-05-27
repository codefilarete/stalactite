package org.gama.stalactite.persistence.mapping;

import org.gama.stalactite.persistence.structure.Table;

/**
 * The interface defining methods necessary to persist an entity (ie an object with an id)
 * 
 * @author Guillaume Mary
 */
public interface IEntityMappingStrategy<C, I, T extends Table> extends IMappingStrategy<C, T>, IIdAccessor<C, I> {
	
	T getTargetTable();
	
	/**
	 * Necessary to distinguish insert or update action on {@link org.gama.stalactite.persistence.engine.Persister#persist(Object)} call
	 * @param c an instance of C
	 * @return true if the instance is not persisted, false if not (a row for its identifier already exists in the targeted table)
	 */
	boolean isNew(C c);
}
