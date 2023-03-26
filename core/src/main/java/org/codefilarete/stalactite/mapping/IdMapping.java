package org.codefilarete.stalactite.mapping;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Contract about entity identifier mapping.
 * 
 * @author Guillaume Mary
 * @param <C> target persisted class
 * @param <I> identifier class
 * @see SimpleIdMapping
 * @see ComposedIdMapping
 */
public interface IdMapping<C, I> {
	
	/**
	 * Expected to indicate if given entity is persisted or not, meaning already exists in database.
	 * Necessary at different steps of the engine, for instance :
	 * - to distinguish SQL statement to apply : insert or update (see {@link EntityPersister#persist(Object)}),
	 * - to prevent from deletion of an instance that's actually not present in database 
	 * 
	 * <br/><br/>
	 * Determining if an instance is persisted or not can merely be done by testing it's id against nullity in majority of cases, that's why this
	 * method is in this class, but we pass it the whole instance so one can use a different strategy such as checking another field of it such
	 * as a creation date ar a simple boolean not owned by identifier.
	 * 
	 * @param entity the entity which the engine need to know if it's persisted 
	 * @return true if the entity is persisted (exists in database), else false
	 */
	boolean isNew(C entity);
	
	IdAccessor<C, I> getIdAccessor();
	
	<T extends Table<T>> IdentifierAssembler<I, T> getIdentifierAssembler();
	
	IdentifierInsertionManager<C, I> getIdentifierInsertionManager();
}
