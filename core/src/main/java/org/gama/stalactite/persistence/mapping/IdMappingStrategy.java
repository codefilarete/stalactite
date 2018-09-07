package org.gama.stalactite.persistence.mapping;

import javax.annotation.Nonnull;

import org.gama.stalactite.persistence.id.assembly.IdentifierAssembler;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;

/**
 * Contract about entity identifier mapping.
 * 
 * @author Guillaume Mary
 * @param <C> target persisted class
 * @param <I> identifier class
 * @see SimpleIdMappingStrategy
 * @see ComposedIdMappingStrategy
 */
public interface IdMappingStrategy<C, I> {
	
	boolean isNew(@Nonnull C entity);
	
	IdAccessor<C, I> getIdAccessor();
	
	IdentifierAssembler<C, I> getIdentifierAssembler();
	
	IdentifierInsertionManager<C, I> getIdentifierInsertionManager();
}
