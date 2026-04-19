package org.codefilarete.stalactite.engine.configurer.model;

import java.util.Set;

/**
 * Marker interface for the three polymorphism storage strategies.
 * Typed on the parent entity type and identifier type only, to allow
 * the consuming builder to work with it without knowing the table types.
 */
public interface EntityPolymorphism<C, I> {
	
	/** Returns all sub-entities registered under this polymorphism */
	Set<Mapping<? extends C, ?>> getSubEntities();
}
