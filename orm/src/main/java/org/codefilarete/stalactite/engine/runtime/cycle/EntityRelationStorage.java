package org.codefilarete.stalactite.engine.runtime.cycle;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.IdentityMap;
import org.codefilarete.tool.collection.Iterables;

/**
 * Mapping between left part of a relation and identifiers of its right part.
 * Created 
 * 
 * @author Guillaume Mary
 */
class EntityRelationStorage<SRC, TRGTID> {
	
	/**
	 * Made as an {@link IdentityMap} to avoid hashCode entity implementation to badly influence our entity lookup because it might vary
	 * during entity fulfillment
	 */
	private final IdentityMap<SRC, Duo<SRC, Set<TRGTID>>> entityRelations = new IdentityMap<>();
	
	void addRelationToInitialize(SRC src, TRGTID targetIdentifier) {
		this.entityRelations.computeIfAbsent(src, k -> new Duo<>(k, new HashSet<>()))
				.getRight().add(targetIdentifier);
	}
	
	Set<TRGTID> getRelationToInitialize(SRC src) {
		return this.entityRelations.get(src).getRight();
	}
	
	Set<SRC> getEntitiesToFulFill() {
		return Iterables.collect(this.entityRelations.values(), Duo::getLeft, HashSet::new);
	}
	
	Set<TRGTID> getIdentifiersToLoad() {
		return this.entityRelations.values().stream().flatMap(duo -> duo.getRight().stream()).collect(Collectors.toSet());
	}
}