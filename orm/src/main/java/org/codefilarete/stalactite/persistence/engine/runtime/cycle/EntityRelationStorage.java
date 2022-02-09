package org.codefilarete.stalactite.persistence.engine.runtime.cycle;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.stalactite.query.builder.IdentityMap;

/**
 * Mapping between left part of a relation and identifiers of its right part
 */
class EntityRelationStorage<SRC, TRGT, TRGTID> {
	
	/**
	 * Made as an {@link IdentityMap} to avoid hashCode entity implementation to badly influence our entity lookup because it might vary
	 * during entity fulfillment
	 */
	private final IdentityMap<SRC, Duo<SRC, Set<TRGTID>>> entityRelations = new IdentityMap<>();
	
	void addRelationToInitialize(SRC src, TRGTID targetIdentifier) {
		Duo<SRC, Set<TRGTID>> existingRelation = this.entityRelations.get(src);
		if (existingRelation == null) {
			existingRelation = new Duo<>(src, new HashSet<>());
			entityRelations.put(src, existingRelation);
		}
		existingRelation.getRight().add(targetIdentifier);
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