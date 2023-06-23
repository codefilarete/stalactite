package org.codefilarete.stalactite.engine.runtime.cycle;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.tool.trace.ModifiableInt;

/**
 * Keep track of relations and entity ids to be loaded during the resolution of a circular bean graph loading.
 * Expected to be used in a {@link ThreadLocal} during whole cycle treatment.
 */
class CycleLoadRuntimeContext<SRC, TRGTID> {
	
	private final Map<String, EntityRelationStorage<SRC, TRGTID>> loadedRelations = new HashMap<>();
	
	public void addRelationToInitialize(String relationName, SRC src, TRGTID targetId) {
		this.loadedRelations.computeIfAbsent(relationName, k -> new EntityRelationStorage<>())
				.addRelationToInitialize(src, targetId);
	}
	
	public Set<TRGTID> giveIdentifiersToLoad() {
		return this.loadedRelations.values().stream().flatMap(map -> map.getIdentifiersToLoad().stream()).collect(Collectors.toSet());
	}
	
	public EntityRelationStorage<SRC, TRGTID> getEntitiesToFulFill(String relationName) {
		return this.loadedRelations.get(relationName);
	}
	
}