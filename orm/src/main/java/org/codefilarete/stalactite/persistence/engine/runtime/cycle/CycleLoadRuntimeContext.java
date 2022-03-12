package org.codefilarete.stalactite.persistence.engine.runtime.cycle;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.tool.trace.ModifiableInt;

/**
 * Keep track of relations and entity ids to be loaded during the resolution of a circular bean graph loading.
 * Expected to be used in a {@link ThreadLocal} during the whole cycle treatment.
 */
class CycleLoadRuntimeContext<SRC, TRGT, TRGTID> {
	
	/**
	 * Used for resource clearance : relations are kept through {@link #loadedRelations} and due to recursive implementation of cycle loading
	 * there's no easy way to know when to clean them ; by incrementing and decrementing a counter before / after each iteration we solve it
	 * since the very last iteration is when counter reaches 0. 
	 */
	private final ModifiableInt selectInvokationCount = new ModifiableInt(0);
	
	private final Map<String, EntityRelationStorage<SRC, TRGT, TRGTID>> loadedRelations = new HashMap<>();
	
	public void addRelationToInitialize(String relationName, SRC src, TRGTID targetId) {
		this.loadedRelations.computeIfAbsent(relationName, k -> new EntityRelationStorage<>())
				.addRelationToInitialize(src, targetId);
	}
	
	public Set<TRGTID> giveIdentifiersToLoad() {
		return this.loadedRelations.values().stream().flatMap(map -> map.getIdentifiersToLoad().stream()).collect(Collectors.toSet());
	}
	
	public EntityRelationStorage<SRC, TRGT, TRGTID> getEntitiesToFulFill(String relationName) {
		return this.loadedRelations.get(relationName);
	}
	
	/**
	 * Method to be invoked before each SQL selection
	 */
	public void beforeSelect() {
		this.selectInvokationCount.increment();
	}
	
	/**
	 * Method to be invoked after each SQL selection
	 */
	public void afterSelect() {
		this.selectInvokationCount.decrement();
		// cleaning context if we've unstacked all selects
		if (this.selectInvokationCount.getValue() == 0) {
			this.loadedRelations.clear();
		}
	}
	
}