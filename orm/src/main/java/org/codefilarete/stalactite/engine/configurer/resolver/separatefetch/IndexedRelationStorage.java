package org.codefilarete.stalactite.engine.configurer.resolver.separatefetch;

import java.util.HashMap;
import java.util.Map;

public class IndexedRelationStorage<SRC, TRGTID> {
	
	// Note that we use index as key (in the second Map) instead of target entities to allow them to appear twice
	// in the same collection (List). Moreover, thanks to this form, it's easy to be applied to the final Collection
	// because values() automatically returns the entities as sorted
	private final Map<SRC, Map<Integer /* index */, TRGTID>> targetIdPerIndexPerSource = new HashMap<>();
	
	void add(SRC sourceEntity, TRGTID targetId, Integer index) {
		targetIdPerIndexPerSource.computeIfAbsent(sourceEntity, k -> new HashMap<>()).put(index, targetId);
	}
	
	public Map<SRC, Map<Integer /* index */, TRGTID>> getTargetIdPerIndexPerSource() {
		return targetIdPerIndexPerSource;
	}
}
