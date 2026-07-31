package org.codefilarete.stalactite.engine.configurer.resolver.separatefetch;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Temporary storage of the relation targets found during the first phase of a separate fetch, indexed by the object
 * that owns the relation (which may be the owning entity itself or its identifier, depending on the caller), then by
 * the index given by the association.
 *
 * @param <SRC> type of the object that owns the relation
 * @param <TRGTID> type of the stored targets : their identifier, or the entities themselves when they are loaded by
 * 					the very same query as the one that gives the relation
 * @author Guillaume Mary
 */
public class IndexedRelationStorage<SRC, TRGTID> {
	
	// Note that we use index as key (in the second Map) instead of target entities to allow them to appear twice
	// in the same collection (List). Moreover, thanks to this form, it's easy to be applied to the final Collection
	// because values() automatically returns the entities as sorted
	private final Map<SRC, Map<Integer /* index */, TRGTID>> targetIdPerIndexPerSource = new HashMap<>();
	
	public void add(SRC sourceEntity, TRGTID targetId, Integer index) {
		targetIdPerIndexPerSource.computeIfAbsent(sourceEntity, k -> new TreeMap<>()).put(index, targetId);
	}
	
	/**
	 * @param sourceEntity the object that owns the relation
	 * @return the targets stored for the given source, sorted by their index, null if none was stored
	 */
	public Map<Integer /* index */, TRGTID> get(SRC sourceEntity) {
		return targetIdPerIndexPerSource.get(sourceEntity);
	}
	
	public Map<SRC, Map<Integer /* index */, TRGTID>> getTargetIdPerIndexPerSource() {
		return targetIdPerIndexPerSource;
	}
}
