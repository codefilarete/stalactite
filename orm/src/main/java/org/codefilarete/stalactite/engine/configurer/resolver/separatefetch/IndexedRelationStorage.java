package org.codefilarete.stalactite.engine.configurer.resolver.separatefetch;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
public class IndexedRelationStorage<SRC, TRGTID> extends AbstractRelationStorage<SRC, Map<Integer, TRGTID>> {
	
	public void add(SRC source, TRGTID targetId, Integer index) {
		// Note that we use index as key (in the second Map) instead of target entities to allow them to appear twice
		// in the same collection (List). Moreover, thanks to this form, it's easy to be applied to the final Collection
		// because values() automatically returns the entities as sorted
		getOrCreate(source, TreeMap::new).put(index, targetId);
	}
	
	/**
	 * @param sourceEntity the object that owns the relation
	 * @return the targets stored for the given source, null if none was stored
	 */
	public Set<TRGTID> getRelatedTargetIdentifiers(SRC sourceEntity) {
		return new HashSet<>(getRelatedTargetIdentifiersPerIndex(sourceEntity).values());
	}
	
	/**
	 * @param sourceEntity the object that owns the relation
	 * @return the targets stored for the given source, sorted by their index, null if none was stored
	 */
	public Map<Integer /* index */, TRGTID> getRelatedTargetIdentifiersPerIndex(SRC sourceEntity) {
		return targetPerSource.get(sourceEntity);
	}
	
	public Map<SRC, Map<Integer /* index */, TRGTID>> getTargetIdPerIndexPerSource() {
		return targetPerSource;
	}
}
