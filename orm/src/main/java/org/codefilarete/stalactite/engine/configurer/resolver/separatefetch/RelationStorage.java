package org.codefilarete.stalactite.engine.configurer.resolver.separatefetch;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Temporary storage of the relation targets found during the first phase of a separate fetch, indexed by the object
 * that owns the relation (which may be the owning entity itself or its identifier, depending on the caller).
 *
 * @param <K> type of the object that owns the relation
 * @param <V> type of the stored targets : their identifier, or the entities themselves when they are loaded by
 * 					the very same query as the one that gives the relation
 * @author Guillaume Mary
 */
public class RelationStorage<K, V> extends AbstractRelationStorage<K, Set<V>> {
	
	public void add(K source, V targetId) {
		getOrCreate(source, HashSet::new).add(targetId);
	}
	
	/**
	 * @param source the object that owns the relation
	 * @return the targets stored for the given source, null if none was stored
	 */
	public Set<V> getRelatedTargetIdentifiers(K source) {
		return getRelatedTargets(source);
	}
	
	public Map<K, Set<V>> getTargetIdPerSource() {
		return targetPerSource;
	}
}
