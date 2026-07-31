package org.codefilarete.stalactite.engine.configurer.resolver.separatefetch;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Temporary storage of the relation targets found during the first phase of a separate fetch, indexed by the object
 * that owns the relation (which may be the owning entity itself or its identifier, depending on the caller).
 *
 * @param <SRC> type of the object that owns the relation
 * @param <TRGTID> type of the stored targets : their identifier, or the entities themselves when they are loaded by
 * 					the very same query as the one that gives the relation
 * @author Guillaume Mary
 */
public class RelationStorage<SRC, TRGTID> {
	
	private final Map<SRC, Set<TRGTID>> targetIdPerSource = new HashMap<>();
	
	public void add(SRC sourceEntity, TRGTID targetId) {
		targetIdPerSource.computeIfAbsent(sourceEntity, k -> new HashSet<>()).add(targetId);
	}
	
	/**
	 * @param sourceEntity the object that owns the relation
	 * @return the targets stored for the given source, null if none was stored
	 */
	public Set<TRGTID> get(SRC sourceEntity) {
		return targetIdPerSource.get(sourceEntity);
	}
	
	public Map<SRC, Set<TRGTID>> getTargetIdPerSource() {
		return targetIdPerSource;
	}
}
