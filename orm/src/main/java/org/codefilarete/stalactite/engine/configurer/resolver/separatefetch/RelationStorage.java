package org.codefilarete.stalactite.engine.configurer.resolver.separatefetch;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RelationStorage<SRC, TRGTID> {
	
	private final Map<SRC, Set<TRGTID>> targetIdPerSource = new HashMap<>();
	
	void add(SRC sourceEntity, TRGTID targetId) {
		targetIdPerSource.computeIfAbsent(sourceEntity, k -> new HashSet<>()).add(targetId);
	}
	
	public Map<SRC, Set<TRGTID>> getTargetIdPerSource() {
		return targetIdPerSource;
	}
}
