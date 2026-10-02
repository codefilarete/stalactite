package org.codefilarete.stalactite.engine.configurer.resolver.separatefetch;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public abstract class AbstractRelationStorage<K, V> {
	
	protected final Map<K, V> targetPerSource = new HashMap<>();
	
	protected Map<K, V> getTargetPerSource() {
		return targetPerSource;
	}
	
	protected V getOrCreate(K source, Supplier<V> factory) {
		return targetPerSource.computeIfAbsent(source, k -> factory.get());
	}
	
	protected V getRelatedTargets(K source) {
		return targetPerSource.get(source);
	}
}
