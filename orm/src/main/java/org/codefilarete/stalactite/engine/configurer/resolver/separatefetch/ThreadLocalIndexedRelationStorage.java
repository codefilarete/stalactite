package org.codefilarete.stalactite.engine.configurer.resolver.separatefetch;

import java.util.Map;

/**
 * {@link ThreadLocal} wrapper around an {@link IndexedRelationStorage} : allows the entities loaded by a separate-fetch
 * query to be gathered, per index, while the result set is read, then to be sewn in order onto the entity that owns the
 * relation once the whole result set is consumed. Made as a {@link ThreadLocal} to support concurrent selects.
 *
 * @param <K> identifier type of the entity that owns the relation
 * @param <V> type of the entities of the relation
 * @author Guillaume Mary
 * @see AssociationTableLoader
 */
public class ThreadLocalIndexedRelationStorage<K, V> extends ThreadLocalStorage<K, Map<Integer, V>, IndexedRelationStorage<K, V>> {
	
	@Override
	protected IndexedRelationStorage<K, V> newStorage() {
		return new IndexedRelationStorage<>();
	}
	
	public void storeRelation(K source, int index, V target) {
		getStorage().add(source, target, index);
	}
	
	/**
	 * @param source identifier of the entity that owns the relation
	 * @return the entities found for the given source, sorted by their index, null if none was found
	 */
	public Map<Integer, V> giveRelatedEntities(K source) {
		return getStorage().getRelatedTargetIdentifiersPerIndex(source);
	}
}
