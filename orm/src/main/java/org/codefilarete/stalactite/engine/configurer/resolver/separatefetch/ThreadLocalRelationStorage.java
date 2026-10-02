package org.codefilarete.stalactite.engine.configurer.resolver.separatefetch;

import java.util.Collection;
import java.util.Set;

/**
 * {@link ThreadLocal} wrapper around a {@link RelationStorage} : allows the entities loaded by a separate-fetch query
 * to be gathered while the result set is read, then to be sewn onto the entity that owns the relation once the whole
 * result set is consumed. Made as a {@link ThreadLocal} to support concurrent selects.
 *
 * @param <K> identifier type of the entity that owns the relation
 * @param <V> type of the entities of the relation
 * @author Guillaume Mary
 * @see AssociationTableLoader
 */
public class ThreadLocalRelationStorage<K, V> extends ThreadLocalStorage<K, Set<V>, RelationStorage<K, V>> {
	
	@Override
	protected RelationStorage<K, V> newStorage() {
		return new RelationStorage<>();
	}
	
	public void storeRelation(K source, V target) {
		getStorage().add(source, target);
	}
	
	/**
	 * @param source identifier of the entity that owns the relation
	 * @return the entities found for the given source, null if none was found
	 */
	public Collection<V> giveRelatedEntities(K source) {
		return getStorage().getRelatedTargets(source);
	}
}
