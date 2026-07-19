package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.Map;

import org.codefilarete.stalactite.engine.EntityReadWriteExecutor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * Small callback contract given to {@link SkeletonAggregateResolver} so it can push all the
 * {@link EntityWriteExecutor} it creates while building a persister subtree : the subtree root, its ancestor
 * persisters (inheritance mapping) and its extra-table persisters.
 * <p>
 * {@link AggregateResolver} owns the actual instance of this contract and passes it down to every relation
 * resolver (which simply forwards it to {@link SkeletonAggregateResolver#buildPersister(org.codefilarete.stalactite.engine.configurer.model.Entity, CreatedPersisterCollector)}),
 * keeping it as the single place that is aware of every persister created while resolving an aggregate, while
 * {@link SkeletonAggregateResolver} stays the single place responsible for actually creating them.
 *
 * @author Guillaume Mary
 */
public class CreatedPersisterCollector<C, I> {
	
	/** Main created persister */
	private EntityReadWriteExecutor<C, I> persister;
	
	/**
	 * Ancestor persisters, ordered from the closest to the furthest ancestor, of the main target one
	 */
	private final KeepOrderSet<EntityWriteExecutor<? super C, I>> ancestorPersisters = new KeepOrderSet<>();
	
	/**
	 * Extra table persisters per ancestor persister or main target persister, expected to be joined to the main persister.
	 */
	private final KeepOrderMap<EntityWriteExecutor<? super C, I>, KeepOrderSet<EntityWriteExecutor<? super C, I>>> extraPersisters = new KeepOrderMap<>();
	
	public EntityReadWriteExecutor<C, I> getPersister() {
		return persister;
	}
	
	public void setPersister(EntityReadWriteExecutor<C, I> persister) {
		this.persister = persister;
	}
	
	public KeepOrderSet<EntityWriteExecutor<C, I>> getAncestorPersisters() {
		return (KeepOrderSet) ancestorPersisters;
	}
	
	public void addAncestorPersister(EntityWriteExecutor<? super C, I> ancestorPersister) {
		this.ancestorPersisters.add(ancestorPersister);
	}
	
	public Map<EntityWriteExecutor<C,I>, KeepOrderSet<EntityWriteExecutor<C,I>>> getExtraPersisters() {
		// Cast issue: we don't need the "? super C" to bother caller because actually it doesn't really stand for
		// a super type, it's much more for function calling (yes, strange Java generics...)
		return (Map) extraPersisters;
	}
	
	/**
	 * Called by {@link SkeletonAggregateResolver} each time it creates an extra-table persister.
	 * 
	 * @param persister the persister to which the extra-table persister belongs (main one or ancestor one)
	 * @param extraTablePersister the extra-table persister that was just created
	 */
	public void addExtraTablePersister(EntityWriteExecutor<? super C, I> persister, EntityWriteExecutor<? super C, I> extraTablePersister) {
		this.extraPersisters.computeIfAbsent(persister, k -> new KeepOrderSet<>()).add(extraTablePersister);
	}
}
