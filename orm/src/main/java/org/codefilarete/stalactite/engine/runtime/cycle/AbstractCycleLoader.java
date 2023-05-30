package org.codefilarete.stalactite.engine.runtime.cycle;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.collection.Iterables;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractCycleLoader<SRC, TRGT, TRGTID> implements SelectListener<TRGT, TRGTID> {
	
	protected final EntityPersister<TRGT, TRGTID> targetPersister;
	
	/**
	 * Relations to be fulfilled.
	 * Stored by their path in the graph (please note that the only expected thing here is the unicity, being the path in the graph fills this goal
	 * and was overall choosen for debugging purpose)
	 */
	protected final Map<String, CascadeConfigurationResult<SRC, TRGT>> relations = new HashMap<>();
	
	protected final ThreadLocal<CycleLoadRuntimeContext<SRC, TRGT, TRGTID>> currentRuntimeContext = ThreadLocal.withInitial(CycleLoadRuntimeContext::new);
	
	protected AbstractCycleLoader(EntityPersister<TRGT, TRGTID> targetPersister) {
		this.targetPersister = targetPersister;
	}
	
	public void addRelation(String relationIdentifier, CascadeConfigurationResult<SRC, TRGT> configurationResult) {
		this.relations.put(relationIdentifier, configurationResult);
	}
	
	@Override
	public void beforeSelect(Iterable<TRGTID> ids) {
		this.currentRuntimeContext.get().beforeSelect();
	}
	
	@Override
	public void afterSelect(Set<? extends TRGT> result) {
		CycleLoadRuntimeContext<SRC, TRGT, TRGTID> runtimeContext = this.currentRuntimeContext.get();
		try {
			Set<TRGTID> targetIds = runtimeContext.giveIdentifiersToLoad();
			// NB: Iterable.forEach(Set.remove(..)) is a better performance way of doing Set.removeAll(Iterable) because :
			// - Iterable must be transformed as a List (with Iterables.collectToList for example)
			// - and algorithm of Set.remove(..) depends on List.contains() (if List is smaller than Set) which is not efficient
			result.forEach(o -> targetIds.remove(targetPersister.getId(o)));
			// WARN : this select may be recursive if targetPersister is the same as source one or owns a relation of same type as source one
			Set<TRGT> targets = targetPersister.select(targetIds);
			Map<TRGTID, TRGT> targetPerId = Iterables.map(targets, targetPersister::getId);
			relations.forEach((relationName, configurationResult) -> {
				EntityRelationStorage<SRC, TRGT, TRGTID> targetIdsPerSource = runtimeContext.getEntitiesToFulFill(relationName);
				if (targetIdsPerSource != null) {
					applyRelationToSource(targetIdsPerSource, configurationResult.getBeanRelationFixer(), targetPerId);
				}
			});
		} finally {
			runtimeContext.afterSelect();
			this.currentRuntimeContext.remove();
		}
	}
	
	protected abstract void applyRelationToSource(EntityRelationStorage<SRC, TRGT, TRGTID> targetIdsPerSource,
												  BeanRelationFixer<SRC, TRGT> beanRelationFixer,
												  Map<TRGTID, TRGT> targetPerId);
	
	@Override
	public void onSelectError(Iterable<TRGTID> ids, RuntimeException exception) {
		this.currentRuntimeContext.remove();
		throw exception;
	}
}
