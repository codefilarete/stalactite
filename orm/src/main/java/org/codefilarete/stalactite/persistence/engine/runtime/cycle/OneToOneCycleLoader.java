package org.codefilarete.stalactite.persistence.engine.runtime.cycle;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.stalactite.persistence.engine.EntityPersister;
import org.codefilarete.stalactite.persistence.engine.configurer.CascadeOneConfigurer.ConfigurationResult;
import org.codefilarete.stalactite.persistence.engine.configurer.CascadeOneConfigurer.FirstPhaseCycleLoadListener;
import org.codefilarete.stalactite.persistence.engine.listening.SelectListener;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.persistence.engine.runtime.SecondPhaseRelationLoader;

/**
 * Loader in case of same entity type present in entity graph as child of itself : A.b -> B.c -> C.a
 * Sibling is not the purpose of this class.
 * 
 * Implemented as such :
 * - very first query reads cycling entity type identifiers
 * - a secondary query is executed to load subgraph with above identifiers
 * 
 * @param <SRC> type of root entity graph
 * @param <TRGT> cycling entity type (the one to be loaded)
 * @param <TRGTID> cycling entity identifier type
 *     
 */
public class OneToOneCycleLoader<SRC, TRGT, TRGTID> implements SelectListener<TRGT, TRGTID>, FirstPhaseCycleLoadListener<SRC, TRGTID> {
	
	private final EntityPersister<TRGT, TRGTID> targetPersister;
	
	/**
	 * Relations to be fulfilled.
	 * Stored by their path in the graph (please note that the only expected thing here is the unicity, being the path in the graph fills this goal
	 * and was overall choosen for debugging purpose)
	 */
	private final Map<String, ConfigurationResult<SRC, TRGT>> relations = new HashMap<>();
	
	private final ThreadLocal<CycleLoadRuntimeContext<SRC, TRGT, TRGTID>> currentRuntimeContext = ThreadLocal.withInitial(CycleLoadRuntimeContext::new);
	
	OneToOneCycleLoader(EntityPersister<TRGT, TRGTID> targetPersister) {
		this.targetPersister = targetPersister;
	}
	
	public void addRelation(String relationIdentifier, ConfigurationResult<SRC, TRGT> configurationResult) {
		this.relations.put(relationIdentifier, configurationResult);
	}
	
	@Override
	public void beforeSelect(Iterable<TRGTID> ids) {
		this.currentRuntimeContext.get().beforeSelect();
	}
	
	/**
	 * Implemented to read very first identifiers of source type
	 */
	@Override
	public void onFirstPhaseRowRead(SRC src, TRGTID targetId) {
		if (!SecondPhaseRelationLoader.isDefaultValue(targetId)) {
			this.relations.forEach((relationName, configurationResult) -> {
				if (configurationResult.getSourcePersister().getClassToPersist().isInstance(src)) {
					this.currentRuntimeContext.get().addRelationToInitialize(relationName, src, targetId);
				}
			});
		}
	}
	
	@Override
	public void afterSelect(Iterable<? extends TRGT> result) {
		CycleLoadRuntimeContext<SRC, TRGT, TRGTID> runtimeContext = this.currentRuntimeContext.get();
		Set<TRGTID> targetIds = runtimeContext.giveIdentifiersToLoad();
		// NB: Iterable.forEach(Set.remove(..)) is a better performance way of doing Set.removeAll(Iterable) because :
		// - Iterable must be transformed as a List (with Iterables.collectToList for example)
		// - and algorithm of Set.remove(..) depends on List.contains() (if List is smaller than Set) which is not efficient
		result.forEach(o -> targetIds.remove(targetPersister.getId(o)));
		// WARN : this select may be recursive if targetPersister is the same as source one or owns a relation of same type as source one
		List<TRGT> targets = targetPersister.select(targetIds);
		final Map<TRGTID, TRGT> targetPerId = Iterables.map(targets, targetPersister::getId);
		relations.forEach((relationName, configurationResult) -> {
			EntityRelationStorage<SRC, TRGT, TRGTID> targetIdsPerSource = runtimeContext.getEntitiesToFulFill(relationName);
			if (targetIdsPerSource != null) {
				applyRelationToSource(targetIdsPerSource, configurationResult.getBeanRelationFixer(), targetPerId);
			}
		});
		runtimeContext.afterSelect();
	}
	
	private void applyRelationToSource(EntityRelationStorage<SRC, TRGT, TRGTID> relationStorage, BeanRelationFixer<SRC, TRGT> beanRelationFixer, Map<TRGTID, TRGT> targetPerId) {
		relationStorage.getEntitiesToFulFill().forEach(src -> {
			Set<TRGTID> trgtids = relationStorage.getRelationToInitialize(src);
			if (trgtids != null) {
				trgtids.forEach(targetId -> beanRelationFixer.apply(src, targetPerId.get(targetId)));
			}
		});
	}
	
	@Override
	public void onError(Iterable<TRGTID> ids, RuntimeException exception) {
		throw exception;
	}
	
}