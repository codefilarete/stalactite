package org.gama.stalactite.persistence.engine.runtime.cycle;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.gama.lang.Duo;
import org.gama.lang.collection.Iterables;
import org.gama.lang.trace.ModifiableInt;
import org.gama.stalactite.persistence.engine.IEntityPersister;
import org.gama.stalactite.persistence.engine.configurer.CascadeOneConfigurer.ConfigurationResult;
import org.gama.stalactite.persistence.engine.configurer.CascadeOneConfigurer.FirstPhaseCycleLoadListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.runtime.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.runtime.SecondPhaseRelationLoader;
import org.gama.stalactite.query.builder.IdentityMap;

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
	
	private final IEntityPersister<TRGT, TRGTID> targetPersister;
	
	/**
	 * Relations to be fulfilled.
	 * Stored by their path in the graph (please note that the only expected thing here is the unicity, being the path in the graph fills this goal
	 * and was overall choosen for debugging purpose)
	 */
	private final Map<String, ConfigurationResult<SRC, TRGT>> relations = new HashMap<>();
	
	private final ThreadLocal<RuntimeContext> currentRuntimeContext = ThreadLocal.withInitial(RuntimeContext::new);
	
	OneToOneCycleLoader(IEntityPersister<TRGT, TRGTID> targetPersister) {
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
		Set<TRGTID> targetIds = this.currentRuntimeContext.get().giveIdentifiersToLoad();
		// NB: Iterable.forEach(Set.remove(..)) is a better performance way of doing Set.removeAll(Iterable) because :
		// - Iterable must be transformed as a List (with Iterables.collectToList for example)
		// - and algorithm of Set.remove(..) depends on List.contains() (if List is smaller than Set) which is not efficient
		result.forEach(o -> targetIds.remove(targetPersister.getId(o)));
		List<TRGT> targets = targetPersister.select(targetIds);
		final Map<TRGTID, TRGT> targetPerId = Iterables.map(targets, targetPersister::getId);
		relations.forEach((relationName, configurationResult) -> {
			EntityRelationStorage targetIdsPerSource = currentRuntimeContext.get().getEntitiesToFulFill(relationName);
			if (targetIdsPerSource != null) {
				applyRelationToSource(targetIdsPerSource, configurationResult.getBeanRelationFixer(), targetPerId);
			}
		});
		this.currentRuntimeContext.get().afterSelect();
	}
	
	private void applyRelationToSource(EntityRelationStorage relationStorage, BeanRelationFixer<SRC, TRGT> beanRelationFixer, Map<TRGTID, TRGT> targetPerId) {
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
	
	/**
	 * Mapping between left part of a relation and identifiers of its right part
	 */
	private class EntityRelationStorage {
		
		/**
		 * Made as an {@link IdentityMap} to avoid hashCode entity implementation to badly influence our entity lookup because it might vary
		 * during entity fulfillment
		 */
		private final IdentityMap<SRC, Duo<SRC, Set<TRGTID>>> entityRelations = new IdentityMap<>();
		
		private void addRelationToInitialize(SRC src, TRGTID targetIdentifier) {
			Duo<SRC, Set<TRGTID>> existingRelation = this.entityRelations.get(src);
			if (existingRelation == null) {
				existingRelation = new Duo<>(src, new HashSet<>());
				entityRelations.put(src, existingRelation);
			}
			existingRelation.getRight().add(targetIdentifier);
		}
		
		private Set<TRGTID> getRelationToInitialize(SRC src) {
			return this.entityRelations.get(src).getRight();
		}
		
		private Set<SRC> getEntitiesToFulFill() {
			return Iterables.collect(this.entityRelations.values(), Duo::getLeft, HashSet::new);
		}
		
		private Set<TRGTID> getIdentifiersToLoad() {
			return this.entityRelations.values().stream().flatMap(duo -> duo.getRight().stream()).collect(Collectors.toSet());
		}
	}
	
	private class RuntimeContext {
		
		private final ModifiableInt selectInvokationCount = new ModifiableInt(0);
		
		private final Map<String, EntityRelationStorage> currentLoadedRelations = new HashMap<>(); 
		
		public void addRelationToInitialize(String relationName, SRC src, TRGTID targetId) {
			this.currentLoadedRelations.computeIfAbsent(relationName, k -> new EntityRelationStorage())
					.addRelationToInitialize(src, targetId);
		}
		
		public Set<TRGTID> giveIdentifiersToLoad() {
			return this.currentLoadedRelations.values().stream().flatMap(map -> map.getIdentifiersToLoad().stream()).collect(Collectors.toSet());
		}
		
		public EntityRelationStorage getEntitiesToFulFill(String relationName) {
			return this.currentLoadedRelations.get(relationName);
		}
		
		public void beforeSelect() {
			this.selectInvokationCount.increment();
		}
		
		public void afterSelect() {
			this.selectInvokationCount.decrement();
			if (this.selectInvokationCount.getValue() == 0) {
				clear();
			}
		}
		
		public void clear() {
			this.selectInvokationCount.reset(0);
			this.currentLoadedRelations.clear();
		}
	}
}