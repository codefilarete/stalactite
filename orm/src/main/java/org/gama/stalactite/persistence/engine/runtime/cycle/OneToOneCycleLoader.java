package org.gama.stalactite.persistence.engine.runtime.cycle;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.gama.lang.Duo;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.IEntityPersister;
import org.gama.stalactite.persistence.engine.configurer.CascadeOneConfigurer.ConfigurationResult;
import org.gama.stalactite.persistence.engine.configurer.CascadeOneConfigurer.FirstPhaseCycleLoadListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.runtime.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.runtime.SecondPhaseRelationLoader;
import org.gama.stalactite.query.builder.IdentityMap;

public class OneToOneCycleLoader<SRC, TRGT, TRGTID> implements SelectListener<TRGT, TRGTID>, FirstPhaseCycleLoadListener<SRC, TRGTID> {
		
		private final IEntityPersister<TRGT, TRGTID> targetPersister;
		
		private final Map<String, ConfigurationResult<SRC, TRGT>> relations = new HashMap<>();
		
		private final ThreadLocal<Map<String, EntityRelationStorage>> currentLoadedRelations = new ThreadLocal<>();
		
		OneToOneCycleLoader(IEntityPersister<TRGT, TRGTID> targetPersister) {
			this.targetPersister = targetPersister;
		}
		
		public void addRelation(String name, ConfigurationResult<SRC, TRGT> configurationResult) {
			this.relations.put(name, configurationResult);
		}
		
		@Override
		public void onFirstPhaseRowRead(SRC src, TRGTID targetId) {
			this.relations.forEach((relationName, configurationResult) -> {
				if (configurationResult.getSourcePersister().getClassToPersist().isInstance(src) && !SecondPhaseRelationLoader.isDefaultValue(targetId)) {
					EntityRelationStorage entityRelationStorage = new EntityRelationStorage();
					currentLoadedRelations.get().put(relationName, entityRelationStorage);
					entityRelationStorage.put(src, targetId);
				}
			});
		}
		
		@Override
		public void beforeSelect(Iterable<TRGTID> ids) {
			if (this.currentLoadedRelations.get() == null) {
				this.currentLoadedRelations.set(new HashMap<>());
			}
		}
		
		@Override
		public void afterSelect(Iterable<? extends TRGT> result) {
			Set<TRGTID> targetIds = this.currentLoadedRelations.get().values().stream().flatMap(map -> map.getValues().stream()).collect(Collectors.toSet());
			// TODO: remove all target Ids already loaded to avoid infinite loop in case of entity cycle or two relations pointing to same entity, will avoid clones too
			targetIds.removeAll(Iterables.collectToList(result, targetPersister::getId));
			List<TRGT> targets = targetPersister.select(targetIds);
			final Map<TRGTID, TRGT> targetPerId = Iterables.map(targets, targetPersister::getId);
			relations.forEach((relationName, configurationResult) -> {
				EntityRelationStorage targetIdsPerSource = currentLoadedRelations.get().get(relationName);
				if (targetIdsPerSource != null) {
					applyRelationToSource(targetIdsPerSource.getKeys(), relationName, configurationResult.getBeanRelationFixer(), targetPerId);
				}
			});
		}
		
		private void applyRelationToSource(Iterable<? extends SRC> result, String name, BeanRelationFixer<SRC, TRGT> beanRelationFixer, Map<TRGTID, TRGT> targetPerId) {
			EntityRelationStorage entityRelationStorage = currentLoadedRelations.get().get(name);
			result.forEach(src -> {
				Set<TRGTID> trgtids = entityRelationStorage.get(src);
				if (trgtids != null) {
					trgtids.forEach(targetId -> beanRelationFixer.apply(src, targetPerId.get(targetId)));
				}
			});
		}
		
		@Override
		public void onError(Iterable<TRGTID> ids, RuntimeException exception) {
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
			
			private void put(SRC src, TRGTID targetIdentifier) {
				Duo<SRC, Set<TRGTID>> existingRelation = this.entityRelations.get(src);
				if (existingRelation == null) {
					existingRelation = new Duo<>(src, new HashSet<>());
					entityRelations.put(src, existingRelation);
				}
				existingRelation.getRight().add(targetIdentifier);
			}
			
			private Set<TRGTID> get(SRC src) {
				return this.entityRelations.get(src).getRight();
			}
			
			private Set<SRC> getKeys() {
				return Iterables.collect(this.entityRelations.values(), Duo::getLeft, HashSet::new);
			}
			
			private Set<TRGTID> getValues() {
				return this.entityRelations.values().stream().flatMap(duo -> duo.getRight().stream()).collect(Collectors.toSet());
			}
		}
	}