package org.gama.stalactite.persistence.engine.runtime;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;

import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.ISelectExecutor;
import org.gama.stalactite.persistence.engine.listening.SelectListener;

/**
 * @author Guillaume Mary
 */
public class SecondPhaseRelationLoader<SRC, TRGT, ID> implements SelectListener<SRC, ID> {
	
	private final BeanRelationFixer<SRC, TRGT> beanRelationFixer;
	private final ThreadLocal<Queue<Set<RelationIds<Object, Object, Object>>>> relationIdsHolder;
	
	public SecondPhaseRelationLoader(BeanRelationFixer<SRC, TRGT> beanRelationFixer, ThreadLocal<Queue<Set<RelationIds<Object, Object, Object>>>> relationIdsHolder) {
		this.beanRelationFixer = beanRelationFixer;
		this.relationIdsHolder = relationIdsHolder;
	}
	
	@Override
	public void beforeSelect(Iterable<ID> ids) {
		Queue<Set<RelationIds<Object, Object, Object>>> existingSet = relationIdsHolder.get();
		if (existingSet == null) {
			existingSet = new ArrayDeque<>();
			relationIdsHolder.set(existingSet);
		}
		existingSet.add(new HashSet<>());
	}
	
	@Override
	public void afterSelect(Iterable<? extends SRC> result) {
		selectTargetEntities(result);
		relationIdsHolder.remove();
	}
	
	/**
	 * Mainly created to clarify types with TRGTID as parameter
	 *
	 * @param sourceEntities main entities, those that have the relation
	 * @param <TRGTID> target identifier type
	 */
	private <TRGTID> void selectTargetEntities(Iterable<? extends SRC> sourceEntities) {
		Map<ISelectExecutor<TRGT, TRGTID>, Set<TRGTID>> selectsToExecute = new HashMap<>();
		Map<ISelectExecutor<TRGT, TRGTID>, Function<TRGT, TRGTID>> idAccessors = new HashMap<>();
		Map<SRC, Set<TRGTID>> targetIdPerSource = new HashMap<>();
		Set<RelationIds<SRC, TRGT, TRGTID>> relationIds = ((Queue<Set<RelationIds<SRC, TRGT, TRGTID>>>) (Queue) relationIdsHolder.get()).poll();
		// we remove null targetIds (Target Ids may be null if relation is nullified) because
		// - selecting entities with null id is non-sensence
		// - it prevents from generating SQL "in ()" which is invalid
		// - it prevents from NullPointerException when applying target to source
		relationIds.stream().filter(r -> !isDefaultValue(r.getTargetId())).forEach(r -> {
			idAccessors.putIfAbsent(r.getSelectExecutor(), r.getIdAccessor());
			targetIdPerSource.computeIfAbsent(r.getSource(), k -> new HashSet<>()).add(r.getTargetId());
			selectsToExecute.computeIfAbsent(r.getSelectExecutor(), k -> new HashSet<>()).add(r.getTargetId());
		});
		
		// we load target entities from their ids, and map them per their loader
		Map<ISelectExecutor, List<TRGT>> targetsPerSelector = new HashMap<>();
		selectsToExecute.forEach((selectExecutor, ids) -> targetsPerSelector.put(selectExecutor, selectExecutor.select(ids)));
		// then we apply them onto their source entities, to remember which target applies to which source, we use target id
		Map<TRGTID, TRGT> targetPerId = new HashMap<>();
		targetsPerSelector.forEach((selector, loadedTargets) -> targetPerId.putAll(Iterables.map(loadedTargets, idAccessors.get(selector))));
		sourceEntities.forEach(src -> Nullable.nullable(targetIdPerSource.get(src))    // source may not have targetIds if relation if null
				.invoke(targetIds -> targetIds.forEach(targetId -> beanRelationFixer.apply(src, targetPerId.get(targetId)))));
	}
	
	public static boolean isDefaultValue(Object value) {
		return value == null || Reflections.PRIMITIVE_DEFAULT_VALUES.get(value.getClass()) == value;
	}
	
	@Override
	public void onError(Iterable<ID> ids, RuntimeException exception) {
		relationIdsHolder.remove();
	}
}
