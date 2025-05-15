package org.codefilarete.stalactite.engine.runtime.cycle;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.onetoone.FirstPhaseCycleLoadListener;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.trace.MutableInt;

/**
 * Abstraction to handle graph loading while it contains a cycle in mapping definition.
 * Mechanism is made of recursion through {@link SelectListener} combined with {@link FirstPhaseCycleLoadListener}, which actually triggers
 * very first iteration while whole graph is loaded.
 * This class must be Thread-safe because its a singleton from a configuration point of view, and, as a {@link SelectListener}, it is invoked
 * by several threads.
 * 
 * @author Guillaume Mary
 */
public abstract class AbstractCycleLoader<SRC, TRGT, TRGTID> implements SelectListener<TRGT, TRGTID> {
	
	protected final EntityPersister<TRGT, TRGTID> targetPersister;
	
	/**
	 * Relations to be fulfilled.
	 * Stored by their path in the graph (please note that the only expected thing here is the uniqueness, being the path in the graph fills this goal
	 * and was overall chosen for debugging purpose)
	 */
	protected final Map<String, CascadeConfigurationResult<SRC, TRGT>> relations = new HashMap<>();
	
	protected final ThreadContext<CycleLoadRuntimeContext<SRC, TRGTID>> currentRuntimeContext = new ThreadContext<>(CycleLoadRuntimeContext::new);
	
	/**
	 * Stores loaded entities. Made to avoid loading too many entities and making job done twice, as well as to avoid
	 * infinite loop if database contains cycling (non tree) data
	 */
	@VisibleForTesting
	final ThreadContext<Set<TRGTID>> currentlyLoadedEntityIdsInCycle = new ThreadContext<>(HashSet::new);
	
	/**
	 * Helper to detect end of beforeSelect / select / afterSelect loop. Made to clear other ThreadLocal fields.
	 */
	@VisibleForTesting
	final ThreadContext<MutableInt> currentCycleCount = new ThreadContext<>(MutableInt::new);
	
	protected AbstractCycleLoader(EntityPersister<TRGT, TRGTID> targetPersister) {
		this.targetPersister = targetPersister;
	}
	
	public void addRelation(String relationIdentifier, CascadeConfigurationResult<SRC, TRGT> configurationResult) {
		this.relations.put(relationIdentifier, configurationResult);
	}
	
	@Override
	public void beforeSelect(Iterable<TRGTID> ids) {
		currentCycleCount.get().increment();
	}
	
	@Override
	public void afterSelect(Set<? extends TRGT> result) {
		Set<TRGTID> alreadyLoadedEntities = this.currentlyLoadedEntityIdsInCycle.get();
		result.forEach(entity -> alreadyLoadedEntities.add(targetPersister.getId(entity)));
		
		CycleLoadRuntimeContext<SRC, TRGTID> runtimeContext = this.currentRuntimeContext.get();
		// We clear context field to avoid keeping previous result in memory which may generate some infinite loop
		// when this.targetPersister is same instance as the one that triggered this method
		// Moreover we only need this.currentRuntimeContext to be accessible from outside and ThreadSafe, acting as a
		// data feeder for this method, so it is no more useful after that current method has read its content. 
		this.currentRuntimeContext.remove();
		
		// we remove already loaded elements
		Set<TRGTID> identifiersToLoad = runtimeContext.giveIdentifiersToLoad();
		identifiersToLoad.removeAll(alreadyLoadedEntities);
		
		if (!identifiersToLoad.isEmpty()) {
			// WARN : this select will be recursive in cycle case : if targetPersister is same as source one or owns a relation of same type as source one
			// Hence, targetPersister.select(..) will trigger this afterSelect() method again, so code right after select(..) will be executed
			Set<TRGT> targets = targetPersister.select(identifiersToLoad);
			Map<TRGTID, TRGT> targetPerId = Iterables.map(targets, targetPersister::getId);
			relations.forEach((relationName, configurationResult) -> {
				EntityRelationStorage<SRC, TRGTID> targetIdsPerSource = runtimeContext.getEntitiesToFulFill(relationName);
				if (targetIdsPerSource != null) {
					applyRelationToSource(targetIdsPerSource, configurationResult.getBeanRelationFixer(), targetPerId);
				}
			});
		}
		if (currentCycleCount.get().decrement() == 0) {
			this.currentlyLoadedEntityIdsInCycle.remove();
			this.currentRuntimeContext.remove();
			this.currentCycleCount.remove();
		}
	}
	
	protected abstract void applyRelationToSource(EntityRelationStorage<SRC, TRGTID> targetIdsPerSource,
												  BeanRelationFixer<SRC, TRGT> beanRelationFixer,
												  Map<TRGTID, TRGT> targetPerId);
	
	@Override
	public void onSelectError(Iterable<TRGTID> ids, RuntimeException exception) {
		this.currentRuntimeContext.remove();
		throw exception;
	}
	
	/**
	 * Wrapper around a {@link ThreadLocal} made to add {@link #isPresent()} to it. Else it is not possible to know
	 * if a {@link ThreadLocal} contains value while it has been created with {@link ThreadLocal#withInitial(Supplier)}.
	 * Please note that {@link #isPresent()} is only necessary for test assertion.
	 * 
	 * @param <T> stored object type
	 */
	@VisibleForTesting
	static class ThreadContext<T> {
		
		private final ThreadLocal<T> store = new ThreadLocal<>();
		
		private final Supplier<T> valueInitializer;
		
		public ThreadContext() {
			this(null);
		}
		
		public ThreadContext(Supplier<T> valueInitializer) {
			this.valueInitializer = valueInitializer;
		}
		
		protected void set(T t) {
			store.set(t);
		}
		
		public T get() {
			if (!isPresent() && valueInitializer != null) {
				store.set(valueInitializer.get());
			}
			return store.get();
		}
		
		public boolean isPresent() {
			return store.get() != null;
		}
		
		public void remove() {
			store.remove();
		}
	}
}
