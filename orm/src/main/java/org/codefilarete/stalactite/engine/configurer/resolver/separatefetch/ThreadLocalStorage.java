package org.codefilarete.stalactite.engine.configurer.resolver.separatefetch;

/**
 * Base class for the in-memory storages used by the second phase of a separate-fetch load : entities are gathered
 * while the result set is read, then sewn onto the entity that owns the relation once the whole result set is
 * consumed. Made as a {@link ThreadLocal} to support concurrent selects.
 *
 * @param <S> the storage type
 * @author Guillaume Mary
 * @see SecondPhaseSelectListener
 */
public abstract class ThreadLocalStorage<S> {
	
	private final ThreadLocal<S> storage = new ThreadLocal<>();
	
	protected S getStorage() {
		return storage.get();
	}
	
	/**
	 * @return a new empty storage, called by {@link #init()}
	 */
	protected abstract S newStorage();
	
	/**
	 * Makes a new storage available to the current thread. Expected to be called before the second-phase query is run.
	 */
	public void init() {
		this.storage.set(newStorage());
	}
	
	/**
	 * Releases the storage of the current thread. Expected to be called once the relations are sewn, whatever the
	 * second-phase query outcome is, to prevent any memory leak.
	 */
	public void clear() {
		this.storage.remove();
	}
	
}
