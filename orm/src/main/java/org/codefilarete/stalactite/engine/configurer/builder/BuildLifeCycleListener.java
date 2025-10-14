package org.codefilarete.stalactite.engine.configurer.builder;

import org.codefilarete.stalactite.engine.PersisterRegistry;

/**
 * Contract triggered after a persister has been built, made to complete some more configuration.
 */
public interface BuildLifeCycleListener {
	
	/**
	 * Invoked after main entity persister creation. At this stage all persisters involved in the entity graph
	 * are expected to be available in the {@link PersisterRegistry}
	 */
	void afterBuild();
	
	/**
	 * Invoked after all {@link #afterBuild()} of all the registered {@link BuildLifeCycleListener}s have been called.
	 * Made for finalization cases.
	 */
	void afterAllBuild();
}
