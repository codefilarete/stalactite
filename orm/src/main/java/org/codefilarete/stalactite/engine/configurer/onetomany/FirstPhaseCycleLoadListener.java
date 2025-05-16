package org.codefilarete.stalactite.engine.configurer.onetomany;

/**
 * Object invoked on row read
 *
 * @param <SRC>
 * @param <TRGTID>
 */
@FunctionalInterface
public interface FirstPhaseCycleLoadListener<SRC, TRGTID> {
	
	void onFirstPhaseRowRead(SRC src, TRGTID targetId);
	
}
