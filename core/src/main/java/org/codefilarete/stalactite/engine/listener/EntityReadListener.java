package org.codefilarete.stalactite.engine.listener;

/**
 * @author Guillaume Mary
 */
public interface EntityReadListener<C, I> {
	
	void addSelectListener(SelectListener<? extends C, I> selectListener);
	
}
