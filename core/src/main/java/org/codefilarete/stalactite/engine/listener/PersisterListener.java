package org.codefilarete.stalactite.engine.listener;

/**
 * @author Guillaume Mary
 */
public interface PersisterListener<C, I> extends EntityWriteListener<C>, EntityReadListener<C, I> {
	
}
