package org.codefilarete.stalactite.engine;

import java.util.Set;

/**
 * Simple interface defining what's expected from selector 
 * 
 * @author Guillaume Mary
 */
public interface SelectExecutor<C, I> {
	
	Set<C> select(Iterable<I> ids);
}
