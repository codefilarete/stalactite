package org.codefilarete.stalactite.engine.runtime;

import org.codefilarete.stalactite.engine.EntityReadExecutor;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;

public interface ConfiguredEntityReader<C, I> extends EntityReadExecutor<C, I> {
	
	EntityJoinTree<C, I> getEntityJoinTree();
}
