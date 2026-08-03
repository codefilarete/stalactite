package org.codefilarete.stalactite.engine.runtime;

import org.codefilarete.stalactite.engine.EntityReadExecutor;
import org.codefilarete.stalactite.engine.EntitySelector;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public interface ConfiguredEntityReader<C, I, T extends Table<T>> extends EntityReadExecutor<C, I>, EntitySelector<C> {
	
	EntityJoinTree<C, I> getEntityJoinTree();
	
	@Override
	EntityMapping<C, I, T> getMapping();
	
	SelectListener<C, I> getSelectListener();
}
