package org.codefilarete.stalactite.engine;

import java.util.Collections;

import org.codefilarete.tool.collection.Arrays;

/**
 * @author Guillaume Mary
 */
public interface InsertExecutor<C> {
	
	void insert(Iterable<? extends C> entities);
	
	default void insert(C entity) {
		insert(Collections.singletonList(entity));
	}
	
	default void insert(C... entities) {
		insert(Arrays.asList(entities));
	}
}
