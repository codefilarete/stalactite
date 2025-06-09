package org.codefilarete.stalactite.engine.runtime;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface ConfiguredRelationalPersister<C, I> extends ConfiguredPersister<C, I>, RelationalEntityPersister<C, I> {
	
	default <T extends Table<T>> T getMainTable() {
		return (T) getEntityJoinTree().getRoot().getTable();
	}
}
