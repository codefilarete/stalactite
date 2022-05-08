package org.codefilarete.stalactite.engine.runtime;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface EntityConfiguredJoinedTablesPersister<C, I> extends ConfiguredJoinedTablesPersister<C, I>, EntityConfiguredPersister<C, I> {
	
	default Table getMainTable() {
		return getEntityJoinTree().getRoot().getTable();
	}
}
