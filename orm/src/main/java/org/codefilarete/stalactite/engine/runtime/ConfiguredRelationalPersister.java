package org.codefilarete.stalactite.engine.runtime;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface ConfiguredRelationalPersister<C, I> extends ConfiguredPersister<C, I>, RelationalEntityPersister<C, I> {
	
	default Table getMainTable() {
		return getEntityJoinTree().getRoot().getTable();
	}
}
