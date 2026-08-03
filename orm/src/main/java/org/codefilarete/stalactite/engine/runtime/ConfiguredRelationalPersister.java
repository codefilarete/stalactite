package org.codefilarete.stalactite.engine.runtime;

import org.codefilarete.stalactite.engine.EntityReadWriteExecutor;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface ConfiguredRelationalPersister<C, I, T extends Table<T>>
		extends ConfiguredPersister<C, I>, EntityReadWriteExecutor<C, I>, ConfiguredEntityReader<C, I, T> {
	
	default T getMainTable() {
		return (T) getEntityJoinTree().getRoot().getTable();
	}
}
