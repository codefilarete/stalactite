package org.codefilarete.stalactite.engine.runtime;

import org.codefilarete.stalactite.engine.VersioningStrategy;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.RollbackObserver;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Some code sharing around optimistic lock manager.
 * Needs that the {@link ConnectionProvider} is also a {@link RollbackObserver} (see constructors).
 * MVCC stands for MultiVersion Concurrency Control.
 */
abstract class AbstractRevertOnRollbackMVCC<E, V, T extends Table<T>> {
	
	protected final VersioningStrategy<E, V> versioningStrategy;
	protected final Column<T, V> versionColumn;
	protected final RollbackObserver rollbackObserver;
	
	/**
	 * Main constructor.
	 *
	 * @param versioningStrategy the entities upgrader
	 * @param versionColumn the column that stores the version
	 * @param rollbackObserver the {@link RollbackObserver} to revert upgrade when rollback happens
	 * @param <C> a {@link ConnectionProvider} that notifies rollback.
	 * {@link ConnectionProvider#giveConnection()} is not used here, simple mark to help understanding
	 */
	protected <C extends RollbackObserver & ConnectionProvider> AbstractRevertOnRollbackMVCC(VersioningStrategy<E, V> versioningStrategy,
																							 Column<T, V> versionColumn,
																							 RollbackObserver rollbackObserver) {
		this.versioningStrategy = versioningStrategy;
		this.versionColumn = versionColumn;
		this.rollbackObserver = rollbackObserver;
	}
}
