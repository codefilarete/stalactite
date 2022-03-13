package org.codefilarete.stalactite.engine.runtime;

import org.codefilarete.stalactite.engine.VersioningStrategy;
import org.codefilarete.tool.Reflections;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.RollbackObserver;

/**
 * Some code mutualisation around optimistic lock manager.
 * Needs that the {@link ConnectionProvider} is also a {@link RollbackObserver} (see constructors).
 * MVCC stands for MultiVersion Concurrency Control.
 */
abstract class AbstractRevertOnRollbackMVCC {
	
	protected final VersioningStrategy versioningStrategy;
	protected final Column versionColumn;
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
	protected <C extends RollbackObserver & ConnectionProvider> AbstractRevertOnRollbackMVCC(VersioningStrategy versioningStrategy, Column versionColumn, C rollbackObserver) {
		this.versioningStrategy = versioningStrategy;
		this.versionColumn = versionColumn;
		this.rollbackObserver = rollbackObserver;
	}
	
	/**
	 * Constructor that will check that the given {@link ConnectionProvider} is also a {@link RollbackObserver}, as the other constructor
	 * expects it. Will throw an {@link UnsupportedOperationException} if it is not the case
	 *
	 * @param versioningStrategy the entities upgrader
	 * @param versionColumn the column that stores the version
	 * @param rollbackObserver a {@link ConnectionProvider} that implements {@link RollbackObserver} to revert upgrade when rollback happens
	 * @throws UnsupportedOperationException if the given {@link ConnectionProvider} doesn't implements {@link RollbackObserver}
	 */
	protected AbstractRevertOnRollbackMVCC(VersioningStrategy versioningStrategy, Column versionColumn, ConnectionProvider rollbackObserver) {
		this.versioningStrategy = versioningStrategy;
		this.versionColumn = versionColumn;
		if (!(rollbackObserver instanceof RollbackObserver)) {
			throw new UnsupportedOperationException("Version control is only supported with " + Reflections.toString(ConnectionProvider.class)
					+ " that also implements " + Reflections.toString(RollbackObserver.class));
		}
		this.rollbackObserver = (RollbackObserver) rollbackObserver;
	}
	
}
