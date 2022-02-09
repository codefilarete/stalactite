package org.codefilarete.stalactite.sql;

/**
 * @author Guillaume Mary
 */
public interface RollbackObserver {
	
	void addRollbackListener(RollbackListener rollbackListener);
}
