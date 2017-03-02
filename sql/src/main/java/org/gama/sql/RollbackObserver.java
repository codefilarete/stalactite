package org.gama.sql;

/**
 * @author Guillaume Mary
 */
public interface RollbackObserver {
	
	void addRollbackListener(RollbackListener rollbackListener);
}
