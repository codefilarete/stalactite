package org.gama.sql;

import java.sql.Savepoint;
import java.util.Collection;

/**
 * @author Guillaume Mary
 */
public class RollbackListenerCollection implements RollbackListener {
	
	private Collection<RollbackListener> rollbackListeners;
	
	public RollbackListenerCollection(Collection<RollbackListener> rollbackListeners) {
		this.rollbackListeners = rollbackListeners;
	}
	
	@Override
	public void beforeRollback() {
		rollbackListeners.forEach(RollbackListener::beforeRollback);
	}
	
	@Override
	public void afterRollback() {
		rollbackListeners.forEach(RollbackListener::afterRollback);
	}
	
	@Override
	public void beforeRollback(Savepoint savepoint) {
		rollbackListeners.forEach(l -> l.beforeRollback(savepoint));
	}
	
	@Override
	public void afterRollback(Savepoint savepoint) {
		rollbackListeners.forEach(l -> l.afterRollback(savepoint));
	}
}
