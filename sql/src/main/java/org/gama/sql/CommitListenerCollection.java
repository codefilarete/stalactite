package org.gama.sql;

import java.util.Collection;

/**
 * @author Guillaume Mary
 */
public class CommitListenerCollection implements CommitListener {
	
	private Collection<CommitListener> commitListeners;
	
	public CommitListenerCollection(Collection<CommitListener> commitListeners) {
		this.commitListeners = commitListeners;
	}
	
	@Override
	public void beforeCommit() {
		commitListeners.forEach(CommitListener::beforeCommit);
	}
	
	@Override
	public void afterCommit() {
		commitListeners.forEach(CommitListener::afterCommit);
	}
	
	@Override
	public boolean isTemporary() {
		return false;
	}
	
}
