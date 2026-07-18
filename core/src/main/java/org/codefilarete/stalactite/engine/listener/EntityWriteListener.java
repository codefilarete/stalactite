package org.codefilarete.stalactite.engine.listener;

/**
 * @author Guillaume Mary
 */
public interface EntityWriteListener<C> {
	
	void addPersistListener(PersistListener<? extends C> persistListener);
	
	void addInsertListener(InsertListener<? extends C> insertListener);
	
	void addUpdateListener(UpdateListener<? extends C> updateListener);
	
	void addUpdateByIdListener(UpdateByIdListener<? extends C> updateByIdListener);
	
	void addDeleteListener(DeleteListener<? extends C> deleteListener);
	
	void addDeleteByIdListener(DeleteByIdListener<? extends C> deleteListener);
	
}
