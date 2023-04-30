package org.codefilarete.stalactite.engine.listener;

/**
 * @author Guillaume Mary
 */
public interface PersisterListener<C, I> {
	
	void addInsertListener(InsertListener<? extends C> insertListener);
	
	void addUpdateListener(UpdateListener<? extends C> updateListener);
	
	void addSelectListener(SelectListener<? extends C, I> selectListener);
	
	void addDeleteListener(DeleteListener<? extends C> deleteListener);
	
	void addDeleteByIdListener(DeleteByIdListener<? extends C> deleteListener);
	
}
