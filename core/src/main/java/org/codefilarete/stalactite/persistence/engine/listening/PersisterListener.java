package org.codefilarete.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public interface PersisterListener<C, I> {
	
	void addInsertListener(InsertListener<C> insertListener);
	
	void addUpdateListener(UpdateListener<C> updateListener);
	
	void addSelectListener(SelectListener<C, I> selectListener);
	
	void addDeleteListener(DeleteListener<C> deleteListener);
	
	void addDeleteByIdListener(DeleteByIdListener<C> deleteListener);
	
}
