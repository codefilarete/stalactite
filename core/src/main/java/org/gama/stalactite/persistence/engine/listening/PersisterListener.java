package org.gama.stalactite.persistence.engine.listening;

/**
 * Simple class that centralize persistence event listening. Delegates listening to encapsulated instance.
 * 
 * @author Guillaume Mary
 */
public class PersisterListener<T> {
	
	private InsertListenerCollection<T> insertListener = new InsertListenerCollection<>();
	private UpdateRouglyListenerCollection<T> updateRouglyListener = new UpdateRouglyListenerCollection<>();
	private UpdateListenerCollection<T> updateListener = new UpdateListenerCollection<>();
	private DeleteListenerCollection<T> deleteListener = new DeleteListenerCollection<>();
	private SelectListenerCollection<T> selectListener = new SelectListenerCollection<>();
	
	public InsertListenerCollection<T> getInsertListener() {
		return insertListener;
	}
	
	public PersisterListener<T> addInsertListener(IInsertListener<T> insertListener) {
		this.insertListener.add(insertListener);
		return this;
	}
	
	public UpdateRouglyListenerCollection<T> getUpdateRouglyListener() {
		return updateRouglyListener;
	}
	
	public PersisterListener<T> addUpdateRouglyListener(IUpdateRouglyListener<T> updateRouglyListener) {
		this.updateRouglyListener.add(updateRouglyListener);
		return this;
	}
	
	public UpdateListenerCollection<T> getUpdateListener() {
		return updateListener;
	}
	
	public PersisterListener<T> addUpdateListener(IUpdateListener<T> updateListener) {
		this.updateListener.add(updateListener);
		return this;
	}
	
	public DeleteListenerCollection<T> getDeleteListener() {
		return deleteListener;
	}
	
	public PersisterListener<T> addDeleteListener(IDeleteListener<T> deleteListener) {
		this.deleteListener.add(deleteListener);
		return this;
	}
	
	public SelectListenerCollection<T> getSelectListener() {
		return selectListener;
	}
	
	public PersisterListener<T> addSelectListener(ISelectListener<T> selectListener) {
		this.selectListener.add(selectListener);
		return this;
	}
}
