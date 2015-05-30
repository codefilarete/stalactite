package org.gama.stalactite.persistence.engine.listening;

import org.gama.stalactite.persistence.engine.NoopDeleteListener;

/**
 * Simple class that centralize persistence event listening. Delegates listening to encapsulated instance.
 * 
 * @author Guillaume Mary
 */
public class PersisterListener<T> {
	
	private IInsertListener<T> insertListener = new NoopInsertListener<>();
	private IUpdateRouglyListener<T> updateRouglyListener = new NoopUpdateRouglyListener<>();
	private IUpdateListener<T> updateListener = new NoopUpdateListener<>();
	private IDeleteListener<T> deleteListener = new NoopDeleteListener<>();
	private ISelectListener<T> selectListener = new NoopSelectListener<>();
	
	public IInsertListener<T> getInsertListener() {
		return insertListener;
	}
	
	public PersisterListener<T> setInsertListener(IInsertListener<T> insertListener) {
		if (insertListener != null) {	// prevent null as specified in interface
			this.insertListener = insertListener;
		}
		return this;
	}
	
	public IUpdateRouglyListener<T> getUpdateRouglyListener() {
		return updateRouglyListener;
	}
	
	public PersisterListener<T> setUpdateRouglyListener(IUpdateRouglyListener<T> updateRouglyListener) {
		if (updateRouglyListener != null) {    // prevent null as specified in interface
			this.updateRouglyListener = updateRouglyListener;
		}
		return this;
	}
	
	public IUpdateListener<T> getUpdateListener() {
		return updateListener;
	}
	
	public PersisterListener<T> setUpdateListener(IUpdateListener<T> updateListener) {
		if (updateListener != null) {    // prevent null as specified in interface
			this.updateListener = updateListener;
		}
		return this;
	}
	
	public IDeleteListener<T> getDeleteListener() {
		return deleteListener;
	}
	
	public PersisterListener<T> setDeleteListener(IDeleteListener<T> deleteListener) {
		if (deleteListener != null) {    // prevent null as specified in interface
			this.deleteListener = deleteListener;
		}
		return this;
	}
	
	public ISelectListener<T> getSelectListener() {
		return selectListener;
	}
	
	public PersisterListener<T> setSelectListener(ISelectListener<T> selectListener) {
		if (selectListener != null) {    // prevent null as specified in interface
			this.selectListener = selectListener;
		}
		return this;
	}
}
