package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public interface IInsertListener<T> {
	
	void beforeInsert(Iterable<T> iterables);
	
	void afterInsert(Iterable<T> iterables);
	
	
}
