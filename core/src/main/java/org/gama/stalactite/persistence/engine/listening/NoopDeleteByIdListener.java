package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
@SuppressWarnings("squid:S1186")	// methods are obviously empty because it is the goal of this class
public class NoopDeleteByIdListener<T> implements IDeleteByIdListener<T> {
	
	@Override
	public void beforeDeleteById(Iterable<T> iterables) {
		
	}
	
	@Override
	public void afterDeleteById(Iterable<T> iterables) {
		
	}
}
