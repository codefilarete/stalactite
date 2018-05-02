package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
@SuppressWarnings("squid:S1186")	// methods are obviously empty because it is the goal of this class
public class NoopSelectListener<T, I> implements ISelectListener<T, I> {
	
	@Override
	public void beforeSelect(Iterable<I> ids) {
		
	}
	
	@Override
	public void afterSelect(Iterable<T> result) {
		
	}
}
