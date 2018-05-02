package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
@SuppressWarnings("squid:S1186")	// methods are obviously empty because it is the goal of this class
public class NoopUpdateByIdListener<T> implements IUpdateByIdListener<T> {
	
	@Override
	public void beforeUpdateById(Iterable<T> iterables) {
		
	}
	
	@Override
	public void afterUpdateById(Iterable<T> iterables) {
		
	}
}
