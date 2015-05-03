package org.stalactite.reflection;

import java.util.ArrayList;
import java.util.List;

import org.gama.lang.collection.Iterables;

/**
 * @author Guillaume Mary
 */
public class AccessorChainMutator<C, X, T> extends AccessorChain<C, X> implements IMutator<C, T> {
	
	private final IMutator<X, T> lastMutator;
	
	public AccessorChainMutator(List<IAccessor> accessors, IMutator<X, T> mutator) {
		super(accessors);
		this.lastMutator = mutator;
	}
	
	public AccessorChainMutator(AccessorChain<C, X> accessors, IMutator<X, T> mutator) {
		super(accessors.getAccessors());
		this.lastMutator = mutator;
	}
	
	@Override
	public void set(C c, T t) {
		X target = get(c);
		if (target == null) {
			throwNullPointerException(c, Iterables.last(getAccessors()));
		}
		lastMutator.set(target, t);
	}
	
	@Override
	public AccessorChain<C, T> toAccessor() {
		ArrayList<IAccessor> newAccessors = new ArrayList<>(getAccessors());
		newAccessors.add(lastMutator.toAccessor());
		return new AccessorChain<>(newAccessors);
	}
}
