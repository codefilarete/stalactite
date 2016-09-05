package org.gama.reflection;

import java.util.ArrayList;
import java.util.List;

import org.gama.lang.collection.Iterables;

/**
 * @author Guillaume Mary
 */
public class AccessorChainMutator<C, X, T> extends AccessorChain<C, X> implements IReversibleMutator<C, T> {
	
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
	
	/**
	 * Only supported when last mutator is reversible (aka implements {@link IReversibleMutator}.
	 * 
	 * @return a new chain which path is the same as this
	 * @throws UnsupportedOperationException if last mutator is not reversible
	 */
	@Override
	public AccessorChain<C, T> toAccessor() {
		if (lastMutator instanceof IReversibleMutator) {
			ArrayList<IAccessor> newAccessors = new ArrayList<>(getAccessors());
			newAccessors.add(((IReversibleMutator) lastMutator).toAccessor());
			return new AccessorChain<>(newAccessors);
		} else {
			throw new UnsupportedOperationException("Last mutator cannot be reverted because it's not " + IReversibleAccessor.class.getName()
					+ ": " + lastMutator);
		}
	}
}
