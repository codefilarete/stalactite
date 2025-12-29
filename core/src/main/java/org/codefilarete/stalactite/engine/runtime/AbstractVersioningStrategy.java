package org.codefilarete.stalactite.engine.runtime;

import org.codefilarete.stalactite.engine.VersioningStrategy;
import org.codefilarete.tool.function.Serie;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.ReversibleAccessor;

/**
 * A template for implementing a {@link VersioningStrategy} that let user defines type of the versioning attribute and its sequence.
 * See {@link VersioningStrategySupport} for a complete implementation.
 *
 * @param <C> versioned entity type
 * @param <V> versioning attribute type
 * @author Guillaume Mary
 */
public abstract class AbstractVersioningStrategy<C, V> implements VersioningStrategy<C, V>, Serie<V> {
	
	private final ReversibleAccessor<C, V> versionAccessor;
	/** {@link Mutator} took from {@link #versionAccessor} to prevent to ask for it at every upgrade because the toMutator() may be costly */
	private final Mutator<C, V> versionMutator;
	
	public AbstractVersioningStrategy(ReversibleAccessor<C, V> versioningAttributeAccessor) {
		this.versionAccessor = versioningAttributeAccessor;
		this.versionMutator = versionAccessor.toMutator();
	}
	
	@Override
	public ReversibleAccessor<C, V> getVersionAccessor() {
		return versionAccessor;
	}
	
	@Override
	public V getVersion(C o) {
		return versionAccessor.get(o);
	}
	
	@Override
	public V upgrade(C o) {
		V currentVersion = getVersion(o);
		V nextVersion = next(currentVersion);
		versionMutator.set(o, nextVersion);
		return currentVersion;
	}
	
	@Override
	public V revert(C o, V previousValue) {
		V currentVersion = getVersion(o);
		versionMutator.set(o, previousValue);
		return currentVersion;
	}
	
	/**
	 * {@link VersioningStrategy} for simple cases
	 * 
	 * @param <E> versioned entity type
	 * @param <V> versioning attribute type
	 */
	public static class VersioningStrategySupport<E, V> extends AbstractVersioningStrategy<E, V> {
		
		private final Serie<V> sequence;
		
		public VersioningStrategySupport(ReversibleAccessor<E, V> versioningAttributeAccessor, Serie<V> sequence) {
			super(versioningAttributeAccessor);
			this.sequence = sequence;
		}
		
		@Override
		public V next(V previousVersion) {
			return sequence.next(previousVersion);
		}
	}
}
