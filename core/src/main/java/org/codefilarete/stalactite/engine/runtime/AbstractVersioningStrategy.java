package org.codefilarete.stalactite.engine.runtime;

import org.codefilarete.stalactite.engine.VersioningStrategy;
import org.codefilarete.tool.function.Serie;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.PropertyAccessor;

/**
 * A template for implementing a {@link VersioningStrategy} that let user defines type of the versioning attribute and its sequence.
 * See {@link VersioningStrategySupport} for a complete implementation.
 *
 * @param <I> versionned bean type
 * @param <C> versioning attribute type
 * @author Guillaume Mary
 */
public abstract class AbstractVersioningStrategy<I, C> implements VersioningStrategy<I, C>, Serie<C> {
	
	private final ReversibleAccessor<I, C> versionAccessor;
	/** {@link Mutator} took from {@link #versionAccessor} to prevent to ask for it at every upgrade because the toMutator() may be costy */
	private final Mutator<I, C> versionMutator;
	
	public AbstractVersioningStrategy(ReversibleAccessor<I, C> versioningAttributeAccessor) {
		this.versionAccessor = versioningAttributeAccessor;
		this.versionMutator = versionAccessor.toMutator();
	}
	
	@Override
	public ReversibleAccessor<I, C> getVersionAccessor() {
		return versionAccessor;
	}
	
	@Override
	public C getVersion(I o) {
		return versionAccessor.get(o);
	}
	
	@Override
	public C upgrade(I o) {
		C currentVersion = getVersion(o);
		C nextVersion = next(currentVersion);
		versionMutator.set(o, nextVersion);
		return currentVersion;
	}
	
	@Override
	public C revert(I o, C previousValue) {
		C currentVersion = getVersion(o);
		versionMutator.set(o, previousValue);
		return currentVersion;
	}
	
	/**
	 * {@link VersioningStrategy} for simple cases
	 * 
	 * @param <I> versionned bean type
	 * @param <C> versioning attribute type
	 */
	public static class VersioningStrategySupport<I, C> extends AbstractVersioningStrategy<I, C> {
		
		private final Serie<C> sequence;
		
		public VersioningStrategySupport(PropertyAccessor<I, C> versioningAttributeAccessor, Serie<C> sequence) {
			super(versioningAttributeAccessor);
			this.sequence = sequence;
		}
		
		@Override
		public C next(C previousVersion) {
			return sequence.next(previousVersion);
		}
	}
}
