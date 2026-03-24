package org.codefilarete.stalactite.engine.runtime;

import org.codefilarete.reflection.PropertyAccessPoint;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.engine.VersioningStrategy;
import org.codefilarete.tool.function.Serie;

/**
 * A template for implementing a {@link VersioningStrategy} that let user defines type of the versioning attribute and its sequence.
 * See {@link VersioningStrategySupport} for a complete implementation.
 *
 * @param <C> versioned entity type
 * @param <V> versioning attribute type
 * @author Guillaume Mary
 */
public abstract class AbstractVersioningStrategy<C, V> implements VersioningStrategy<C, V>, Serie<V> {
	
	private final ReadWritePropertyAccessPoint<C, V> versionAccessor;
	
	public AbstractVersioningStrategy(ReadWritePropertyAccessPoint<C, V> versioningAttributeAccessor) {
		this.versionAccessor = versioningAttributeAccessor;
	}
	
	@Override
	public PropertyAccessPoint<C, V> getVersionAccessor() {
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
		versionAccessor.set(o, nextVersion);
		return currentVersion;
	}
	
	@Override
	public V revert(C o, V previousValue) {
		V currentVersion = getVersion(o);
		versionAccessor.set(o, previousValue);
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
		
		public VersioningStrategySupport(ReadWritePropertyAccessPoint<E, V> versioningAttributeAccessor, Serie<V> sequence) {
			super(versioningAttributeAccessor);
			this.sequence = sequence;
		}
		
		@Override
		public V next(V previousVersion) {
			return sequence.next(previousVersion);
		}
	}
}
