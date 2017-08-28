package org.gama.stalactite.persistence.engine;

import org.gama.lang.function.Serie;
import org.gama.reflection.PropertyAccessor;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractVersioningStrategy<I, C> implements VersioningStrategy<I, C>, Serie<C> {
	
	private final PropertyAccessor<I, C> propertyAccessor;
	
	public AbstractVersioningStrategy(PropertyAccessor<I, C> propertyAccessor) {
		this.propertyAccessor = propertyAccessor;
	}
	
	@Override
	public PropertyAccessor<I, C> getPropertyAccessor() {
		return propertyAccessor;
	}
	
	@Override
	public C getVersion(I o) {
		return propertyAccessor.get(o);
	}
	
	@Override
	public C upgrade(I o) {
		C currentVersion = getVersion(o);
		C nextVersion = next(currentVersion);
		propertyAccessor.set(o, nextVersion);
		return currentVersion;
	}
	
	@Override
	public C revert(I o, C previousValue) {
		C currentVersion = getVersion(o);
		propertyAccessor.set(o, previousValue);
		return currentVersion;
	}
	
	@Override
	public abstract C next(C previousVersion);
	
	public static class VersioningStrategySupport<I, C> extends AbstractVersioningStrategy<I, C> {
		
		private final Serie<C> sequence;
		
		public VersioningStrategySupport(PropertyAccessor<I, C> propertyAccessor, Serie<C> sequence) {
			super(propertyAccessor);
			this.sequence = sequence;
		}
		
		@Override
		public C next(C previousVersion) {
			return sequence.next(previousVersion);
		}
	}
}
