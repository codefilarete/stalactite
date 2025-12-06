package org.codefilarete.stalactite.engine.configurer.entity;

import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.VersioningStrategy;
import org.codefilarete.stalactite.engine.runtime.AbstractVersioningStrategy;
import org.codefilarete.tool.function.Serie;

import static org.codefilarete.tool.Reflections.propertyName;

class OptimisticLockOption<C> {
	
	private final VersioningStrategy<Object, C> versioningStrategy;
	
	public OptimisticLockOption(AccessorByMethodReference<Object, C> versionAccessor, Serie<C> serie) {
		this.versioningStrategy = new AbstractVersioningStrategy.VersioningStrategySupport<>(new PropertyAccessor<>(
				versionAccessor,
				Accessors.mutator(versionAccessor.getDeclaringClass(), propertyName(versionAccessor.getMethodName()), versionAccessor.getPropertyType())
		), serie);
	}
	
	public VersioningStrategy getVersioningStrategy() {
		return versioningStrategy;
	}
}
