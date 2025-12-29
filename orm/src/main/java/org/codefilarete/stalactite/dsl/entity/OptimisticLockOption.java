package org.codefilarete.stalactite.dsl.entity;

import javax.annotation.Nullable;

import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.tool.function.Serie;

import static org.codefilarete.tool.Reflections.propertyName;

public class OptimisticLockOption<C, V> {
	
	private final PropertyAccessor<C, V> versionAccessor;
	
	@Nullable
	private final Serie<V> serie;
	
	public OptimisticLockOption(PropertyAccessor<C, V> versionAccessor, @Nullable Serie<V> serie) {
		this.versionAccessor = versionAccessor;
		this.serie = serie;
		
	}
	
	public OptimisticLockOption(AccessorByMethodReference<C, V> versionAccessor, Serie<V> serie) {
		this(new PropertyAccessor<>(
				versionAccessor,
				Accessors.mutator(versionAccessor.getDeclaringClass(), propertyName(versionAccessor.getMethodName()), versionAccessor.getPropertyType())
		), serie);
	}
	
	public PropertyAccessor<C, V> getVersionAccessor() {
		return versionAccessor;
	}
	
	@Nullable
	public Serie<V> getSerie() {
		return serie;
	}
}
