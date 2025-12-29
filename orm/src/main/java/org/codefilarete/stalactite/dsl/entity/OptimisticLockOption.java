package org.codefilarete.stalactite.dsl.entity;

import javax.annotation.Nullable;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.configurer.ValueAccessPointVariantSupport;
import org.codefilarete.tool.function.Serie;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

public class OptimisticLockOption<C, V> {
	
	private final ValueAccessPointVariantSupport<C, V> versionAccessor;
	
	@Nullable
	private final Serie<V> serie;
	
	public OptimisticLockOption(SerializableFunction<C, V> getter, @Nullable Serie<V> serie) {
		this.versionAccessor = new ValueAccessPointVariantSupport<>(getter);
		this.serie = serie;
	}
	
	public OptimisticLockOption(SerializableBiConsumer<C, V> setter, @Nullable Serie<V> serie) {
		this.versionAccessor = new ValueAccessPointVariantSupport<>(setter);
		this.serie = serie;
	}
	
	public OptimisticLockOption(Class persistedClass, String fieldName, @Nullable Serie<V> serie) {
		this.versionAccessor = new ValueAccessPointVariantSupport<>(persistedClass, fieldName);
		this.serie = serie;
	}
	
	public ReversibleAccessor<C, V> getVersionAccessor() {
		return versionAccessor.getAccessor();
	}
	
	@Nullable
	public Serie<V> getSerie() {
		return serie;
	}
}
