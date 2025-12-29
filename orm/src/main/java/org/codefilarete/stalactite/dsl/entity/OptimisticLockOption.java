package org.codefilarete.stalactite.dsl.entity;

import javax.annotation.Nullable;
import java.lang.reflect.Field;

import org.codefilarete.reflection.AccessorByField;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.configurer.PropertyAccessorResolver;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.function.Serie;
import org.codefilarete.tool.function.ThreadSafeLazyInitializer;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

public class OptimisticLockOption<C, V> {
	
	private final ThreadSafeLazyInitializer<ReversibleAccessor<C, V>> versionAccessor;
	
	private SerializableFunction<C, V> getter;
	
	private SerializableBiConsumer<C, V> setter;
	
	private Field field;
	
	@Nullable
	private final Serie<V> serie;
	
	public OptimisticLockOption(SerializableFunction<C, V> getter, @Nullable Serie<V> serie) {
		this.getter = getter;
		this.versionAccessor = new AccessorFieldLazyInitializer();
		this.serie = serie;
	}
	
	public OptimisticLockOption(SerializableBiConsumer<C, V> setter, @Nullable Serie<V> serie) {
		this.setter = setter;
		this.versionAccessor = new AccessorFieldLazyInitializer();
		this.serie = serie;
	}
	
	public OptimisticLockOption(Class persistedClass, String fieldName, @Nullable Serie<V> serie) {
		this.versionAccessor = new ThreadSafeLazyInitializer<ReversibleAccessor<C, V>>() {
			@Override
			protected ReversibleAccessor<C, V> createInstance() {
				return new AccessorByField<>(Reflections.getField(persistedClass, fieldName));
			}
		};
		this.serie = serie;
	}
	
	public ReversibleAccessor<C, V> getVersionAccessor() {
		return versionAccessor.get();
	}
	
	@Nullable
	public Serie<V> getSerie() {
		return serie;
	}
	
	/**
	 * Internal class that computes a {@link PropertyAccessor} from getter or setter according to which one is set up
	 */
	private class AccessorFieldLazyInitializer extends ThreadSafeLazyInitializer<ReversibleAccessor<C, V>> {
		
		@Override
		protected ReversibleAccessor<C, V> createInstance() {
			return new PropertyAccessorResolver<>(new PropertyAccessorResolver.PropertyMapping<C, V>() {
				@Override
				public SerializableFunction<C, V> getGetter() {
					return OptimisticLockOption.this.getter;
				}
				
				@Override
				public SerializableBiConsumer<C, V> getSetter() {
					return OptimisticLockOption.this.setter;
				}
				
				@Override
				public Field getField() {
					return OptimisticLockOption.this.field;
				}
			}).resolve();
		}
	}
}
