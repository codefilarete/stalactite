package org.codefilarete.stalactite.engine.configurer;

import java.lang.reflect.Field;

import org.codefilarete.reflection.AccessorByField;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.function.ThreadSafeLazyInitializer;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * Storage for different ways of declaring an attribute accessor: for each case, a constructor is available. Either by accessor, or mutator, or field.
 * The output is a {@link ReversibleAccessor} based on the attribute accessor given at construction time. The resolution is made through
 * {@link PropertyAccessorResolver}.
 *
 * @param <T> the owning type of the attribute
 * @param <O> attribute type
 * @author Guillaume Mary
 */
public class ValueAccessPointVariantSupport<T, O> {
	
	private final ThreadSafeLazyInitializer<ReversibleAccessor<T, O>> accessor;
	
	private SerializableFunction<T, O> getter;
	
	private SerializableBiConsumer<T, O> setter;
	
	// to be used in addition to getter or setter when attribute differs from method name (case when Java Naming Convention is not respected)
	private Field field;
	
	public ValueAccessPointVariantSupport(SerializableFunction<T, O> getter) {
		this.getter = getter;
		this.accessor = new AccessorFieldLazyInitializer();
	}
	
	public ValueAccessPointVariantSupport(SerializableBiConsumer<T, O> setter) {
		this.setter = setter;
		this.accessor = new AccessorFieldLazyInitializer();
	}
	
	public ValueAccessPointVariantSupport(Class persistedClass, String fieldName) {
		this.accessor = new ThreadSafeLazyInitializer<ReversibleAccessor<T, O>>() {
			@Override
			protected ReversibleAccessor<T, O> createInstance() {
				return new AccessorByField<>(Reflections.getField(persistedClass, fieldName));
			}
		};
	}
	
	public void setField(Class persistedClass, String fieldName) {
		this.field = Reflections.getField(persistedClass, fieldName);;
	}
	
	public ReversibleAccessor<T, O> getAccessor() {
		return accessor.get();
	}
	
	/**
	 * Internal class that computes a {@link ReversibleAccessor} from getter or setter according to which one is set up
	 */
	private class AccessorFieldLazyInitializer extends ThreadSafeLazyInitializer<ReversibleAccessor<T, O>> {
		
		@Override
		protected ReversibleAccessor<T, O> createInstance() {
			return new PropertyAccessorResolver<>(new PropertyAccessorResolver.PropertyMapping<T, O>() {
				
				@Override
				public SerializableFunction<T, O> getGetter() {
					return ValueAccessPointVariantSupport.this.getter;
				}
				
				@Override
				public SerializableBiConsumer<T, O> getSetter() {
					return ValueAccessPointVariantSupport.this.setter;
				}
				
				@Override
				public Field getField() {
					return ValueAccessPointVariantSupport.this.field;
				}
			}).resolve();
		}
	}
}
