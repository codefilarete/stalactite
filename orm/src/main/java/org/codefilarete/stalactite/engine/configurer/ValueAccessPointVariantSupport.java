package org.codefilarete.stalactite.engine.configurer;

import java.lang.reflect.Field;
import javax.annotation.Nullable;

import org.codefilarete.reflection.AccessorByField;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.DefaultReadWritePropertyAccessPoint;
import org.codefilarete.reflection.ReadWriteAccessorChain;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.SerializablePropertyAccessor;
import org.codefilarete.reflection.SerializablePropertyMutator;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.function.ThreadSafeLazyInitializer;

/**
 * Storage for different ways of declaring an attribute accessor: for each case, a constructor is available. Either by accessor, or mutator, or field.
 * The output is a {@link ReadWritePropertyAccessPoint} based on the attribute accessor given at construction time. The resolution is made through
 * {@link PropertyAccessorResolver}.
 *
 * @param <C> the owning type of the attribute
 * @param <O> attribute type
 * @author Guillaume Mary
 */
public class ValueAccessPointVariantSupport<C, O> {
	
	private final ThreadSafeLazyInitializer<ReadWritePropertyAccessPoint<C, O>> accessor;
	
	@Nullable
	private SerializablePropertyAccessor<C, O> getter;
	
	@Nullable
	private SerializablePropertyMutator<C, O> setter;
	
	// to be used in addition to getter or setter when attribute differs from method name (case when Java Naming Convention is not respected)
	@Nullable
	private Field field;
	
	public ValueAccessPointVariantSupport(SerializablePropertyAccessor<C, O> getter) {
		this.getter = getter;
		this.accessor = new AccessorFieldLazyInitializer();
	}
	
	public ValueAccessPointVariantSupport(SerializablePropertyMutator<C, O> setter) {
		this.setter = setter;
		this.accessor = new AccessorFieldLazyInitializer();
	}
	
	public ValueAccessPointVariantSupport(Class persistedClass, String fieldName) {
		this.accessor = new ThreadSafeLazyInitializer<ReadWritePropertyAccessPoint<C, O>>() {
			@Override
			protected ReadWritePropertyAccessPoint<C, O> createInstance() {
				return new DefaultReadWritePropertyAccessPoint<>(new AccessorByField<>(Reflections.getField(persistedClass, fieldName)));
			}
		};
	}
	
	public void setField(Class persistedClass, String fieldName) {
		this.field = Reflections.getField(persistedClass, fieldName);;
	}
	
	public ReadWritePropertyAccessPoint<C, O> getAccessor() {
		return accessor.get();
	}
	
	public <X> ReadWritePropertyAccessPoint<X, O> shift(ReadWritePropertyAccessPoint<X, C> accessor) {
		return new ReadWriteAccessorChain<>(AccessorChain.fromAccessorsWithNullSafe(Arrays.asList(accessor, getAccessor())));
	}
	/**
	 * Internal class that computes a {@link ReversibleAccessor} from getter or setter according to which one is set up
	 */
	private class AccessorFieldLazyInitializer extends ThreadSafeLazyInitializer<ReadWritePropertyAccessPoint<C, O>> {
		
		@Override
		protected ReadWritePropertyAccessPoint<C, O> createInstance() {
			return new PropertyAccessorResolver<>(new PropertyAccessorResolver.AccessPointCoordinates<C, O>() {
				
				@Override
				public SerializablePropertyAccessor<C, O> getGetter() {
					return ValueAccessPointVariantSupport.this.getter;
				}
				
				@Override
				public SerializablePropertyMutator<C, O> getSetter() {
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
