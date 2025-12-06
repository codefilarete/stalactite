package org.codefilarete.stalactite.engine.configurer.entity;

import javax.annotation.Nullable;
import java.lang.reflect.Field;

import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.SingleKeyMapping;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.engine.configurer.PropertyAccessorResolver;
import org.codefilarete.stalactite.engine.configurer.property.ColumnLinkageOptionsSupport;
import org.codefilarete.stalactite.engine.configurer.property.LocalColumnLinkageOptions;
import org.codefilarete.tool.function.ThreadSafeLazyInitializer;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * Storage for single key mapping definition. See {@link org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder#mapKey(SerializableFunction, IdentifierPolicy)} methods.
 */
class SingleKeyLinkageSupport<C, I> implements SingleKeyMapping<C, I> {
	
	private final IdentifierPolicy<I> identifierPolicy;
	
	private LocalColumnLinkageOptions columnOptions = new ColumnLinkageOptionsSupport();
	
	private boolean setByConstructor;
	
	private final ThreadSafeLazyInitializer<ReversibleAccessor<C, I>> accessor;
	
	private SerializableFunction<C, I> getter;
	
	private SerializableBiConsumer<C, I> setter;
	
	private Field field;
	
	public SingleKeyLinkageSupport(SerializableFunction<C, I> getter, IdentifierPolicy<I> identifierPolicy) {
		this.getter = getter;
		this.identifierPolicy = identifierPolicy;
		this.accessor = new AccessorFieldLazyInitializer();
	}
	
	public SingleKeyLinkageSupport(SerializableBiConsumer<C, I> setter, IdentifierPolicy<I> identifierPolicy) {
		this.setter = setter;
		this.identifierPolicy = identifierPolicy;
		this.accessor = new AccessorFieldLazyInitializer();
	}
	
	@Override
	public IdentifierPolicy<I> getIdentifierPolicy() {
		return identifierPolicy;
	}
	
	@Override
	public ReversibleAccessor<C, I> getAccessor() {
		return accessor.get();
	}
	
	@Override
	public LocalColumnLinkageOptions getColumnOptions() {
		return columnOptions;
	}
	
	public void setColumnOptions(LocalColumnLinkageOptions columnOptions) {
		this.columnOptions = columnOptions;
	}
	
	public void setByConstructor() {
		this.setByConstructor = true;
	}
	
	@Override
	@Nullable
	public Field getField() {
		return field;
	}
	
	public void setField(Field field) {
		this.field = field;
	}
	
	@Override
	public boolean isSetByConstructor() {
		return setByConstructor;
	}
	
	/**
	 * Internal class that computes a {@link PropertyAccessor} from getter or setter according to which one is set up
	 */
	private class AccessorFieldLazyInitializer extends ThreadSafeLazyInitializer<ReversibleAccessor<C, I>> {
		
		@Override
		protected ReversibleAccessor<C, I> createInstance() {
			return new PropertyAccessorResolver<>(new PropertyAccessorResolver.PropertyMapping<C, I>() {
				@Override
				public SerializableFunction<C, I> getGetter() {
					return SingleKeyLinkageSupport.this.getter;
				}
				
				@Override
				public SerializableBiConsumer<C, I> getSetter() {
					return SingleKeyLinkageSupport.this.setter;
				}
				
				@Override
				public Field getField() {
					return SingleKeyLinkageSupport.this.getField();
				}
			}).resolve();
		}
	}
}
