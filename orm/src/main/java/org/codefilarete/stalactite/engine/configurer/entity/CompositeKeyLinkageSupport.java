package org.codefilarete.stalactite.engine.configurer.entity;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.CompositeKeyMapping;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.key.CompositeKeyMappingConfigurationProvider;

import static org.codefilarete.stalactite.dsl.key.CompositeKeyMappingConfiguration.CompositeKeyLinkage;

/**
 * Storage for composite key mapping definition. See {@link FluentEntityMappingBuilder#mapKey(org.codefilarete.reflection.SerializablePropertyAccessor, CompositeKeyMappingConfigurationProvider, Consumer, Function)} methods.
 */
public class CompositeKeyLinkageSupport<C, I> implements CompositeKeyMapping<C, I> {
	
	private final ReadWritePropertyAccessPoint<C, I> accessor;
	private final CompositeKeyMappingConfigurationProvider<I> compositeKeyMappingBuilder;
	private final Consumer<C> markAsPersistedFunction;
	private final Function<C, Boolean> isPersistedFunction;
	
	private boolean setByConstructor;
	
	public CompositeKeyLinkageSupport(ReadWritePropertyAccessPoint<C, I> accessor,
									  CompositeKeyMappingConfigurationProvider<I> compositeKeyMappingBuilder,
									  Consumer<C> markAsPersistedFunction,
									  Function<C, Boolean> isPersistedFunction) {
		this.accessor = accessor;
		this.compositeKeyMappingBuilder = compositeKeyMappingBuilder;
		this.markAsPersistedFunction = markAsPersistedFunction;
		this.isPersistedFunction = isPersistedFunction;
	}
	
	public CompositeKeyMappingConfigurationProvider<I> getCompositeKeyMappingBuilder() {
		return compositeKeyMappingBuilder;
	}
	
	@Override
	public ReadWritePropertyAccessPoint<C, I> getAccessor() {
		return accessor;
	}
	
	public void setByConstructor() {
		this.setByConstructor = true;
	}
	
	@Override
	public boolean isSetByConstructor() {
		return setByConstructor;
	}
	
	public List<CompositeKeyLinkage<I, ?>> getPropertiesMapping() {
		return compositeKeyMappingBuilder.getConfiguration().getPropertiesMapping();
	}
	
	@Override
	public Consumer<C> getMarkAsPersistedFunction() {
		return markAsPersistedFunction;
	}
	
	@Override
	public Function<C, Boolean> getIsPersistedFunction() {
		return isPersistedFunction;
	}
	
}
