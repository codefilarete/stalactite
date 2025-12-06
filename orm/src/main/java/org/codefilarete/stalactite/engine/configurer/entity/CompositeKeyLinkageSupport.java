package org.codefilarete.stalactite.engine.configurer.entity;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.CompositeKeyMapping;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.key.CompositeKeyMappingConfiguration;
import org.codefilarete.stalactite.dsl.key.CompositeKeyMappingConfigurationProvider;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * Storage for composite key mapping definition. See {@link FluentEntityMappingBuilder#mapCompositeKey(SerializableFunction, CompositeKeyMappingConfigurationProvider, Consumer, Function)} methods.
 */
public class CompositeKeyLinkageSupport<C, I> implements CompositeKeyMapping<C, I> {
	
	private final ReversibleAccessor<C, I> accessor;
	private final CompositeKeyMappingConfigurationProvider<I> compositeKeyMappingBuilder;
	private final Consumer<C> markAsPersistedFunction;
	private final Function<C, Boolean> isPersistedFunction;
	
	@javax.annotation.Nullable
	private EntityMappingConfiguration.CompositeKeyLinkageOptions columnOptions;
	
	private boolean setByConstructor;
	
	public CompositeKeyLinkageSupport(ReversibleAccessor<C, I> accessor,
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
	public ReversibleAccessor<C, I> getAccessor() {
		return accessor;
	}
	
	public void setColumnOptions(EntityMappingConfiguration.CompositeKeyLinkageOptions columnOptions) {
		this.columnOptions = columnOptions;
	}
	
	public void setByConstructor() {
		this.setByConstructor = true;
	}
	
	@Override
	public boolean isSetByConstructor() {
		return setByConstructor;
	}
	
	public List<CompositeKeyMappingConfiguration.CompositeKeyLinkage> getPropertiesMapping() {
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
	
	@javax.annotation.Nullable
	@Override
	public EntityMappingConfiguration.CompositeKeyLinkageOptions getColumnsOptions() {
		return columnOptions;
	}
}
