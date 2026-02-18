package org.codefilarete.stalactite.dsl.key;

import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;

/**
 * A mashup of {@link FluentEntityMappingBuilder} and {@link CompositeKeyOptions} for the fluent API.
 * 
 * @param <C>
 * @param <I>
 * @author Guillaume Mary
 */
public interface FluentEntityMappingBuilderCompositeKeyOptions<C, I> extends FluentEntityMappingBuilder<C, I>, CompositeKeyOptions<C, I> {

}
