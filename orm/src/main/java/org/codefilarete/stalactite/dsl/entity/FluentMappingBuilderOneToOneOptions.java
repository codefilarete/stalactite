package org.codefilarete.stalactite.dsl.entity;

import org.codefilarete.stalactite.dsl.relation.OneToOneEntityOptions;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

public interface FluentMappingBuilderOneToOneOptions<C, I, O> extends FluentEntityMappingBuilder<C, I>,
		OneToOneEntityOptions<C, I, O> {
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderOneToOneOptions<C, I, O> mandatory();
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseLink opposite owner of the relation (setter)
	 * @return the global mapping configurer
	 */
	FluentMappingBuilderOneToOneOptions<C, I, O> mappedBy(SerializableBiConsumer<? super O, C> reverseLink);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseLink opposite owner of the relation (getter)
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderOneToOneOptions<C, I, O> mappedBy(SerializableFunction<? super O, C> reverseLink);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseLink opposite owner of the relation
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderOneToOneOptions<C, I, O> mappedBy(Column<?, I> reverseLink);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseColumnName opposite owner of the relation
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderOneToOneOptions<C, I, O> mappedBy(String reverseColumnName);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param relationMode any {@link RelationMode}
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderOneToOneOptions<C, I, O> cascading(RelationMode relationMode);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderOneToOneOptions<C, I, O> fetchSeparately();
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderOneToOneOptions<C, I, O> unique();
}
