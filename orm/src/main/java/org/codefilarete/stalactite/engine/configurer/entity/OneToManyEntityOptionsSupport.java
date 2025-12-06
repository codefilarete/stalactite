package org.codefilarete.stalactite.engine.configurer.entity;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.stalactite.dsl.entity.FluentMappingBuilderOneToManyOptions;
import org.codefilarete.stalactite.dsl.relation.OneToManyEntityOptions;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * A small class for one-to-many options storage into a {@link OneToManyEntityOptions}. Acts as a wrapper over it.
 */
public class OneToManyEntityOptionsSupport<C, I, O, S extends Collection<O>, O_ID>
		implements OneToManyEntityOptions<C, I, O, S> {
	
	private final OneToManyRelation<C, O, O_ID, S> oneToManyRelation;
	
	public OneToManyEntityOptionsSupport(OneToManyRelation<C, O, O_ID, S> oneToManyRelation) {
		this.oneToManyRelation = oneToManyRelation;
	}
	
	@Override
	public FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(SerializableBiConsumer<O, ? super C> reverseLink) {
		oneToManyRelation.setReverseSetter(reverseLink);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(SerializableFunction<O, ? super C> reverseLink) {
		oneToManyRelation.setReverseGetter(reverseLink);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(Column<?, I> reverseLink) {
		oneToManyRelation.setReverseColumn(reverseLink);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(String reverseColumnName) {
		oneToManyRelation.setReverseColumn(reverseColumnName);
		return null;
	}
	
	@Override
	public FluentMappingBuilderOneToManyOptions<C, I, O, S> reverselySetBy(SerializableBiConsumer<O, C> reverseLink) {
		oneToManyRelation.setReverseLink(reverseLink);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentMappingBuilderOneToManyOptions<C, I, O, S> initializeWith(Supplier<S> collectionFactory) {
		oneToManyRelation.setCollectionFactory(collectionFactory);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentMappingBuilderOneToManyOptions<C, I, O, S> cascading(RelationMode relationMode) {
		oneToManyRelation.setRelationMode(relationMode);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentMappingBuilderOneToManyOptions<C, I, O, S> fetchSeparately() {
		oneToManyRelation.fetchSeparately();
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentMappingBuilderOneToManyOptions<C, I, O, S> indexedBy(Column<?, Integer> orderingColumn) {
		oneToManyRelation.setIndexingColumn(orderingColumn);
		return null;
	}
	
	@Override
	public FluentMappingBuilderOneToManyOptions<C, I, O, S> indexedBy(String columnName) {
		oneToManyRelation.setIndexingColumnName(columnName);
		return null;
	}
	
	@Override
	public FluentMappingBuilderOneToManyOptions<C, I, O, S> indexed() {
		oneToManyRelation.ordered();
		return null;
	}
}
