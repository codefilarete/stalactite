package org.codefilarete.stalactite.engine.configurer.embeddable;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilderOneToManyOptions;
import org.codefilarete.stalactite.dsl.relation.OneToManyEntityOptions;
import org.codefilarete.stalactite.dsl.relation.OneToManyOptions;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * A small class for one-to-many options storage into a {@link OneToManyEntityOptions}. Acts as a wrapper over it.
 */
class OneToManyOptionsSupport<C, O, S extends Collection<O>>
		implements OneToManyOptions<C, O, S> {
	
	private final OneToManyRelation<C, O, ?, S> oneToManyRelation;
	
	public OneToManyOptionsSupport(OneToManyRelation<C, O, ?, S> oneToManyRelation) {
		this.oneToManyRelation = oneToManyRelation;
	}
	
	@Override
	public FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> mappedBy(SerializableBiConsumer<O, ? super C> reverseLink) {
		oneToManyRelation.setReverseSetter(reverseLink);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> mappedBy(SerializableFunction<O, ? super C> reverseLink) {
		oneToManyRelation.setReverseGetter(reverseLink);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> reverseJoinColumn(String reverseColumnName) {
		oneToManyRelation.setReverseColumn(reverseColumnName);
		return null;
	}
	
	@Override
	public FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> joinTable(String tableName) {
		oneToManyRelation.setAssociationTableName(tableName);
		return null;
	}
	
	@Override
	public FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> reverselySetBy(SerializableBiConsumer<O, C> reverseLink) {
		oneToManyRelation.setReverseLink(reverseLink);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> initializeWith(Supplier<S> collectionFactory) {
		oneToManyRelation.setCollectionFactory(collectionFactory);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> cascading(RelationMode relationMode) {
		oneToManyRelation.setRelationMode(relationMode);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> fetchSeparately() {
		oneToManyRelation.fetchSeparately();
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> indexedBy(String columnName) {
		oneToManyRelation.setIndexingColumnName(columnName);
		return null;
	}
	
	@Override
	public FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> indexed() {
		oneToManyRelation.ordered();
		return null;
	}
}
