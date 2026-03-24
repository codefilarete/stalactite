package org.codefilarete.stalactite.engine.configurer.embeddable;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.reflection.SerializablePropertyAccessor;
import org.codefilarete.reflection.SerializablePropertyMutator;
import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilderOneToManyOptions;
import org.codefilarete.stalactite.dsl.relation.OneToManyOptions;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;

/**
 * A small class for one-to-many options storage into a {@link OneToManyOptions}. Acts as a wrapper over it.
 */
class OneToManyOptionsSupport<C, O, S extends Collection<O>>
		implements OneToManyOptions<C, O, S> {
	
	private final OneToManyRelation<C, O, ?, S> oneToManyRelation;
	
	public OneToManyOptionsSupport(OneToManyRelation<C, O, ?, S> oneToManyRelation) {
		this.oneToManyRelation = oneToManyRelation;
	}
	
	@Override
	public FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> mappedBy(SerializablePropertyMutator<O, ? super C> reverseLink) {
		oneToManyRelation.setReverseSetter(reverseLink);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> mappedBy(SerializablePropertyAccessor<O, ? super C> reverseLink) {
		oneToManyRelation.setReverseGetter(reverseLink);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> reverseJoinColumn(String reverseColumnName) {
		oneToManyRelation.setReverseColumn(reverseColumnName);
		return null;
	}
	
	@Override
	public FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> reverselySetBy(SerializablePropertyMutator<O, C> reverseLink) {
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
