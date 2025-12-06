package org.codefilarete.stalactite.engine.configurer.entity;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.stalactite.dsl.entity.FluentMappingBuilderManyToManyOptions;
import org.codefilarete.stalactite.dsl.relation.ManyToManyOptions;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * A small class for one-to-many options storage into a {@link OneToManyRelation}. Acts as a wrapper over it.
 */
public class ManyToManyOptionsSupport<C, I, O, S1 extends Collection<O>, S2 extends Collection<C>>
		implements ManyToManyOptions<C, O, S1, S2> {
	
	private final ManyToManyRelation<C, O, I, S1, S2> manyToManyRelation;
	
	public ManyToManyOptionsSupport(ManyToManyRelation<C, O, I, S1, S2> manyToManyRelation) {
		this.manyToManyRelation = manyToManyRelation;
	}
	
	@Override
	public FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> initializeWith(Supplier<S1> collectionFactory) {
		manyToManyRelation.setCollectionFactory(collectionFactory);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> reverselySetBy(SerializableBiConsumer<O, C> reverseLink) {
		manyToManyRelation.getMappedByConfiguration().setReverseCombiner(reverseLink);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> reverseCollection(SerializableFunction<O, S2> collectionAccessor) {
		manyToManyRelation.getMappedByConfiguration().setReverseCollectionAccessor(collectionAccessor);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> reverseCollection(SerializableBiConsumer<O, S2> collectionMutator) {
		manyToManyRelation.getMappedByConfiguration().setReverseCollectionMutator(collectionMutator);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> reverselyInitializeWith(Supplier<S2> collectionFactory) {
		manyToManyRelation.getMappedByConfiguration().setReverseCollectionFactory(collectionFactory);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> cascading(RelationMode relationMode) {
		manyToManyRelation.setRelationMode(relationMode);
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> fetchSeparately() {
		manyToManyRelation.fetchSeparately();
		return null;    // we can return null because dispatcher will return proxy
	}
	
	@Override
	public FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> indexedBy(String columnName) {
		manyToManyRelation.setIndexingColumnName(columnName);
		return null;
	}
	
	@Override
	public FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> indexed() {
		manyToManyRelation.ordered();
		return null;
	}
}
