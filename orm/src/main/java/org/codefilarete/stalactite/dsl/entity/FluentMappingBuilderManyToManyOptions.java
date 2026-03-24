package org.codefilarete.stalactite.dsl.entity;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.reflection.SerializablePropertyAccessor;
import org.codefilarete.reflection.SerializablePropertyMutator;
import org.codefilarete.stalactite.dsl.relation.ManyToManyEntityOptions;

public interface FluentMappingBuilderManyToManyOptions<C, I, O, S1 extends Collection<O>, S2 extends Collection<C>> extends FluentEntityMappingBuilder<C, I>, ManyToManyEntityOptions<C, O, S1, S2> {
	
	@Override
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> initializeWith(Supplier<S1> collectionFactory);
	
	@Override
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> reverselySetBy(SerializablePropertyMutator<O, C> reverseLink);
	
	@Override
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> reverseCollection(SerializablePropertyAccessor<O, S2> collectionAccessor);
	
	@Override
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> reverseCollection(SerializablePropertyMutator<O, S2> collectionMutator);
	
	@Override
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> reverselyInitializeWith(Supplier<S2> collectionFactory);
	
	@Override
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> cascading(RelationMode relationMode);
	
	@Override
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> fetchSeparately();
	
	@Override
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> indexedBy(String columnName);
	
	@Override
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> indexed();
	
	@Override
	FluentMappingBuilderManyToManyJoinTableOptions<C, I, O, S1, S2> joinTable(String tableName);
	
}
