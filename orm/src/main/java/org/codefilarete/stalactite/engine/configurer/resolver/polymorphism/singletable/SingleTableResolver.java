package org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.singletable;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.EntityReadWriteExecutor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.PolymorphicEntity;
import org.codefilarete.stalactite.engine.configurer.model.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.configurer.resolver.CreatedPersisterCollector;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.runtime.singletable.SingleTablePolymorphismWriter;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Iterables;

public class SingleTableResolver {
	
	private final SkeletonAggregateResolver skeletonAggregateResolver;
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public SingleTableResolver(SkeletonAggregateResolver skeletonAggregateResolver, Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.skeletonAggregateResolver = skeletonAggregateResolver;
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	public <TRGT, TRGTID, RIGHTTABLE extends Table<RIGHTTABLE>, SUBTRGT extends TRGT, DTYPE>
	SingleTablePolymorphismWriter<TRGT, TRGTID, RIGHTTABLE, SUBTRGT, DTYPE> resolve(PolymorphicEntity<TRGT, TRGTID, RIGHTTABLE> targetEntity,
	                                                                                CreatedPersisterCollector<TRGT, TRGTID> persisterCollector) {
		
		EntityWriteExecutor<TRGT, TRGTID> templateWriter = skeletonAggregateResolver.buildPersister(targetEntity, persisterCollector);
		
		SingleTablePolymorphism<TRGT, TRGTID, DTYPE, RIGHTTABLE> singleTablePolymorphism = (SingleTablePolymorphism<TRGT, TRGTID, DTYPE, RIGHTTABLE>) targetEntity.getPolymorphism();
		Set<EntityReadWriteExecutor<SUBTRGT, TRGTID>> subPersisters = singleTablePolymorphism.getSubEntities().stream().map(subEntity -> {
			// we need to build the sub-entities persisters first, so that they are registered in the persister registry
			// and can be found by the TablePerClassPolymorphismEngine
			// TODO: we should call the CreatedPersisterCollector for each subclass
			return buildSubPersister((Entity<SUBTRGT, TRGTID, ?>) subEntity);
		}).collect(Collectors.toSet());
		
		Map<Class<SUBTRGT>, EntityReadWriteExecutor<SUBTRGT, TRGTID>> persisterPerSubType = Iterables.map(subPersisters, EntityReadWriteExecutor::getClassToPersist);
		Function<? super Class<SUBTRGT>, DTYPE> discriminatorValueProvider = singleTablePolymorphism::getDiscriminatorValue;
		
		return new SingleTablePolymorphismWriter<>(
				templateWriter,
				persisterPerSubType,
				singleTablePolymorphism.getDiscriminatorColumn(),
				discriminatorValueProvider,
				dialect,
				connectionConfiguration
		);
	}
	
	private <SUBENTITY, SUB_ENTITYID, SUBTABLE extends Table<SUBTABLE>>
	EntityReadWriteExecutor<SUBENTITY, SUB_ENTITYID> buildSubPersister(Entity<SUBENTITY, SUB_ENTITYID, SUBTABLE> subEntity) {
		return skeletonAggregateResolver.buildPersister(subEntity, new CreatedPersisterCollector<>());
	}
}
