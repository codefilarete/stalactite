package org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.tableperclass;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.EntityReadWriteExecutor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.PolymorphicEntity;
import org.codefilarete.stalactite.engine.configurer.resolver.CreatedPersisterCollector;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.runtime.tableperclass.TablePerClassPolymorphismWriter;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Iterables;

public class TablePerClassResolver {
	
	private final SkeletonAggregateResolver skeletonAggregateResolver;
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public TablePerClassResolver(SkeletonAggregateResolver skeletonAggregateResolver, Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.skeletonAggregateResolver = skeletonAggregateResolver;
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	public <TRGT, TRGTID, RIGHTTABLE extends Table<RIGHTTABLE>, SUBTRGT extends TRGT>
	TablePerClassPolymorphismWriter<TRGT, TRGTID, RIGHTTABLE, SUBTRGT> resolve(PolymorphicEntity<TRGT, TRGTID, RIGHTTABLE> targetEntity,
	                                                                           CreatedPersisterCollector<TRGT, TRGTID> persisterCollector) {
		
		EntityWriteExecutor<TRGT, TRGTID> templateWriter = skeletonAggregateResolver.buildPersister(targetEntity, persisterCollector);
		IdMapping<TRGT, TRGTID> idMapping = skeletonAggregateResolver.createIdMapping(targetEntity);
		
		Set<EntityReadWriteExecutor<SUBTRGT, TRGTID>> subPersisters = targetEntity.getPolymorphism().getSubEntities().stream().map(subEntity -> {
			// we need to build the sub-entities persisters first, so that they are registered in the persister registry
			// and can be found by the TablePerClassPolymorphismEngine
			// TODO: we should call the CreatedPersisterCollector for each subclass
			return buildSubPersister((Entity<SUBTRGT, TRGTID, ?>) subEntity);
		}).collect(Collectors.toSet());
		
		Map<Class<SUBTRGT>, EntityReadWriteExecutor<SUBTRGT, TRGTID>> map = Iterables.map(subPersisters, EntityReadWriteExecutor::getClassToPersist);
		TablePerClassPolymorphismWriter<TRGT, TRGTID, RIGHTTABLE, SUBTRGT> result = new TablePerClassPolymorphismWriter<>(
				templateWriter,
				map,
				dialect,
				connectionConfiguration
		);
		
		return result;
	}
	
	private <SUBENTITY, SUB_ENTITYID, SUBTABLE extends Table<SUBTABLE>>
	EntityReadWriteExecutor<SUBENTITY, SUB_ENTITYID> buildSubPersister(Entity<SUBENTITY, SUB_ENTITYID, SUBTABLE> subEntity) {
		return skeletonAggregateResolver.buildPersister(subEntity, new CreatedPersisterCollector<>());
	}
}
