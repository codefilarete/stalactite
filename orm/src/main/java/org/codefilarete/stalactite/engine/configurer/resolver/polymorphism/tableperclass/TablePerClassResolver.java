package org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.tableperclass;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
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
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;
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
		
		// TODO : The internal code that build the TablePerClassPolymorphismEngine should be moved there for clarity
		// TODO : we should make it not implement selection because the appender is expected to make it for us
//		EntityWriteExecutor<TRGT, TRGTID> templatePersister = skeletonAggregateResolver.buildPersister(targetEntity);
		
		Set<EntityReadWriteExecutor<SUBTRGT, TRGTID>> subPersisters = targetEntity.getPolymorphism().getSubEntities().stream().map(subEntity -> {
			// we need to build the sub-entities persisters first, so that they are registered in the persister registry
			// and can be found by the TablePerClassPolymorphismEngine
			return buildSubPersister((Entity<SUBTRGT, TRGTID, ?>) subEntity);
		}).collect(Collectors.toSet());
		
		Map<Class<SUBTRGT>, EntityReadWriteExecutor<SUBTRGT, TRGTID>> map = Iterables.map(subPersisters, EntityReadWriteExecutor::getClassToPersist);
		TablePerClassPolymorphismWriter<TRGT, TRGTID, RIGHTTABLE, SUBTRGT> result = new TablePerClassPolymorphismWriter<>(
				templateWriter,
				map,
				dialect,
				connectionConfiguration
		);
		// TODO: build the subclasses here
		// TODO: we should call the consumer for each subclass
//		createdPersisterConsumer.accept(result);
		// TODO: try to not use the mainPersister
		
		return result;
	}
	
//	private <X, I, TT extends Table<TT>, EXTRATABLE extends Table<EXTRATABLE>> void addMapping(Entity<X, I, TT> entity,
////	                                                                                           InheritanceConfigurationResolver.ResolvedConfiguration<X, I> configuration,
//	                                                                                           Set<AbstractEntity.AbstractPropertyMapping<X, ?, TT>> mapping,
//	                                                                                           TT table) {
////		PropertyMappingResolver<X, TT> propertyMappingResolver = new PropertyMappingResolver<>(dialect.getColumnBinderRegistry());
////		Set<AbstractEntity.AbstractPropertyMapping<X, ?, TT>> mapping = propertyMappingResolver.resolve(
////				configuration.getMappingConfiguration().getPropertiesMapping(),
////				table,
////				configuration.getNamingConfiguration().getColumnNamingStrategy());
//		
//		Set<AbstractEntity.AbstractPropertyMapping<X, ?, EXTRATABLE>> extraTableMappings = new KeepOrderSet<>();
//		mapping.forEach(mappingPawn -> {
//			if (mappingPawn.getColumn().getTable() == table) {
//				entity.getPropertyMappingHolder().addMapping(mappingPawn);
//			} else {
//				extraTableMappings.add((AbstractEntity.AbstractPropertyMapping<X, ?, EXTRATABLE>) mappingPawn);
//			}
//		});
//		
//	}
	
	private <SUBENTITY, SUB_ENTITYID, SUBTABLE extends Table<SUBTABLE>>
	EntityReadWriteExecutor<SUBENTITY, SUB_ENTITYID> buildSubPersister(Entity<SUBENTITY, SUB_ENTITYID, SUBTABLE> subEntity) {
		Entity<SUBENTITY, SUB_ENTITYID, SUBTABLE> castSubEntity = subEntity;
		Duo<? extends ReadWritePropertyAccessPoint<SUBENTITY, ?>, ? extends Column<SUBTABLE, ?>> subVersioning = null;
//		if (versioning != null) {
//			// TODO: project the column of the template persister versioning to one of the sub entity table
//			subVersioning = null;
//		}
		
		// we could have used the IdMapping of the template persister, but we would need to temper with it to
		// adapt its Columns (accessors wouldn't need any modification since the template is a supertype) and re-instantiate
		// too many things, it isn't worth it
//		IdMapping<SUBENTITY, SUB_ENTITYID> subEntityIdMapping = skeletonAggregateResolver.createIdMapping(castSubEntity);
//		
//		PropertyMappingHolder<SUBENTITY, SUBTABLE> propertyMappingHolder = castSubEntity.getPropertyMappingHolder();
//		DefaultEntityMapping<SUBENTITY, SUB_ENTITYID, SUBTABLE> subEntityMapping = new DefaultEntityMapping<>(
//				castSubEntity.getEntityType(), castSubEntity.getTable(),
//				propertyMappingHolder.getWritablePropertiesPerAccessor(), propertyMappingHolder.getReadonlyPropertiesPerAccessor(),
//				subVersioning,
//				subEntityIdMapping,
//				null,
//				false
//		);
		
		EntityReadWriteExecutor<SUBENTITY, SUB_ENTITYID> subPersister = skeletonAggregateResolver.buildPersister(castSubEntity, new CreatedPersisterCollector<>());
//		SimpleRelationalEntityPersister<SUBENTITY, SUB_ENTITYID, SUBTABLE> subPersister = new SimpleRelationalEntityPersister<>(
//				subEntityMapping,
//				persistenceContext.getDialect(),
//				persistenceContext.getConnectionConfiguration());
		
		return subPersister;
	}
}
