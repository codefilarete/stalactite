package org.codefilarete.stalactite.engine.configurer.resolver;

import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.configurer.model.AbstractEntity;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.PolymorphicEntity;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.PolymorphicSkeletonResolver;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class EntitySkeletonResolver {
	
	private final SkeletonAggregateResolver skeletonAggregateResolver;
	private final PolymorphicSkeletonResolver polymorphicSkeletonResolver;
	
	public EntitySkeletonResolver(PersistenceContext persistenceContext) {
		this.skeletonAggregateResolver = new SkeletonAggregateResolver(persistenceContext);
		this.polymorphicSkeletonResolver = new PolymorphicSkeletonResolver(persistenceContext);
	}
	
	public <TRGT, TRGTID, TRGTTABLE extends Table<TRGTTABLE>>
	DelegatingReadWriteEntityExecutor<TRGT, TRGTID> resolve(AbstractEntity<TRGT, TRGTID, TRGTTABLE> targetEntity,
															CreatedPersisterCollector<TRGT, TRGTID> persisterCollector) {
		
		DelegatingReadWriteEntityExecutor<TRGT, TRGTID> targetPersister;
		if (targetEntity instanceof PolymorphicEntity) {
			PolymorphicEntity<TRGT, TRGTID, TRGTTABLE> polymorphicEntity = (PolymorphicEntity<TRGT, TRGTID, TRGTTABLE>) targetEntity;
			targetPersister = polymorphicSkeletonResolver.resolve((PolymorphicEntity<TRGT, TRGTID, ?>) polymorphicEntity, persisterCollector);
		} else {
			targetPersister = skeletonAggregateResolver.resolve((Entity<TRGT, TRGTID, ?>) targetEntity, persisterCollector);
		}
		return targetPersister;
	}
}
