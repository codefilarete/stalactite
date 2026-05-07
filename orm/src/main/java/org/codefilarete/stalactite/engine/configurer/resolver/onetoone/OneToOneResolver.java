package org.codefilarete.stalactite.engine.configurer.resolver.onetoone;

import java.util.function.BiConsumer;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.property.CascadeOptions;
import org.codefilarete.stalactite.engine.configurer.manyToOne.ManyToOneOwnedBySourceConfigurer.MandatoryRelationAssertBeforeUpdateListener;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneConfigurerTemplate.MandatoryRelationAssertBeforeInsertListener;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.onetoone.AbstractOneToOneEngine;
import org.codefilarete.stalactite.engine.runtime.onetoone.OneToOneOwnedBySourceEngine;
import org.codefilarete.stalactite.engine.runtime.onetoone.OneToOneOwnedByTargetEngine;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class OneToOneResolver {
	
	private final SkeletonAggregateResolver skeletonAggregateResolver;
	
	public OneToOneResolver(SkeletonAggregateResolver skeletonAggregateResolver) {
		this.skeletonAggregateResolver = skeletonAggregateResolver;
	}
	
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	void appendOneToOnes(Entity<SRC, SRCID, LEFTTABLE> entity, ConfiguredRelationalPersister<SRC, SRCID> result, BiConsumer<ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID>, ConfiguredRelationalPersister<TRGT, TRGTID>> createdPersisterConsumer) {
		entity.getRelations().stream()
				.filter(ResolvedOneToOneRelation.class::isInstance)
				.map(ResolvedOneToOneRelation.class::cast)
				.forEach(relation -> {
					
					assertConfigurationIsSupported(relation.getRelationMode());
					
					ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> resolvedRelation = relation;
					ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister = skeletonAggregateResolver.buildPersister(resolvedRelation.getTargetEntity());
					createdPersisterConsumer.accept(resolvedRelation, targetPersister);
					
					ReadWritePropertyAccessPoint<SRC, TRGT> targetAccessor = resolvedRelation.getAccessor();
					KeyMapping<LEFTTABLE, RIGHTTABLE, JOINID> foreignKeyColumnsMapping = resolvedRelation.getJoin().getLeftKey().reference(resolvedRelation.getJoin().getRightKey());
					
					AbstractOneToOneEngine<SRC, TRGT, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> oneToOneEngine;
					if (resolvedRelation.isOwnedByTarget()) {
						oneToOneEngine = new OneToOneOwnedByTargetEngine<>(result, targetPersister, targetAccessor, foreignKeyColumnsMapping.getMapping());
					} else {
						oneToOneEngine = new OneToOneOwnedBySourceEngine<>(result, targetPersister, targetAccessor, foreignKeyColumnsMapping.getMapping());
					}
					
					boolean orphanRemoval = resolvedRelation.getRelationMode() == CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
					boolean writeAuthorized = resolvedRelation.getRelationMode() != CascadeOptions.RelationMode.READ_ONLY;
					if (writeAuthorized) {
						// if cascade is mandatory, then adding nullability checking before insert
						if (resolvedRelation.isMandatory()) {
							result.addInsertListener(new MandatoryRelationAssertBeforeInsertListener<>(targetAccessor));
							result.addUpdateListener(new MandatoryRelationAssertBeforeUpdateListener<>(targetAccessor));
						}
						
						oneToOneEngine.addInsertCascade();
						oneToOneEngine.addUpdateCascade(orphanRemoval);
						oneToOneEngine.addDeleteCascade(orphanRemoval);
					} else {
						// even if write is not authorized, we still have to insert and update source-to-target link, because we are in relation-owned-by-source
						if (!resolvedRelation.isOwnedByTarget()) {
							((OneToOneOwnedBySourceEngine) oneToOneEngine).addForeignKeyMaintainer();
						}
					}
				});
	}
	
	private void assertConfigurationIsSupported(CascadeOptions.RelationMode maintenanceMode) {
		if (maintenanceMode == CascadeOptions.RelationMode.ASSOCIATION_ONLY) {
			throw new MappingConfigurationException(CascadeOptions.RelationMode.ASSOCIATION_ONLY + " is only relevant for one-to-many association");
		}
	}
}
