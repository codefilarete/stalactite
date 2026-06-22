package org.codefilarete.stalactite.engine.configurer.resolver.onetoone;

import java.util.function.Consumer;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.property.CascadeOptions;
import org.codefilarete.stalactite.engine.configurer.manytoone.ManyToOneConfigurer.MandatoryRelationAssertBeforeUpdateListener;
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
	
	/**
	 * Configure the given one-to-one relation by creating the persister for its target entity type and the engine that
	 * will maintain the cascades.
	 * It calls the given {@link Consumer} for the created target persister.
	 * 
	 * @param relationDefinition the one-to-one object that defines the relation to append
	 * @param sourcePersister the persister having the one-to-one relation
	 * @param createdPersisterConsumer a consumer to handle the created persister
	 * @param <SRC> type of the source entity
	 * @param <SRCID> type of the source entity's identifier
	 * @param <TRGT> type of the target entity
	 * @param <TRGTID> type of the target entity's identifier
	 * @param <LEFTTABLE> type of the left table in the one-to-one relation
	 * @param <RIGHTTABLE> type of the right table in the one-to-one relation
	 * @param <JOINID> type of the join identifier
	 */
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	void resolve(ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> relationDefinition,
	             ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
	             Consumer<ConfiguredRelationalPersister<TRGT, TRGTID>> createdPersisterConsumer) {
		
		assertConfigurationIsSupported(relationDefinition.getRelationMode());
		
		ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister = skeletonAggregateResolver.buildPersister(relationDefinition.getTargetEntity());
		createdPersisterConsumer.accept(targetPersister);
		
		ReadWritePropertyAccessPoint<SRC, TRGT> targetAccessor = relationDefinition.getAccessor();
		KeyMapping<LEFTTABLE, RIGHTTABLE, JOINID> foreignKeyColumnsMapping = relationDefinition.getJoin().getLeftKey().reference(relationDefinition.getJoin().getRightKey());
		
		AbstractOneToOneEngine<SRC, TRGT, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> oneToOneEngine;
		if (relationDefinition.isOwnedByTarget()) {
			oneToOneEngine = new OneToOneOwnedByTargetEngine<>(sourcePersister, targetPersister, targetAccessor, foreignKeyColumnsMapping.getMapping());
		} else {
			oneToOneEngine = new OneToOneOwnedBySourceEngine<>(sourcePersister, targetPersister, targetAccessor, foreignKeyColumnsMapping.getMapping());
		}
		
		boolean orphanRemoval = relationDefinition.getRelationMode() == CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
		boolean writeAuthorized = relationDefinition.getRelationMode() != CascadeOptions.RelationMode.READ_ONLY;
		if (writeAuthorized) {
			// if cascade is mandatory, then adding nullability checking before insert
			if (relationDefinition.isMandatory()) {
				sourcePersister.addInsertListener(new MandatoryRelationAssertBeforeInsertListener<>(targetAccessor));
				sourcePersister.addUpdateListener(new MandatoryRelationAssertBeforeUpdateListener<>(targetAccessor));
			}
			
			oneToOneEngine.addInsertCascade();
			oneToOneEngine.addUpdateCascade(orphanRemoval);
			oneToOneEngine.addDeleteCascade(orphanRemoval);
		} else {
			// even if write is not authorized, we still have to insert and update source-to-target link, because we are in relation-owned-by-source
			if (!relationDefinition.isOwnedByTarget()) {
				((OneToOneOwnedBySourceEngine) oneToOneEngine).addForeignKeyMaintainer();
			}
		}
	}
	
	private void assertConfigurationIsSupported(CascadeOptions.RelationMode maintenanceMode) {
		if (maintenanceMode == CascadeOptions.RelationMode.ASSOCIATION_ONLY) {
			throw new MappingConfigurationException(CascadeOptions.RelationMode.ASSOCIATION_ONLY + " is only relevant for one-to-many association");
		}
	}
}
