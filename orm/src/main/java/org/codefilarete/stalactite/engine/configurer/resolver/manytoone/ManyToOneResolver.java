package org.codefilarete.stalactite.engine.configurer.resolver.manytoone;

import java.util.Collection;

import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.property.CascadeOptions;
import org.codefilarete.stalactite.engine.EntityReadWriteExecutor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.configurer.manytoone.ManyToOneConfigurer.MandatoryRelationAssertBeforeInsertListener;
import org.codefilarete.stalactite.engine.configurer.manytoone.ManyToOneConfigurer.MandatoryRelationAssertBeforeUpdateListener;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.CreatedPersisterCollector;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.READ_ONLY;

/**
 * Handles write-cascade wiring for a resolved {@link ResolvedManyToManyRelation}.
 * Many-to-many relations always use an intermediary association table; there is no "owned by reverse side" variant.
 * Two sub-paths are supported:
 * <ul>
 *   <li>Non-indexed: {@link AssociationTable} + {@link org.codefilarete.stalactite.engine.configurer.resolver.onetomany.OneToManyWithAssociationTableEngine}</li>
 *   <li>Indexed: {@link IndexedAssociationTable} + {@link org.codefilarete.stalactite.engine.configurer.resolver.onetomany.OneToManyWithIndexedAssociationTableEngine}</li>
 * </ul>
 *
 * @author Guillaume Mary
 */
public class ManyToOneResolver {
	
	private final SkeletonAggregateResolver skeletonAggregateResolver;
	
	public ManyToOneResolver(SkeletonAggregateResolver skeletonAggregateResolver) {
		this.skeletonAggregateResolver = skeletonAggregateResolver;
	}
	
	/**
	 * Resolves the given many-to-many relation by building a persister for the target entity and wiring write cascades
	 * (INSERT / UPDATE / DELETE) onto the source persister via the appropriate association-table engine.
	 *
	 * @param resolvedRelation the resolved model relation carrying the join structure and cascade options
	 * @param sourcePersister the persister that owns the collection
	 * @param persisterCollector callback notified of every persister created by {@link SkeletonAggregateResolver} while building the target entity subtree
	 */
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<SRC>,
			LEFTTABLE extends Table<LEFTTABLE>,
			RIGHTTABLE extends Table<RIGHTTABLE>>
	void resolve(ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE> resolvedRelation,
	             EntityWriteExecutor<SRC, SRCID> sourcePersister,
	             CreatedPersisterCollector<TRGT, TRGTID> persisterCollector) {
		
		assertConfigurationIsSupported(resolvedRelation.getRelationMode());
		
		EntityReadWriteExecutor<TRGT, TRGTID> targetPersister = skeletonAggregateResolver.buildPersister(resolvedRelation.getTargetEntity(), persisterCollector);
		
		ManyToOneEngine<SRC, TRGT, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> engine = new ManyToOneEngine<>(
				sourcePersister,
				targetPersister,
				resolvedRelation.getAccessor(),
				resolvedRelation.getJoin().getKeyMapping().getMapping());
		
		boolean writeAuthorized = resolvedRelation.getRelationMode() != READ_ONLY;
		boolean orphanRemoval = resolvedRelation.getRelationMode() == CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
		if (writeAuthorized) {
			// if cascade is mandatory, then adding nullability checking before insert
			if (!resolvedRelation.isNullable()) {
				sourcePersister.addInsertListener(new MandatoryRelationAssertBeforeInsertListener<>(resolvedRelation.getAccessor()));
				sourcePersister.addUpdateListener(new MandatoryRelationAssertBeforeUpdateListener<>(resolvedRelation.getAccessor()));
			}
			engine.addInsertCascade();
			engine.addUpdateCascade(orphanRemoval);
			engine.addDeleteCascade(orphanRemoval);
		} else {
			// even if write is not authorized, we still have to insert and update source-to-target link, because we are in relation-owned-by-source
			engine.addForeignKeyMaintainer();
		}
	}
	
	private void assertConfigurationIsSupported(CascadeOptions.RelationMode maintenanceMode) {
		if (maintenanceMode == CascadeOptions.RelationMode.ASSOCIATION_ONLY) {
			throw new MappingConfigurationException(CascadeOptions.RelationMode.ASSOCIATION_ONLY + " is only relevant for many-to-one association");
		}
	}
}
