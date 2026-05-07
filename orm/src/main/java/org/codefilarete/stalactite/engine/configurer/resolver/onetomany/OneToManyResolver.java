package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.onetomany.AbstractOneToManyEngine;
import org.codefilarete.stalactite.engine.runtime.onetomany.IndexedMappedManyRelationDescriptor;
import org.codefilarete.stalactite.engine.runtime.onetomany.MappedManyRelationDescriptor;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithIndexedMappedAssociationEngine;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Maps;

import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.ASSOCIATION_ONLY;
import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.READ_ONLY;

public class OneToManyResolver {
	
	private final SkeletonAggregateResolver skeletonAggregateResolver;
	
	public OneToManyResolver(SkeletonAggregateResolver skeletonAggregateResolver) {
		this.skeletonAggregateResolver = skeletonAggregateResolver;
	}
	
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	void appendOneToManys(Entity<SRC, SRCID, LEFTTABLE> entity, ConfiguredRelationalPersister<SRC, SRCID> result, BiConsumer<ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>, ConfiguredRelationalPersister<TRGT, TRGTID>> createdPersisterConsumer) {
		entity.getRelations().stream()
				.filter(ResolvedOneToManyRelation.class::isInstance)
				.map(ResolvedOneToManyRelation.class::cast)
				.forEach(relation -> {
					ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> resolvedRelation = relation;
					ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister = skeletonAggregateResolver.buildPersister(resolvedRelation.getTargetEntity());
					createdPersisterConsumer.accept(resolvedRelation, targetPersister);
					
					KeyMapping<LEFTTABLE, RIGHTTABLE, SRCID> foreignKeyColumnsMapping = resolvedRelation.getJoin().getLeftKey().reference(resolvedRelation.getJoin().getRightKey());
					
					AbstractOneToManyEngine<SRC, TRGT, SRCID, TRGTID, Collection<TRGT>> oneToManyEngine = null;
					if (resolvedRelation.isOwnedByReverseSide()) {
						
						Function<SRCID, Map<Column<RIGHTTABLE, ?>, ?>> reverseColumnsValueProvider;
						reverseColumnsValueProvider = srcid -> {
							IdentifierAssembler<SRCID, LEFTTABLE> identifierAssembler = result.getMapping().getIdMapping().getIdentifierAssembler();
							Map<Column<LEFTTABLE, ?>, ?> columnValues = identifierAssembler.getColumnValues(srcid);
							return Maps.innerJoin(foreignKeyColumnsMapping.getMapping(), columnValues);
						};
						Set<Column<RIGHTTABLE, ?>> reverseColumns = resolvedRelation.getJoin().getRightKey().getColumns();
						
						if (resolvedRelation.isOrdered()) {
							IndexedMappedManyRelationDescriptor manyRelationDescriptor = new IndexedMappedManyRelationDescriptor<>(
									relation.getAccessor(),
									resolvedRelation.getComponentFactory(),
									resolvedRelation.getMappedByAccessor(),
									resolvedRelation.getJoin().getRightKey(),
									resolvedRelation.getIndexingColumn(),
									result.getMapping()::getId,
									targetPersister.getMapping()::getId,
									resolvedRelation.getRelationMode() == ASSOCIATION_ONLY,
									resolvedRelation.getRelationMode() == ALL_ORPHAN_REMOVAL);
							
							
							oneToManyEngine = new OneToManyWithIndexedMappedAssociationEngine<>(
									targetPersister,
									manyRelationDescriptor,
									result,
									reverseColumns,
									reverseColumnsValueProvider);
						} else {
							MappedManyRelationDescriptor manyRelationDescriptor = new MappedManyRelationDescriptor<>(
									relation.getAccessor(),
									resolvedRelation.getComponentFactory(),
									resolvedRelation.getMappedByAccessor(),
									resolvedRelation.getJoin().getRightKey(),
									resolvedRelation.getRelationMode() == ASSOCIATION_ONLY,
									resolvedRelation.getRelationMode() == ALL_ORPHAN_REMOVAL);
							oneToManyEngine = new OneToManyWithMappedAssociationEngine<>(
									targetPersister,
									manyRelationDescriptor,
									result,
									reverseColumns,
									reverseColumnsValueProvider);
						}
					} else {
//						oneToOneEngine = new OneToOneOwnedBySourceEngine<>(result, targetPersister, targetAccessor, foreignKeyColumnsMapping.getMapping());
					}
					
					boolean writeAuthorized = resolvedRelation.getRelationMode() != READ_ONLY;
					if (writeAuthorized) {
						oneToManyEngine.addInsertCascade(targetPersister);
						oneToManyEngine.addUpdateCascade(targetPersister);
						oneToManyEngine.addDeleteCascade(targetPersister);
					} else {
//						// even if write is not authorized, we still have to insert and update source-to-target link, because we are in relation-owned-by-source
//						if (!resolvedRelation.isOwnedByReverseSide()) {
//							((OneToOneOwnedBySourceEngine) oneToOneEngine).addForeignKeyMaintainer();
//						}
					}
				});
	}
}
