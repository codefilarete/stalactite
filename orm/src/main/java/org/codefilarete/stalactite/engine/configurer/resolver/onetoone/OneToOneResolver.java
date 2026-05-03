package org.codefilarete.stalactite.engine.configurer.resolver.onetoone;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.property.CascadeOptions;
import org.codefilarete.stalactite.engine.cascade.AfterDeleteByIdSupport;
import org.codefilarete.stalactite.engine.cascade.AfterDeleteSupport;
import org.codefilarete.stalactite.engine.cascade.BeforeInsertSupport;
import org.codefilarete.stalactite.engine.configurer.manyToOne.ManyToOneOwnedBySourceConfigurer;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneConfigurerTemplate;
import org.codefilarete.stalactite.engine.configurer.onetoone.OrphanRemovalOnUpdate;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.Mapping.ShadowColumnValueProvider;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.function.Functions;
import org.codefilarete.tool.function.Predicates;

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
					ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> resolvedRelation = relation;
					ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister = skeletonAggregateResolver.buildPersister(resolvedRelation.getTargetEntity());
					createdPersisterConsumer.accept(resolvedRelation, targetPersister);
					
					boolean orphanRemoval = resolvedRelation.getRelationMode() == CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
					boolean writeAuthorized = resolvedRelation.getRelationMode() != CascadeOptions.RelationMode.READ_ONLY;
					if (writeAuthorized) {
						// NB: "delete removed" will be treated internally by updateCascade() and deleteCascade()
						
						ReadWritePropertyAccessPoint<SRC, TRGT> targetAccessor = resolvedRelation.getAccessor();
						// if cascade is mandatory, then adding nullability checking before insert
						if (resolvedRelation.isMandatory()) {
							result.addInsertListener(new OneToOneConfigurerTemplate.MandatoryRelationAssertBeforeInsertListener<>(targetAccessor));
							result.addUpdateListener(new ManyToOneOwnedBySourceConfigurer.MandatoryRelationAssertBeforeUpdateListener<>(targetAccessor));
						}
						
						KeyMapping<LEFTTABLE, RIGHTTABLE, JOINID> keyColumnsMapping = resolvedRelation.getJoin().getLeftKey().reference(resolvedRelation.getJoin().getRightKey());
						
						IdMapping<TRGT, TRGTID> trgtIdMapping = targetPersister.getMapping().getIdMapping();
						ShadowColumnValueProvider<SRC, LEFTTABLE> foreignKeyValueProvider = new ShadowColumnValueProvider<SRC, LEFTTABLE>() {
							@Override
							public Set<Column<LEFTTABLE, ?>> getColumns() {
								return keyColumnsMapping.getLeftColumns();
							}
							
							@Override
							public Map<Column<LEFTTABLE, ?>, ?> giveValue(SRC bean) {
								TRGT trgt = targetAccessor.get(bean);
								TRGTID trgtid = null;
								if (trgt != null) {
									trgtid = trgtIdMapping.getIdAccessor().getId(trgt);
								}
								Map<Column<RIGHTTABLE, ?>, ?> columnValues = trgtIdMapping.<RIGHTTABLE>getIdentifierAssembler().getColumnValues(trgtid);
								return Maps.innerJoinOnValuesAndKeys(keyColumnsMapping.getMapping(), columnValues);
							}
						};
						
						// Managing insert cascade
						result.<LEFTTABLE>getMapping().addShadowColumnInsert(foreignKeyValueProvider);
						// adding cascade treatment: before source insert, target is inserted to comply with foreign key constraint
						result.addInsertListener(new BeforeInsertSupport<>(targetPersister::persist, targetAccessor::get, Objects::nonNull));
						
						// Managing update cascade
						if (orphanRemoval) {
							result.addUpdateListener(new OrphanRemovalOnUpdate<>(targetPersister, targetAccessor));
						}
						result.<LEFTTABLE>getMapping().addShadowColumnUpdate(foreignKeyValueProvider);
						// - insert non-persisted target instances to fulfill foreign key requirement
						Function<SRC, TRGT> targetProviderAsFunction = new Functions.NullProofFunction<>(targetAccessor::get);
						result.addUpdateListener(new UpdateListener<SRC>() {
							
							private final Predicate<TRGT> newInstancePredicate = Predicates.<TRGT>predicate(Objects::nonNull).and(targetPersister.getMapping()::isNew);
							
							@Override
							public void beforeUpdate(Iterable<? extends Duo<SRC, SRC>> payloads, boolean allColumnsStatement) {
								// we only insert new instances
								targetPersister.persist(Iterables.stream(payloads).map(duo -> targetProviderAsFunction.apply(duo.getLeft()))
										.filter(newInstancePredicate).collect(Collectors.toSet()));
							}
						});
						// - after source update, target is updated too
						result.addUpdateListener(new UpdateListener<SRC>() {
							
							@Override
							public void afterUpdate(Iterable<? extends Duo<SRC, SRC>> payloads, boolean allColumnsStatement) {
								List<Duo<TRGT, TRGT>> targetsToUpdate = Iterables.collect(payloads,
										// targets of nullified relations don't need to be updated 
										e -> getTarget(e.getLeft()) != null,
										e -> getTargets(e.getLeft(), e.getRight()),
										ArrayList::new);
								targetPersister.update(targetsToUpdate, allColumnsStatement);
							}
							
							private Duo<TRGT, TRGT> getTargets(SRC modifiedTrigger, SRC unmodifiedTrigger) {
								return new Duo<>(getTarget(modifiedTrigger), getTarget(unmodifiedTrigger));
							}
							
							private TRGT getTarget(SRC src) {
								return targetAccessor.get(src);
							}
						});
						
						// Managing delete cascade
						if (orphanRemoval) {
							// adding cascade treatment: target is deleted after source deletion (because of foreign key constraint)
							result.addDeleteListener(new AfterDeleteSupport<>(targetPersister::delete, targetAccessor::get, Objects::nonNull));
							// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
							result.addDeleteByIdListener(new AfterDeleteByIdSupport<>(targetPersister::delete, targetAccessor::get, Objects::nonNull));
						} // else : no target entities deletion asked (no delete orphan) : nothing more to do than deleting the source entity
					}
				});
	}
}
