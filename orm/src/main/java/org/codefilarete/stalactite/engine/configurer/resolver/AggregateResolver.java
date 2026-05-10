package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.configurer.builder.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.configurer.builder.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.dslresolver.AggregateMetadataResolver;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.onetomany.OneToManyResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.onetoone.AggregateOneToOneAppender;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Duo;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.*;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

public class AggregateResolver {
	
	private final AggregateMetadataResolver aggregateMetadataResolver;
	private final PersisterRegistry persisterRegistry;
	private final SkeletonAggregateResolver skeletonAggregateResolver;
	private final AggregateOneToOneAppender oneToOneAppender;
	private final OneToManyResolver oneToManyResolver;
	
	public AggregateResolver(PersistenceContext persistenceContext) {
		this(persistenceContext, persistenceContext.getPersisterRegistry());
	}
	
	AggregateResolver(PersistenceContext persistenceContext, PersisterRegistry persisterRegistry) {
		this.aggregateMetadataResolver = new AggregateMetadataResolver(persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
		this.persisterRegistry = persisterRegistry;
		this.skeletonAggregateResolver = new SkeletonAggregateResolver(persistenceContext);
		this.oneToOneAppender = new AggregateOneToOneAppender(skeletonAggregateResolver);
		this.oneToManyResolver = new OneToManyResolver(skeletonAggregateResolver, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
	}
	
	public <C, I> EntityPersister<C, I> resolve(EntityMappingConfiguration<C, I> rootConfiguration) {
		Entity<C, I, ?> rootEntity = aggregateMetadataResolver.resolve(rootConfiguration);
		return build(rootEntity);
	}
	
	<C, I> ConfiguredRelationalPersister<C, I> build(Entity<C, I, ?> rootEntity) {
		// all this is left for compatibility with existing persister builders mechanism
		// it should be removed (or replaced by a close mechanism) at the very end of the implementation of the new persister build mechanism
		PersisterBuilderContext persisterBuilderContext = PersisterBuilderContext.CURRENT.get();
		boolean isInitiator = false;
		if (persisterBuilderContext == null) {
			persisterBuilderContext = new PersisterBuilderContext(persisterRegistry);
			PersisterBuilderContext.CURRENT.set(persisterBuilderContext);
			isInitiator = true;
		}
		
		try {
			ConfiguredRelationalPersister<C, I> result = buildPersister(rootEntity);
			// making aggregate persister available for external usage
			persisterRegistry.addPersister(result);
			if (isInitiator) {
				// This if is only there to execute code below only once, at the very end of persistence graph build,
				// even if it could seem counterintuitive since it compares "isInitiator" whereas this comment talks about end of graph :
				// because persistence configuration is made with a deep-first algorithm, this code (after doBuild()) will be called at the very end.
				persisterBuilderContext.getBuildLifeCycleListeners().forEach(BuildLifeCycleListener::afterBuild);
				persisterBuilderContext.getBuildLifeCycleListeners().forEach(BuildLifeCycleListener::afterAllBuild);
			}
			return result;
		} finally {
			if (isInitiator) {
				PersisterBuilderContext.CURRENT.remove();
			}
		}
	}
	
	private <B, C extends B, I, T extends Table<T>>
	ConfiguredRelationalPersister<C, I> buildPersister(Entity<C, I, T> rootEntity) {
		// TODO: check for ealready existing persister in the persistence context
		// TODO: wrap result in an OptimizedUpdatePersister
		// TODO: be inspired from DefaultPersisterBuilder.build()
		
		ConfiguredRelationalPersister<C, I> result = skeletonAggregateResolver.buildPersister(rootEntity);
		
		oneToOneAppender.append(rootEntity, result);
		appendOneToManys(rootEntity, result);
		
		return result;
	}
	
	
	
	<SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	void appendOneToManys(Entity<SRC, SRCID, LEFTTABLE> entity, ConfiguredRelationalPersister<SRC, SRCID> aggregatePersister) {
		Queue<Duo<ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>, ConfiguredRelationalPersister<TRGT, TRGTID>>> foundRelations = new ArrayDeque<>();
		Map<ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>, String> joinNames = new HashMap<>();
		
		oneToManyResolver.appendOneToManys(entity, aggregatePersister, new BiConsumer<ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>, ConfiguredRelationalPersister<TRGT, TRGTID>>() {
			
			@Override
			public void accept(ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> resolvedRelation, ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
				
				if (resolvedRelation.isOwnedByReverseSide()) {
					Set<Column<RIGHTTABLE, ?>> columnsToSelect;
					Function<ColumnedRow, Object> duplicateIdentifierProvider;
					if (resolvedRelation.isOrdered()) {
						columnsToSelect = new HashSet<>(targetPersister.<RIGHTTABLE>getMainTable().getPrimaryKey().getColumns());
						columnsToSelect.add(resolvedRelation.getIndexingMappedColumn());
						duplicateIdentifierProvider = (columnedRow) -> {
							TRGTID identifier = targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnedRow);
							Integer targetEntityIndex = columnedRow.get(resolvedRelation.getIndexingMappedColumn());
							return identifier + "-" + targetEntityIndex;
						};
					} else {
						columnsToSelect = Collections.emptySet();
						duplicateIdentifierProvider = (columnedRow) -> {
							TRGTID identifier = targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnedRow);
							return identifier;
						};
					}
					DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID> join = (DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID>) resolvedRelation.getJoin();
					String joinName = targetPersister.joinAsMany(
							EntityJoinTree.ROOT_JOIN_NAME,
							aggregatePersister,
							resolvedRelation.getAccessor(),
							join.getLeftKey(),
							join.getRightKey(),
							resolvedRelation.getRelationFixer(),
							duplicateIdentifierProvider,
							columnsToSelect,
							true,
							resolvedRelation.isFetchSeparately());
					foundRelations.add(new Duo<>(resolvedRelation, targetPersister));
					joinNames.put(resolvedRelation, joinName);
				} else {
					if (resolvedRelation.isOrdered()) {
						
					} else {
						// we join on the association table
						IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ?, SRCID, TRGTID> join = (IntermediaryRelationJoin) resolvedRelation.getJoin();
						String associationTableJoinNodeName = aggregatePersister.getEntityJoinTree().addPassiveJoin(ROOT_JOIN_NAME,
								join.getLeftKey(),
								join.getLeftAssociationKey(),
								OUTER,
								Collections.emptySet());
						
						targetPersister.joinAsMany(associationTableJoinNodeName, aggregatePersister, resolvedRelation.getAccessor(),
								join.getJoinTable().getManySideForeignKey(), join.getJoinTable().getManySideKey(),
								resolvedRelation.getRelationFixer(), null, true, false);
					}
					
				}
			}
		});
		
//		while (!foundRelations.isEmpty()) {
//			Duo<ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID>, ConfiguredRelationalPersister<TRGT, TRGTID>> foundRelation = foundRelations.poll();
//			deepOneToOnes(foundRelation, result, joinNames);
//		}
		
	}
}
