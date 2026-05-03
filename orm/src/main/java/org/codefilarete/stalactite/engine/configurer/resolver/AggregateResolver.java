package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.function.BiConsumer;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.configurer.builder.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.configurer.builder.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.dslresolver.AggregateMetadataResolver;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.onetoone.OneToOneResolver;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Duo;

public class AggregateResolver {
	
	private final AggregateMetadataResolver aggregateMetadataResolver;
	private final PersisterRegistry persisterRegistry;
	private final SkeletonAggregateResolver skeletonAggregateResolver;
	private final OneToOneResolver oneToOneResolver;
	
	public AggregateResolver(PersistenceContext persistenceContext) {
		this(persistenceContext, persistenceContext.getPersisterRegistry());
	}
	
	AggregateResolver(PersistenceContext persistenceContext, PersisterRegistry persisterRegistry) {
		this.aggregateMetadataResolver = new AggregateMetadataResolver(persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
		this.persisterRegistry = persisterRegistry;
		this.skeletonAggregateResolver = new SkeletonAggregateResolver(persistenceContext);
		this.oneToOneResolver = new OneToOneResolver(skeletonAggregateResolver);
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
		
		appendOneToOnes(rootEntity, result);
		
		return result;
	}
	
	<SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	void appendOneToOnes(Entity<SRC, SRCID, LEFTTABLE> entity, ConfiguredRelationalPersister<SRC, SRCID> result) {
		Queue<Duo<ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID>, ConfiguredRelationalPersister<TRGT, TRGTID>>> foundRelations = new ArrayDeque<>();
		Map<ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID>, String> joinNames = new HashMap<>();
		
		oneToOneResolver.appendOneToOnes(entity, result, new BiConsumer<ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID>, ConfiguredRelationalPersister<TRGT, TRGTID>>() {
			
			@Override
			public void accept(ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> resolvedRelation, ConfiguredRelationalPersister<TRGT, TRGTID> configuredRelationalPersister) {
				String joinName = configuredRelationalPersister.joinAsOne(result,
						resolvedRelation.getAccessor(),
						resolvedRelation.getJoin().getLeftKey(),
						resolvedRelation.getJoin().getRightKey(),
						null,
						resolvedRelation.getBeanRelationFixer(),
						true,
						resolvedRelation.isFetchSeparately());
				
				foundRelations.add(new Duo<>(resolvedRelation, configuredRelationalPersister));
				joinNames.put(resolvedRelation, joinName);
			}
		});
		
		while (!foundRelations.isEmpty()) {
			Duo<ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID>, ConfiguredRelationalPersister<TRGT, TRGTID>> foundRelation = foundRelations.poll();
			deepOneToOnes(foundRelation, result, joinNames);
		}
		
	}
	
	private <SRC, SRCID, TRGT, TRGTID, ANOTHER, ANOTHERID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, ANOTHERTABLE extends Table<ANOTHERTABLE>, JOINID>
	void deepOneToOnes(Duo<ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID>, ConfiguredRelationalPersister<TRGT, TRGTID>> foundRelation,
	                   ConfiguredRelationalPersister<SRC, SRCID> result,
	                   Map<ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID>, String> joinNames) {
		oneToOneResolver.appendOneToOnes(foundRelation.getLeft().getTargetEntity(), foundRelation.getRight(),
				new BiConsumer<ResolvedOneToOneRelation<TRGT, ANOTHER, RIGHTTABLE, ANOTHERTABLE, JOINID>, ConfiguredRelationalPersister<ANOTHER, ANOTHERID>>() {
					
					@Override
					public void accept(ResolvedOneToOneRelation<TRGT, ANOTHER, RIGHTTABLE, ANOTHERTABLE, JOINID> resolvedRelation, ConfiguredRelationalPersister<ANOTHER, ANOTHERID> configuredRelationalPersister) {
						
						AccessorChain<SRC, TRGT> accessorChain = new AccessorChain<>(foundRelation.getLeft().getAccessor(), resolvedRelation.getAccessor());
						accessorChain.setNullValueHandler(AccessorChain.RETURN_NULL);
						EntityMappingAdapter<ANOTHER, ANOTHERID, ANOTHERTABLE> strategy = new EntityMappingAdapter<>(configuredRelationalPersister.<ANOTHERTABLE>getMapping());
						BeanRelationFixer<TRGT, ANOTHER> beanRelationFixer = (target, input) -> {
							resolvedRelation.getBeanRelationFixer().apply(target, input);
						};
						String createdJoinNodeName = result.getEntityJoinTree().addRelationJoin(
								joinNames.get(foundRelation.getLeft()),
								// because joinAsOne can be called in either case of owned relation or reversely owned relation, generics can't be set correctly,
								// so we simply cast first argument
								strategy,
								accessorChain,
								resolvedRelation.getJoin().getLeftKey(),
								resolvedRelation.getJoin().getRightKey(),
								null,
								resolvedRelation.isMandatory() ? EntityJoinTree.JoinType.INNER : EntityJoinTree.JoinType.OUTER,
								beanRelationFixer,
								Collections.emptySet());
					}
				});
	}
}
