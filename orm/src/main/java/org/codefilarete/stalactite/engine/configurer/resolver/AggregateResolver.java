package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.EntityReadWriteExecutor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.configurer.builder.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.configurer.builder.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.dslresolver.AggregateMetadataResolver;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecord;
import org.codefilarete.stalactite.engine.configurer.model.AbstractEntity;
import org.codefilarete.stalactite.engine.configurer.model.AncestorJoin;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.EntityRelation;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.MappingJoin;
import org.codefilarete.stalactite.engine.configurer.model.PolymorphicEntity;
import org.codefilarete.stalactite.engine.configurer.model.RelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToOneRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.elementcollection.AggregateElementCollectionAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.elementcollection.ElementCollectionResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.elementcollection.ElementCollectionResolver.ElementRecordPersister;
import org.codefilarete.stalactite.engine.configurer.resolver.manytomany.AggregateManyToManyAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.manytomany.ManyToManyResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.manytoone.AggregateManyToOneAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.manytoone.ManyToOneResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.map.AggregateMapAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.map.MapResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.onetomany.AggregateOneToManyAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.onetomany.OneToManyResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.onetoone.AggregateOneToOneAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.onetoone.OneToOneResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.tableperclass.TablePerClassAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.tableperclass.TablePerClassResolver;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.tableperclass.TablePerClassPolymorphismReader;
import org.codefilarete.stalactite.engine.runtime.tableperclass.TablePerClassPolymorphismWriter;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

/**
 * Builds a runtime {@link ConfiguredRelationalPersister} for a whole aggregate of entities from its resolved metadata.
 * This is the top-level entry point of the persister build pipeline.
 * It orchestrates two distinct phases:
 * - Metadata resolution: an {@link EntityMappingConfiguration} (the DSL configuration) is turned
 * into a structural {@link Entity} model by the {@link AggregateMetadataResolver}.
 * - Persister assembly: the {@link Entity} model is turned into a persister, by first building
 * the structural "bones" through the {@link SkeletonAggregateResolver} (identifier, direct/embedded properties,
 * inheritance and extra tables), then grafting every relation onto it through the dedicated {@code Aggregate*Appender}
 * collaborators.
 * - Responsibilities are intentionally split: this class owns the relation grafting traversal and the build
 * lifecycle, while the structural mapping is delegated to {@link SkeletonAggregateResolver} and each relation kind is
 * delegated to its own appender (one-to-one, one-to-many, many-to-many, many-to-one, element collection and map).
 * 
 * @author Guillaume Mary
 * @see SkeletonAggregateResolver
 * @see AggregateMetadataResolver
 */
public class AggregateResolver {
	
	private final PersistenceContext persistenceContext;
	private final AggregateMetadataResolver aggregateMetadataResolver;
	/** Registry in which built persisters are published so they become available for external lookup. */
	private final PersisterRegistry persisterRegistry;
	/** Builds the structural "bones" of an entity (identifier, properties, inheritance, extra tables) without relations. */
	private final SkeletonAggregateResolver skeletonAggregateResolver;
	/** Grafts one-to-one relations onto the aggregate persister. */
	private final AggregateOneToOneAppender oneToOneAppender;
	/** Grafts one-to-many relations onto the aggregate persister. */
	private final AggregateOneToManyAppender oneToManyAppender;
	/** Grafts many-to-many relations onto the aggregate persister. */
	private final AggregateManyToManyAppender manyToManyAppender;
	/** Grafts many-to-one relations onto the aggregate persister. */
	private final AggregateManyToOneAppender manyToOneAppender;
	/** Grafts element-collection relations onto the aggregate persister. */
	private final AggregateElementCollectionAppender elementCollectionAppender;
	/** Grafts map relations onto the aggregate persister. */
	private final AggregateMapAppender mapAppender;
	
	private final OneToOneResolver oneToOneResolver;
	private final OneToManyResolver oneToManyResolver;
	private final ManyToManyResolver manyToManyResolver;
	private final ManyToOneResolver manyToOneResolver;
	private final ElementCollectionResolver elementCollectionResolver;
	private final MapResolver mapResolver;
	
	/**
	 * Creates a resolver bound to the given {@link PersistenceContext}, publishing built persisters into the context's
	 * own {@link PersisterRegistry}.
	 * 
	 * @param persistenceContext the persistence context providing the dialect, connection configuration and persister registry
	 */
	public AggregateResolver(PersistenceContext persistenceContext) {
		this(persistenceContext, persistenceContext.getPersisterRegistry());
	}
	
	/**
	 * Creates a resolver bound to the given {@link PersistenceContext} but publishing built persisters into the supplied
	 * {@link PersisterRegistry}. Package-private to allow tests to provide an isolated registry.
	 * 
	 * @param persistenceContext the persistence context providing the dialect and connection configuration
	 * @param persisterRegistry the registry into which built persisters are published
	 */
	AggregateResolver(PersistenceContext persistenceContext, PersisterRegistry persisterRegistry) {
		this.persistenceContext = persistenceContext;
		this.aggregateMetadataResolver = new AggregateMetadataResolver(persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
		this.persisterRegistry = persisterRegistry;
		this.skeletonAggregateResolver = new SkeletonAggregateResolver(persistenceContext);
		this.oneToOneAppender = new AggregateOneToOneAppender();
		this.oneToManyAppender = new AggregateOneToManyAppender();
		this.manyToManyAppender = new AggregateManyToManyAppender();
		this.manyToOneAppender = new AggregateManyToOneAppender();
		this.elementCollectionAppender = new AggregateElementCollectionAppender();
		this.mapAppender = new AggregateMapAppender();
		
		this.oneToOneResolver = new OneToOneResolver(skeletonAggregateResolver);
		this.oneToManyResolver = new OneToManyResolver(skeletonAggregateResolver, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
		this.manyToManyResolver = new ManyToManyResolver(skeletonAggregateResolver, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
		this.manyToOneResolver = new ManyToOneResolver(skeletonAggregateResolver);
		this.elementCollectionResolver = new ElementCollectionResolver(persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
		this.mapResolver = new MapResolver(skeletonAggregateResolver, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
	}
	
	/**
	 * Resolves and builds the persister for the aggregate rooted at the given DSL configuration.
	 * Convenience entry point combining metadata resolution ({@link AggregateMetadataResolver#resolve(EntityMappingConfiguration)})
	 * and persister assembly ({@link #build(AbstractEntity)}).
	 * 
	 * @param rootConfiguration the DSL configuration describing the aggregate root
	 * @param <C> the root entity type
	 * @param <I> the root entity identifier type
	 * @return the built {@link EntityPersister} for the aggregate
	 */
	public <C, I> EntityPersister<C, I> resolve(EntityMappingConfiguration<C, I> rootConfiguration) {
		AbstractEntity<C, I, ?> rootEntity = aggregateMetadataResolver.resolve(rootConfiguration);
		return build(rootEntity);
	}
	
	/**
	 * Builds the persister for the given {@link Entity} model and manages the build lifecycle.
	 * A {@link PersisterBuilderContext} is set up as a {@link ThreadLocal} for the duration of the build, mainly for
	 * compatibility with the existing (legacy) persister builders. The very first invocation on the thread (the
	 * initiator) is the one that created the context; because persistence configuration is processed depth-first, the
	 * initiator's frame is also the last to complete, so it is responsible for firing the terminal
	 * lifecycle callbacks once the whole graph is built: {@link BuildLifeCycleListener#afterBuild()} then
	 * {@link BuildLifeCycleListener#afterAllBuild()}.
	 * The built persister is registered into {@link PersisterRegistry} so it becomes externally available.
	 * 
	 * @param rootEntity the resolved metadata of the aggregate root
	 * @param <C> the root entity type
	 * @param <I> the root entity identifier type
	 * @return the built {@link ConfiguredRelationalPersister} for the aggregate
	 */
	<C, I> EntityPersister<C, I> build(AbstractEntity<C, I, ?> rootEntity) {
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
			EntityPersister<C, I> result = buildPersister(rootEntity);
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
	
	/**
	 * Assembles the persister for the given {@link Entity} model: first builds the structural skeleton, then appends
	 * all relations onto it.
	 * 
	 * @param rootEntity the resolved metadata of the aggregate root
	 * @param <C> the root entity type
	 * @param <I> the root entity identifier type
	 * @param <T> the root entity table type
	 * @return the assembled {@link ConfiguredRelationalPersister}
	 */
	private <C, I, T extends Table<T>>
	EntityPersister<C, I> buildPersister(AbstractEntity<C, I, T> rootEntity) {
		// TODO: check for ealready existing persister in the persistence context
		// TODO: wrap result in an OptimizedUpdatePersister
		// TODO: be inspired from DefaultPersisterBuilder.build()
		
		EntityWriteExecutor<C, I> rootWriter = null;
		ConfiguredEntityReader<C, I> aggregateReader = null;
		CreatedPersisterCollector<C, I> rootPersisterCollector = new CreatedPersisterCollector<>();
		if (rootEntity instanceof Entity) {
			rootWriter = skeletonAggregateResolver.buildPersister(rootEntity, rootPersisterCollector);
			aggregateReader = new EntityReader<>(rootWriter.<T>getMapping(),
					persistenceContext.getConnectionProvider(),
					persistenceContext.getDialect());
		} else {
			if (rootEntity instanceof PolymorphicEntity) {
				PolymorphicEntity<C, I, T> polymorphicEntity = (PolymorphicEntity<C, I, T>) rootEntity;
				TablePerClassResolver tablePerClassResolver = new TablePerClassResolver(skeletonAggregateResolver, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
				TablePerClassPolymorphismWriter<C, I, T, C> tablePerClassPolymorphismWriter = tablePerClassResolver.resolve(polymorphicEntity, rootPersisterCollector);
				rootWriter = tablePerClassPolymorphismWriter;
				EntityReader<C, I, T> reader = new EntityReader<>(rootWriter.<T>getMapping(),
						persistenceContext.getConnectionProvider(),
						persistenceContext.getDialect());
				Map<? extends Class<C>, ConfiguredEntityReader<C, I>> map = Iterables.map(tablePerClassPolymorphismWriter.getSubEntitiesPersisters().entrySet(), Map.Entry::getKey, entry -> {
							return new EntityReader<>(entry.getValue().<T>getMapping(),
									persistenceContext.getConnectionProvider(),
									persistenceContext.getDialect());
						});
				aggregateReader = new TablePerClassPolymorphismReader<C, I, T>(reader,
						map,
						persistenceContext.getConnectionProvider(),
						persistenceContext.getDialect());
				TablePerClassAppender tablePerClassAppender = new TablePerClassAppender();
				tablePerClassAppender.append(aggregateReader.getEntityJoinTree(), null, rootWriter, null);
			} else {
				throw new UnsupportedOperationException("Unsupported entity type: " + rootEntity.getClass());
			}
		}
		// TODO: wrap the result into a selector that redirect the select method and projections to a finder instance
		Map<MappingJoin<?, ?, ?>, Object> createdPersistersMap = (Map) appendWriteCascades(rootEntity, rootWriter);
		createdPersistersMap.put(null, rootPersisterCollector);
		composeLoadTree(rootEntity, aggregateReader, createdPersistersMap);
		
		DelegatingReadWriteEntityExecutor<C, I> almostResult = new DelegatingReadWriteEntityExecutor<>(rootWriter, aggregateReader);
		
		Set<Table<?>> tables = collectTables(rootEntity);
		
		ConfiguredPersister<C, I> result = wrapToConfiguredPersister(almostResult, tables);
		
		return result;
	}
	
	private <C, I> ConfiguredPersister<C, I> wrapToConfiguredPersister(DelegatingReadWriteEntityExecutor<C, I> almostResult, Set<? extends Table<?>> tables) {
		return new DelegatingConfiguredPersister<>(almostResult, tables);
	}
	
	/**
	 * Traverses the whole relation graph of the aggregate and grafts every relation onto the given aggregate persister.
	 * The traversal is an explicit, stack-based breadth-first walk (no recursion). Each visited node is represented by
	 * an {@link CascadePoint} that carries everything the next iteration needs: the relation-owning {@link Entity},
	 * its persister, the name of the parent join node to attach to, and the accessor used to shift property paths at depth.
	 * For every relation owned by the current node, the matching {@code Aggregate*Appender} is invoked. One-to-one,
	 * one-to-many, many-to-many and many-to-one appenders return a new {@link CascadePoint} (the target becomes a new
	 * node to descend into) which is pushed back onto the stack; element-collection and map relations are leaf relations
	 * and therefore do not produce a new node.
	 * 
	 * Note: breadth-first vs depth-first ordering is not significant here, since each {@link CascadePoint} self-contains
	 * the context required to resume at the correct depth.
	 * 
	 * @param rootEntity the aggregate root entity to start the traversal from
	 * @param aggregatePersister the persister onto which every relation join must be grafted
	 * @param <SRC> the source (left) entity type
	 * @param <SRCID> the source entity identifier type
	 * @param <TRGT> the target entity type
	 * @param <TRGTID> the target entity identifier type
	 * @param <S> the collection type for to-many/element-collection relations
	 * @param <LEFTTABLE> the left (source) table type of the join
	 * @param <RIGHTTABLE> the right (target) table type of the join
	 * @param <JOINID> the join column type
	 */
	<SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	Map<MappingJoin<LEFTTABLE, RIGHTTABLE, ?>, Object> appendWriteCascades(AbstractEntity<SRC, SRCID, LEFTTABLE> rootEntity,
	                                                                       EntityWriteExecutor<SRC, SRCID> aggregatePersister) {
		
		// Iterating over all the relations of the tree (starting from the given root entity).
		// It's made by a breadth-first algorithm with node stacking, no recursion here.
		// Bread-first principle shouldn't be important because we maintain some CascadePoints to keep track of the
		// depth and the necessary information for the next iteration.
		Queue<CascadePoint<?, ?, ?>> relationStack = new ArrayDeque<>();
		// We start by a kind of fake seed, without relation, because we don't have any for the root entity
		relationStack.add(new CascadePoint<>(rootEntity, aggregatePersister));
		
		Map<MappingJoin<LEFTTABLE, RIGHTTABLE, ?>, Object> result = new HashMap<>();
		
		while (!relationStack.isEmpty()) {
			
			// - create the polymorphic template entity : this is done by skeletonAggregateResolver in TablePerClassResolver
			// at this step it has no select concept, nor union clause : it is expected to be joined in TablePerClassAppender (which calls TablePerClassResolver)
			// how to add a join to it ?
			// how to we call it below ? without conflicting with relations ?
			
			CascadePoint<?, ?, ?> assemblyPawn = relationStack.poll();
			EntityWriteExecutor<SRC, SRCID> relationOwnerPersister = (EntityWriteExecutor<SRC, SRCID>) assemblyPawn.getRelationOwnerPersister();
			assemblyPawn.getRelationOwnerEntity().getRelations()
					.forEach(relationPawn -> {
						if (relationPawn instanceof ResolvedOneToOneRelation) {
							ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> localRelation = (ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID>) relationPawn;
							CreatedPersisterCollector<TRGT, TRGTID> createdPersisterCollector = new CreatedPersisterCollector<>();
							oneToOneResolver.resolve(
									localRelation,
									relationOwnerPersister,
									createdPersisterCollector,
									localRelation.getAccessor());
							result.put(localRelation, createdPersisterCollector);
							relationStack.add(new CascadePoint(localRelation.getTargetEntity(), createdPersisterCollector.getPersister()));
						}
						if (relationPawn instanceof ResolvedOneToManyRelation) {
							ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> localRelation = (ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn;
							CreatedPersisterCollector<TRGT, TRGTID> createdPersisterCollector = new CreatedPersisterCollector<>();
							oneToManyResolver.resolve(
									localRelation,
									relationOwnerPersister,
									createdPersisterCollector);
							result.put(localRelation, createdPersisterCollector);
							relationStack.add(new CascadePoint(localRelation.getTargetEntity(), createdPersisterCollector.getPersister()));
						}
						if (relationPawn instanceof ResolvedManyToManyRelation) {
							ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> localRelation = (ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn;
							CreatedPersisterCollector<TRGT, TRGTID> createdPersisterCollector = new CreatedPersisterCollector<>();
							manyToManyResolver.resolve(
									localRelation,
									relationOwnerPersister,
									createdPersisterCollector);
							result.put(localRelation, createdPersisterCollector);
							relationStack.add(new CascadePoint(localRelation.getTargetEntity(), createdPersisterCollector.getPersister()));
						}
						if (relationPawn instanceof ResolvedManyToOneRelation) {
							ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE> localRelation = (ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn;
							CreatedPersisterCollector<TRGT, TRGTID> createdPersisterCollector = new CreatedPersisterCollector<>();
							manyToOneResolver.resolve(
									localRelation,
									relationOwnerPersister,
									createdPersisterCollector);
							result.put(localRelation, createdPersisterCollector);
							relationStack.add(new CascadePoint(localRelation.getTargetEntity(), createdPersisterCollector.getPersister()));
						}
						if (relationPawn instanceof ResolvedElementCollectionRelation) {
							CreatedPersisterCollector<ElementRecord<TRGT, SRCID>, ElementRecord<TRGT, SRCID>> createdPersisterCollector = new CreatedPersisterCollector<>();
							ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, LEFTTABLE, RIGHTTABLE, ElementRecord<TRGT, SRCID>> localRelation = (ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, LEFTTABLE, RIGHTTABLE, ElementRecord<TRGT, SRCID>>) relationPawn;
							elementCollectionResolver.resolve(
									localRelation,
									relationOwnerPersister,
									createdPersisterCollector);
							result.put(localRelation, createdPersisterCollector);
						}
						if (relationPawn instanceof ResolvedMapRelation) {
							ResolvedMapRelation<SRC, SRCID, Object, Object, Object, Object, Map<Object, Object>, LEFTTABLE, RIGHTTABLE, ?, ?> localRelation = (ResolvedMapRelation<SRC, SRCID, Object, Object, Object, Object, Map<Object, Object>, LEFTTABLE, RIGHTTABLE, ?, ?>) relationPawn;
							MapCreatedPersisterCollector<SRCID, Object, Object, Object, Object, RIGHTTABLE, Object, Object> mapCreatedPersisterCollector = new MapCreatedPersisterCollector<>();
							mapResolver.resolve(
									localRelation,
									relationOwnerPersister,
									mapCreatedPersisterCollector);
							result.put(localRelation, mapCreatedPersisterCollector);
							if (localRelation.getKeyEntityDefinition() != null) {
								relationStack.add(new CascadePoint(localRelation.getKeyEntityDefinition().getEntity(), mapCreatedPersisterCollector.getKeyPersisterCollector().getPersister()));
							}
							if (localRelation.getValueEntityDefinition() != null) {
								relationStack.add(new CascadePoint(localRelation.getValueEntityDefinition().getEntity(), mapCreatedPersisterCollector.getValuePersisterCollector().getPersister()));
							}
						}
					});
		}
		
		return result;
	}
	
	<SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	void composeLoadTree(AbstractEntity<SRC, SRCID, LEFTTABLE> rootEntity,
	                     EntityReader<SRC, SRCID, ?> aggregatePersister,
	                     Map<MappingJoin<?, ?, ?>, Object> createdPersisters) {
		
		Queue<GraftPoint<?, ?, ?, ?, ?>> relationStack = new ArrayDeque<>();
		relationStack.add(new GraftPoint<>(rootEntity, aggregatePersister, ROOT_JOIN_NAME, aggregatePersister.getEntityJoinTree()));
		new SkeletonAggregateAppender()
				.appendInheritance((CreatedPersisterCollector<SRC, SRCID>) createdPersisters.get(null), aggregatePersister.getEntityJoinTree());
		
		while (!relationStack.isEmpty()) {
			GraftPoint<SRC, SRCID, LEFTTABLE, SRC, SRCID> assemblyPawn = (GraftPoint<SRC, SRCID, LEFTTABLE, SRC, SRCID>) relationStack.poll();
			EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister = (EntityReader<SRC, SRCID, LEFTTABLE>) assemblyPawn.getRelationOwnerPersister();
			assemblyPawn.getRelationOwnerEntity().getRelations()
					.forEach(relationPawn -> {
						if (relationPawn instanceof ResolvedOneToOneRelation) {
							CreatedPersisterCollector<TRGT, TRGTID> localCreatedPersistor = (CreatedPersisterCollector<TRGT, TRGTID>) createdPersisters.get(relationPawn);
							ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> localRelation = (ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID>) relationPawn;
							EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister = new EntityReader<>(
									localCreatedPersistor.getPersister().<RIGHTTABLE>getMapping(), persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
							GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> graftPoint = oneToOneAppender.append(
									localRelation,
									sourcePersister,
									targetPersister,
									assemblyPawn.getParentJoinPoint(),
									assemblyPawn.getAggregateTree());
							
							new SkeletonAggregateAppender()
									.appendInheritance(localCreatedPersistor, aggregatePersister.getEntityJoinTree());
							
							relationStack.add(graftPoint);
						}
						if (relationPawn instanceof ResolvedOneToManyRelation) {
							CreatedPersisterCollector<TRGT, TRGTID> localCreatedPersistor = (CreatedPersisterCollector<TRGT, TRGTID>) createdPersisters.get(relationPawn);
							ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> localRelation = (ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn;
							EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister = new EntityReader<>(
									localCreatedPersistor.getPersister().<RIGHTTABLE>getMapping(), persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
							GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> graftPoint = oneToManyAppender.append(
									localRelation,
									sourcePersister,
									targetPersister,
									assemblyPawn.getParentJoinPoint(),
									assemblyPawn.getAggregateTree(),
									persistenceContext.getDialect(),
									persistenceContext.getConnectionProvider());
							
							new SkeletonAggregateAppender()
									.appendInheritance(localCreatedPersistor, aggregatePersister.getEntityJoinTree());
							
							relationStack.add(graftPoint);
						}
						if (relationPawn instanceof ResolvedManyToManyRelation) {
							CreatedPersisterCollector<TRGT, TRGTID> localCreatedPersistor = (CreatedPersisterCollector<TRGT, TRGTID>) createdPersisters.get(relationPawn);
							ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> localRelation = (ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn;
							EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister = new EntityReader<>(
									localCreatedPersistor.getPersister().<RIGHTTABLE>getMapping(), persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
							
							GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> graftPoint = manyToManyAppender.append(
									localRelation,
									sourcePersister,
									targetPersister,
									assemblyPawn.getParentJoinPoint(),
									assemblyPawn.getAggregateTree(),
									persistenceContext.getDialect(),
									persistenceContext.getConnectionProvider());
							new SkeletonAggregateAppender()
									.appendInheritance(localCreatedPersistor, aggregatePersister.getEntityJoinTree());
							
							relationStack.add(graftPoint);
						}
						if (relationPawn instanceof ResolvedManyToOneRelation) {
							CreatedPersisterCollector<TRGT, TRGTID> localCreatedPersistor = (CreatedPersisterCollector<TRGT, TRGTID>) createdPersisters.get(relationPawn);
							ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE> localRelation = (ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn;
							EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister = new EntityReader<>(
									localCreatedPersistor.getPersister().<RIGHTTABLE>getMapping(), persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
							GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> graftPoint = manyToOneAppender.append(
									localRelation,
									sourcePersister,
									targetPersister,
									assemblyPawn.getParentJoinPoint(),
									assemblyPawn.getAggregateTree());
							new SkeletonAggregateAppender()
									.appendInheritance(localCreatedPersistor, aggregatePersister.getEntityJoinTree());
							relationStack.add(graftPoint);
						}
						if (relationPawn instanceof ResolvedElementCollectionRelation) {
							CreatedPersisterCollector<TRGT, TRGTID> localCreatedPersistor = (CreatedPersisterCollector<TRGT, TRGTID>) createdPersisters.get(relationPawn);
							EntityReadWriteExecutor<TRGT, TRGTID> persister = localCreatedPersistor.getPersister();
							ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, LEFTTABLE, RIGHTTABLE, ElementRecord<TRGT, SRCID>> localRelation = (ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, LEFTTABLE, RIGHTTABLE, ElementRecord<TRGT, SRCID>>) relationPawn;
							elementCollectionAppender.append(
									localRelation,
									sourcePersister,
									(ElementRecordPersister<TRGT, SRCID, RIGHTTABLE, ElementRecord<TRGT, SRCID>>) persister,
									assemblyPawn.getParentJoinPoint(),
									assemblyPawn.getAggregateTree(),
									persistenceContext.getDialect(),
									persistenceContext.getConnectionProvider());
						}
						if (relationPawn instanceof ResolvedMapRelation) {
							MapCreatedPersisterCollector mapCreatedPersisterCollector = (MapCreatedPersisterCollector) createdPersisters.get(relationPawn);
							ResolvedMapRelation localRelation = (ResolvedMapRelation) relationPawn;
							Duo<GraftPoint, GraftPoint> keyValueGraftPoints = graftMapRelation(localRelation, assemblyPawn.getAggregateTree(), assemblyPawn.getParentJoinPoint(), sourcePersister, mapCreatedPersisterCollector);
							
							if (keyValueGraftPoints.getLeft() != null) {
								relationStack.add(keyValueGraftPoints.getLeft());
							}
							if (keyValueGraftPoints.getRight() != null) {
								relationStack.add(keyValueGraftPoints.getRight());
							}
						}
					});
		}
	}
	
	private <X, Y, SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>, KTABLE extends Table<KTABLE>, VTABLE extends Table<VTABLE>>
	Duo<GraftPoint, GraftPoint> graftMapRelation(ResolvedMapRelation relation,
	                                             EntityJoinTree<SRC, SRCID> aggregateTree,
	                                             String mountPoint,
	                                             EntityReader<SRC, SRCID, LEFTTABLE> relationOwnerPersister,
	                                             MapCreatedPersisterCollector collectedPersisters) {
		MapCreatedPersisterCollector<SRCID, K, KID, V, VID, MAPTABLE, X, Y> typedMapCreatedPersisterCollector = (MapCreatedPersisterCollector<SRCID, K, KID, V, VID, MAPTABLE, X, Y>) collectedPersisters;
		ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE> typedRelation = (ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE>) relation;
		EntityReader<K, KID, KTABLE> keyEntityReader = null;
		if (typedMapCreatedPersisterCollector.getKeyPersisterCollector() != null) {
			keyEntityReader = new EntityReader<>(
					typedMapCreatedPersisterCollector.getKeyPersisterCollector().getPersister().<KTABLE>getMapping(), persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
		}
		EntityReader<V, VID, VTABLE> valueEntityReader = null;
		if (typedMapCreatedPersisterCollector.getValuePersisterCollector() != null) {
			valueEntityReader = new EntityReader<>(
					typedMapCreatedPersisterCollector.getValuePersisterCollector().getPersister().<VTABLE>getMapping(), persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
		}
		
		return mapAppender.append(
				typedRelation,
				aggregateTree,
				relationOwnerPersister,
				mountPoint,
				typedMapCreatedPersisterCollector.getKeyValueRecordPersister(),
				keyEntityReader,
				valueEntityReader,
				persistenceContext.getDialect(),
				persistenceContext.getConnectionProvider()
		);
	}
	
	private <C, I, T extends Table<T>> Set<Table<?>> collectTables(AbstractEntity<C, I, T> rootEntity) {
		Set<Table<?>> tables = new HashSet<>();
		Queue<AbstractEntity<?, ?, ?>> relationStack = new ArrayDeque<>();
		// Initializing the stack
		// Particular case : we don't want to add the "abstract table" of table-per-class but rather its sub-types ones
		if (rootEntity.isTablePerClass()) {
			relationStack.addAll(((PolymorphicEntity<?, ?, ?>) rootEntity).getPolymorphism().getSubEntities());
		} else {
			relationStack.add(rootEntity);
		}
		while (!relationStack.isEmpty()) {
			AbstractEntity<?, ?, ?> entity = relationStack.poll();
			tables.add(entity.getTable());
			entity.getRelations().forEach(relation -> {
				RelationJoin relationJoin = relation.getJoin();
				if (relationJoin instanceof DirectRelationJoin) {
					DirectRelationJoin directRelationJoin = (DirectRelationJoin) relationJoin;
					tables.add((Table<?>) directRelationJoin.getLeftKey().getTable());
					tables.add((Table<?>) directRelationJoin.getRightKey().getTable());
				} else if (relationJoin instanceof IntermediaryRelationJoin) {
					IntermediaryRelationJoin intermediaryRelationJoin = (IntermediaryRelationJoin) relationJoin;
					tables.add((Table<?>) intermediaryRelationJoin.getLeftKey().getTable());
					tables.add((Table<?>) intermediaryRelationJoin.getRightKey().getTable());
					tables.add(intermediaryRelationJoin.getJoinTable());
				}
				
				// preparing for next iteration
				if (relation instanceof EntityRelation) {
					relationStack.add(((EntityRelation) relation).getTargetEntity());
				} else if (relation instanceof ResolvedOneToManyRelation) {
					relationStack.add(((ResolvedOneToManyRelation) relation).getTargetEntity());
				} else if (relation instanceof ResolvedManyToManyRelation) {
					relationStack.add(((ResolvedManyToManyRelation) relation).getTargetEntity());
				} else if (relation instanceof ResolvedMapRelation) {
					ResolvedMapRelation mapRelation = (ResolvedMapRelation) relation;
					if (mapRelation.getKeyEntityDefinition() != null) {
						relationStack.add(mapRelation.getKeyEntityDefinition().getEntity());
					}
					if (mapRelation.getValueEntityDefinition() != null) {
						relationStack.add(mapRelation.getValueEntityDefinition().getEntity());
					}
				}
			});
			AncestorJoin<?, ?, ?, ?> parent = entity.getParent();
			if (parent != null) {
				relationStack.add(parent.getAncestor());
			}
			if (entity instanceof PolymorphicEntity) {
				relationStack.addAll(((PolymorphicEntity<?, ?, ?>) entity).getPolymorphism().getSubEntities());
			}
		}
		return tables;
	}
	
	/**
	 * Structure that describes one node of the relation-grafting traversal performed by {@link #appendWriteCascades(AbstractEntity, EntityWriteExecutor)}.
	 * It bundles the four pieces of state needed to both process the relations owned by a node and to resume the
	 * traversal at the correct place once a relation target becomes a new node to descend into:
	 * the relation-owning {@link Entity}, its {@link EntityWriteExecutor}, the parent join node name to attach
	 * to, and the accessor used to shift nested property paths.
	 * 
	 * @param <SRC> the relation-owning (source) entity type
	 * @param <SRCID> the source entity identifier type
	 * @param <LEFTTABLE> the source table type
	 */
	public static class CascadePoint<SRC, SRCID, LEFTTABLE extends Table<LEFTTABLE>> {
		
		/** The entity whose relations are being grafted at this traversal node. */
		private final AbstractEntity<SRC, SRCID, LEFTTABLE> relationOwnerEntity;
		/** The persister of {@link #relationOwnerEntity}, onto which child relation joins are attached. */
		private final EntityWriteExecutor<SRC, SRCID> relationOwnerPersister;
		
		public CascadePoint(AbstractEntity<SRC, SRCID, LEFTTABLE> relationOwnerEntity,
		                    EntityWriteExecutor<SRC, SRCID> relationOwnerPersister) {
			this.relationOwnerEntity = relationOwnerEntity;
			this.relationOwnerPersister = relationOwnerPersister;
		}
		
		public EntityWriteExecutor<SRC, SRCID> getRelationOwnerPersister() {
			return relationOwnerPersister;
		}
		
		public AbstractEntity<SRC, SRCID, LEFTTABLE> getRelationOwnerEntity() {
			return relationOwnerEntity;
		}
	}
	
	public static class GraftPoint<SRC, SRCID, LEFTTABLE extends Table<LEFTTABLE>, ROOT, ROOTID> {
		
		private final AbstractEntity<SRC, SRCID, LEFTTABLE> relationOwnerEntity;
		private final EntityReader<SRC, SRCID, ?> relationOwnerPersister;
		private final String parentJoinPoint;
		/** Accessor from the source entity to the related target, used to shift property paths when descending; {@code null} for the root seed. */
		private EntityJoinTree<ROOT, ROOTID> aggregateTree;
		
		public GraftPoint(AbstractEntity<SRC, SRCID, LEFTTABLE> relationOwnerEntity,
		                  EntityReader<SRC, SRCID, ?> relationOwnerPersister,
		                  String parentJoinPoint) {
			this.relationOwnerEntity = relationOwnerEntity;
			this.relationOwnerPersister = relationOwnerPersister;
			this.parentJoinPoint = parentJoinPoint;
		}
		
		public GraftPoint(AbstractEntity<SRC, SRCID, LEFTTABLE> relationOwnerEntity,
		                  EntityReader<SRC, SRCID, ?> relationOwnerPersister,
		                  String parentJoinPoint,
		                  EntityJoinTree<ROOT, ROOTID> aggregateTree) {
			this.relationOwnerEntity = relationOwnerEntity;
			this.relationOwnerPersister = relationOwnerPersister;
			this.parentJoinPoint = parentJoinPoint;
			this.aggregateTree = aggregateTree;
		}
		
		/** @return the persister of the relation-owning entity, onto which child relation joins are attached */
		public EntityReader<SRC, SRCID, ?> getRelationOwnerPersister() {
			return relationOwnerPersister;
		}
		
		/** @return the parent join node name under which the next relation join must be added */
		public String getParentJoinPoint() {
			return parentJoinPoint;
		}
		
		/** @return the entity whose relations are being grafted at this traversal node */
		public AbstractEntity<SRC,SRCID,LEFTTABLE> getRelationOwnerEntity() {
			return relationOwnerEntity;
		}
		
		/** @return the accessor from source to target used to shift nested property paths, or {@code null} for the root seed */
		public EntityJoinTree<ROOT, ROOTID> getAggregateTree() {
			return aggregateTree;
		}
	}
}
