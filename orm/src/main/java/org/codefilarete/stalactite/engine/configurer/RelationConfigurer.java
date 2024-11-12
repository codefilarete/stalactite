package org.codefilarete.stalactite.engine.configurer;

import java.util.Collection;
import java.util.Map;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityPersister.EntityCriteria;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.PostInitializer;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecord;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.map.EntityAsKeyAndValueMapRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.map.EntityAsKeyMapRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.map.MapRelation;
import org.codefilarete.stalactite.engine.configurer.map.MapRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.map.ValueAsKeyMapRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelationConfigurer;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.cycle.ManyToManyCycleConfigurer;
import org.codefilarete.stalactite.engine.runtime.cycle.OneToManyCycleConfigurer;
import org.codefilarete.stalactite.engine.runtime.cycle.OneToOneCycleConfigurer;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Iterables;

/**
 * Main class that manage the configurations of all type of relations:
 * - one-to-one
 * - one-to-many
 * - many-to-many
 * - element collection
 * 
 * @author Guillaume Mary
 */
public class RelationConfigurer<C, I, T extends Table<T>> {
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	private final PersisterRegistry persisterRegistry;
	private final SimpleRelationalEntityPersister<C, I, T> sourcePersister;
	private final NamingConfiguration namingConfiguration;
	
	public RelationConfigurer(Dialect dialect,
							  ConnectionConfiguration connectionConfiguration,
							  PersisterRegistry persisterRegistry,
							  SimpleRelationalEntityPersister<C, I, T> sourcePersister,
							  NamingConfiguration namingConfiguration) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.persisterRegistry = persisterRegistry;
		this.sourcePersister = sourcePersister;
		this.namingConfiguration = namingConfiguration;
	}
	
	<TRGT, TRGTID> void configureRelations(EntityMappingConfiguration<C, I> entityMappingConfiguration) {
		
		PersisterBuilderContext currentBuilderContext = PersisterBuilderContext.CURRENT.get();
		
		for (OneToOneRelation<C, TRGT, TRGTID> oneToOneRelation : entityMappingConfiguration.<TRGT, TRGTID>getOneToOnes()) {
			OneToOneRelationConfigurer<C, TRGT, I, TRGTID> oneToOneRelationConfigurer = new OneToOneRelationConfigurer<>(oneToOneRelation,
					sourcePersister,
					dialect,
					connectionConfiguration,
					namingConfiguration.getForeignKeyNamingStrategy(),
					namingConfiguration.getJoinColumnNamingStrategy());
			
			String relationName = AccessorDefinition.giveDefinition(oneToOneRelation.getTargetProvider()).getName();
			
			if (currentBuilderContext.isCycling(oneToOneRelation.getTargetMappingConfiguration())) {
				// cycle detected
				// we had a second phase load because cycle can hardly be supported by simply joining things together because at one time we will
				// fall into infinite loop (think to SQL generation of a cycling graph ...)
				Class<TRGT> targetEntityType = oneToOneRelation.getTargetMappingConfiguration().getEntityType();
				// adding the relation to an eventually already existing cycle configurer for the entity
				OneToOneCycleConfigurer<TRGT> cycleSolver = (OneToOneCycleConfigurer<TRGT>)
						Iterables.find(currentBuilderContext.getBuildLifeCycleListeners(), p -> p instanceof OneToOneCycleConfigurer && ((OneToOneCycleConfigurer<?>) p).getEntityType() == targetEntityType);
				if (cycleSolver == null) {
					cycleSolver = new OneToOneCycleConfigurer<>(targetEntityType);
					currentBuilderContext.addBuildLifeCycleListener(cycleSolver);
				}
				cycleSolver.addCycleSolver(relationName, oneToOneRelationConfigurer);
			} else {
				oneToOneRelationConfigurer.configure(relationName, new PersisterBuilderImpl<>(oneToOneRelation.getTargetMappingConfiguration()), oneToOneRelation.isFetchSeparately());
			}
			// Registering relation to EntityCriteria so one can use it as a criteria. Declared as a lazy initializer to work with lazy persister building such as cycling ones
			currentBuilderContext.addBuildLifeCycleListener(new GraphLoadingRelationRegisterer<>(oneToOneRelation.getTargetMappingConfiguration().getEntityType(),
					oneToOneRelation.getTargetProvider()));
		}
		for (OneToManyRelation<C, TRGT, TRGTID, ? extends Collection<TRGT>> oneToManyRelation : entityMappingConfiguration.<TRGT, TRGTID>getOneToManys()) {
			OneToManyRelationConfigurer oneToManyRelationConfigurer = new OneToManyRelationConfigurer<>(oneToManyRelation,
					sourcePersister,
					dialect,
					connectionConfiguration,
					namingConfiguration.getForeignKeyNamingStrategy(),
					namingConfiguration.getJoinColumnNamingStrategy(),
					namingConfiguration.getAssociationTableNamingStrategy(),
					namingConfiguration.getIndexColumnNamingStrategy());
			
			String relationName = AccessorDefinition.giveDefinition(oneToManyRelation.getCollectionProvider()).getName();
			
			if (currentBuilderContext.isCycling(oneToManyRelation.getTargetMappingConfiguration())) {
				// cycle detected
				// we had a second phase load because cycle can hardly be supported by simply joining things together because at one time we will
				// fall into infinite loop (think to SQL generation of a cycling graph ...)
				Class<TRGT> targetEntityType = oneToManyRelation.getTargetMappingConfiguration().getEntityType();
				// adding the relation to an eventually already existing cycle configurer for the entity
				OneToManyCycleConfigurer<TRGT> cycleSolver = (OneToManyCycleConfigurer<TRGT>)
						Iterables.find(currentBuilderContext.getBuildLifeCycleListeners(), p -> p instanceof OneToManyCycleConfigurer && ((OneToManyCycleConfigurer<?>) p).getEntityType() == targetEntityType);
				if (cycleSolver == null) {
					cycleSolver = new OneToManyCycleConfigurer<>(targetEntityType);
					currentBuilderContext.addBuildLifeCycleListener(cycleSolver);
				}
				cycleSolver.addCycleSolver(relationName, oneToManyRelationConfigurer);
			} else {
				oneToManyRelationConfigurer.configure(new PersisterBuilderImpl<>(oneToManyRelation.getTargetMappingConfiguration()));
			}
			// Registering relation to EntityCriteria so one can use it as a criteria. Declared as a lazy initializer to work with lazy persister building such as cycling ones
			currentBuilderContext.addBuildLifeCycleListener(new GraphLoadingRelationRegisterer<>(oneToManyRelation.getTargetMappingConfiguration().getEntityType(),
					oneToManyRelation.getCollectionProvider()));
		}
		
		for (ManyToManyRelation<C, TRGT, TRGTID, Collection<TRGT>, Collection<C>> manyToManyRelation : entityMappingConfiguration.<TRGT, TRGTID>getManyToManyRelations()) {
			ManyToManyRelationConfigurer<C, TRGT, I, TRGTID, Collection<TRGT>, Collection<C>> manyRelationConfigurer = new ManyToManyRelationConfigurer<>(manyToManyRelation,
					sourcePersister,
					dialect,
					connectionConfiguration,
					namingConfiguration.getForeignKeyNamingStrategy(),
					namingConfiguration.getJoinColumnNamingStrategy(),
					namingConfiguration.getIndexColumnNamingStrategy(),
					namingConfiguration.getAssociationTableNamingStrategy()
			);
			
			String relationName = AccessorDefinition.giveDefinition(manyToManyRelation.getCollectionAccessor()).getName();
			
			if (currentBuilderContext.isCycling(manyToManyRelation.getTargetMappingConfiguration())) {
				// cycle detected
				// we had a second phase load because cycle can hardly be supported by simply joining things together because at one time we will
				// fall into infinite loop (think to SQL generation of a cycling graph ...)
				Class<TRGT> targetEntityType = manyToManyRelation.getTargetMappingConfiguration().getEntityType();
				// adding the relation to an eventually already existing cycle configurer for the entity
				ManyToManyCycleConfigurer<TRGT> cycleSolver = (ManyToManyCycleConfigurer<TRGT>)
						Iterables.find(currentBuilderContext.getBuildLifeCycleListeners(), p -> p instanceof ManyToManyCycleConfigurer && ((ManyToManyCycleConfigurer<?>) p).getEntityType() == targetEntityType);
				if (cycleSolver == null) {
					cycleSolver = new ManyToManyCycleConfigurer<>(targetEntityType);
					currentBuilderContext.addBuildLifeCycleListener(cycleSolver);
				}
				cycleSolver.addCycleSolver(relationName, manyRelationConfigurer);
			} else {
				manyRelationConfigurer.configure(new PersisterBuilderImpl<>(manyToManyRelation.getTargetMappingConfiguration()));
			}
			
			// Registering relation to EntityCriteria so one can use it as a criteria. Declared as a lazy initializer to work with lazy persister building such as cycling ones
			currentBuilderContext.addBuildLifeCycleListener(new GraphLoadingRelationRegisterer<>(manyToManyRelation.getTargetMappingConfiguration().getEntityType(),
					manyToManyRelation.getCollectionAccessor()));
		}
		
		// taking element collections into account
		for (ElementCollectionRelation<C, ?, ? extends Collection> elementCollection : entityMappingConfiguration.getElementCollections()) {
			ElementCollectionRelationConfigurer<C, ?, I, ? extends Collection> elementCollectionRelationConfigurer = new ElementCollectionRelationConfigurer<>(
					elementCollection,
					sourcePersister,
					namingConfiguration.getForeignKeyNamingStrategy(),
					namingConfiguration.getColumnNamingStrategy(),
					namingConfiguration.getElementCollectionTableNamingStrategy(),
					dialect,
					connectionConfiguration);
			SimpleRelationalEntityPersister<? extends ElementRecord<?, I>, ? extends ElementRecord<?, I>, ?> collectionPersister = elementCollectionRelationConfigurer.configure();
			// Registering relation to EntityCriteria so one can use it as a criteria. Declared as a lazy initializer to work with lazy persister building such as cycling ones
			currentBuilderContext.addBuildLifeCycleListener(new GraphLoadingRelationRegisterer<C>(entityMappingConfiguration.getEntityType(),
					elementCollection.getCollectionProvider()) {

				@Override
				public void consume(ConfiguredRelationalPersister<C, ?> targetPersister) {
					// targetPersister is not the one we need : it's owning class one's (see constructor) whereas
					// we need the one created by the configure() method above because it manages the relation through
					// the generic ElementRecord which can't be found in the PersisterRegistry.
					// Note that we could have used BuildLifeCycleListener directly but I prefer using GraphLoadingRelationRegisterer to clarify the intention
					super.consume((ConfiguredRelationalPersister<C, ?>) collectionPersister);
				}
			});
		}
		
		// taking map relations into account
		for (MapRelation<C, ?, ?, ? extends Map> map : entityMappingConfiguration.getMaps()) {
			if (map.getKeyEntityConfigurationProvider() != null && map.getValueEntityConfigurationProvider() != null) {
				EntityMappingConfiguration<Object, Object> keyEntityConfiguration = (EntityMappingConfiguration<Object, Object>) map.getKeyEntityConfigurationProvider().getConfiguration();
				ConfiguredRelationalPersister<Object, Object> keyEntityPersister = new PersisterBuilderImpl<>(keyEntityConfiguration)
						.build(dialect, connectionConfiguration, null);
				EntityMappingConfiguration<Object, Object> valueEntityConfiguration = (EntityMappingConfiguration<Object, Object>) map.getValueEntityConfigurationProvider().getConfiguration();
				ConfiguredRelationalPersister<Object, Object> valueEntityPersister = new PersisterBuilderImpl<>(valueEntityConfiguration)
						.build(dialect, connectionConfiguration, null);
				EntityAsKeyAndValueMapRelationConfigurer entityAsKeyMapRelationConfigurer = new EntityAsKeyAndValueMapRelationConfigurer<>(
						(MapRelation) map,
						sourcePersister,
						keyEntityPersister,
						valueEntityPersister,
						namingConfiguration.getForeignKeyNamingStrategy(),
						namingConfiguration.getColumnNamingStrategy(),
						namingConfiguration.getEntryMapTableNamingStrategy(),
						dialect,
						connectionConfiguration);
				entityAsKeyMapRelationConfigurer.configure();
			} else if (map.getKeyEntityConfigurationProvider() != null) {
				EntityMappingConfiguration<Object, Object> keyEntityConfiguration = (EntityMappingConfiguration<Object, Object>) map.getKeyEntityConfigurationProvider().getConfiguration();
				ConfiguredRelationalPersister<Object, Object> keyEntityPersister = new PersisterBuilderImpl<>(keyEntityConfiguration)
						.build(dialect, connectionConfiguration, null);
				EntityAsKeyMapRelationConfigurer entityAsKeyMapRelationConfigurer = new EntityAsKeyMapRelationConfigurer<>(
						(MapRelation) map,
						sourcePersister,
						keyEntityPersister,
						namingConfiguration.getForeignKeyNamingStrategy(),
						namingConfiguration.getColumnNamingStrategy(),
						namingConfiguration.getEntryMapTableNamingStrategy(),
						dialect,
						connectionConfiguration);
				entityAsKeyMapRelationConfigurer.configure();
			} else if (map.getValueEntityConfigurationProvider() != null) {
				EntityMappingConfiguration<Object, Object> valueEntityConfiguration = (EntityMappingConfiguration<Object, Object>) map.getValueEntityConfigurationProvider().getConfiguration();
				ConfiguredRelationalPersister<Object, Object> valueEntityPersister = new PersisterBuilderImpl<>(valueEntityConfiguration)
						.build(dialect, connectionConfiguration, null);
				ValueAsKeyMapRelationConfigurer valueAsKeyMapRelationConfigurer = new ValueAsKeyMapRelationConfigurer<>(
						(MapRelation) map,
						sourcePersister,
						valueEntityPersister,
						namingConfiguration.getForeignKeyNamingStrategy(),
						namingConfiguration.getColumnNamingStrategy(),
						namingConfiguration.getEntryMapTableNamingStrategy(),
						dialect,
						connectionConfiguration);
				valueAsKeyMapRelationConfigurer.configure();
			} else {
				MapRelationConfigurer<C, I, ?, ?, ? extends Map> mapRelationConfigurer = new MapRelationConfigurer<>(
						map,
						sourcePersister,
						namingConfiguration.getForeignKeyNamingStrategy(),
						namingConfiguration.getColumnNamingStrategy(),
						namingConfiguration.getEntryMapTableNamingStrategy(),
						dialect,
						connectionConfiguration
				);
				mapRelationConfigurer.configure();
			}
		}
	}
	
	/**
	 * Small container aimed at lazily registering a relation into source persister so it can be targeted by {@link EntityCriteria}
	 * 
	 * @param <TRGT> relation entity type
	 */
	class GraphLoadingRelationRegisterer<TRGT> extends PostInitializer<TRGT> {
		
		private final ReversibleAccessor<C, Object> targetEntityAccessor;
		
		GraphLoadingRelationRegisterer(Class<TRGT> targetEntityType, ReversibleAccessor<C, ?> targetEntityAccessor) {
			super(targetEntityType);
			this.targetEntityAccessor = (ReversibleAccessor<C, Object>) targetEntityAccessor;
		}
		
		@Override
		public void consume(ConfiguredRelationalPersister<TRGT, ?> targetPersister) {
			// we must dynamically retrieve the persister into the registry because sourcePersister might not be the
			// final / registered one in particular in case of polymorphism 
			ConfiguredRelationalPersister<C, I> registeredSourcePersister = ((ConfiguredRelationalPersister<C, I>) persisterRegistry.getPersister(sourcePersister.getClassToPersist()));
			registeredSourcePersister.registerRelation(targetEntityAccessor, targetPersister);
			
		}
	}
}
