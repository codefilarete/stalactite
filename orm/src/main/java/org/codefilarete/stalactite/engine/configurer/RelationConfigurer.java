package org.codefilarete.stalactite.engine.configurer;

import java.util.Collection;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityPersister.EntityCriteria;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.PostInitializer;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelationConfigurer;
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
	private final ColumnNamingStrategy columnNamingStrategy;
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private final ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy;
	private final JoinColumnNamingStrategy joinColumnNamingStrategy;
	private final ColumnNamingStrategy indexColumnNamingStrategy;
	private final AssociationTableNamingStrategy associationTableNamingStrategy;
	
	public RelationConfigurer(Dialect dialect,
							  ConnectionConfiguration connectionConfiguration,
							  PersisterRegistry persisterRegistry,
							  SimpleRelationalEntityPersister<C, I, T> sourcePersister,
							  ColumnNamingStrategy columnNamingStrategy,
							  ForeignKeyNamingStrategy foreignKeyNamingStrategy,
							  ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy,
							  JoinColumnNamingStrategy joinColumnNamingStrategy,
							  ColumnNamingStrategy indexColumnNamingStrategy,
							  AssociationTableNamingStrategy associationTableNamingStrategy) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.persisterRegistry = persisterRegistry;
		this.sourcePersister = sourcePersister;
		this.columnNamingStrategy = columnNamingStrategy;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		this.elementCollectionTableNamingStrategy = elementCollectionTableNamingStrategy;
		this.joinColumnNamingStrategy = joinColumnNamingStrategy;
		this.indexColumnNamingStrategy = indexColumnNamingStrategy;
		this.associationTableNamingStrategy = associationTableNamingStrategy;
	}
	
	<TRGT, TRGTID> void configureRelations(EntityMappingConfiguration<C, I> entityMappingConfiguration) {
		
		PersisterBuilderContext currentBuilderContext = PersisterBuilderContext.CURRENT.get();
		
		for (OneToOneRelation<C, TRGT, TRGTID> oneToOneRelation : entityMappingConfiguration.<TRGT, TRGTID>getOneToOnes()) {
			OneToOneRelationConfigurer<C, TRGT, I, TRGTID> oneToOneRelationConfigurer = new OneToOneRelationConfigurer<>(oneToOneRelation,
					sourcePersister,
					dialect,
					connectionConfiguration,
					persisterRegistry,
					foreignKeyNamingStrategy,
					joinColumnNamingStrategy);
			
			String relationName = AccessorDefinition.giveDefinition(oneToOneRelation.getTargetProvider()).getName();
			
			if (currentBuilderContext.isCycling(oneToOneRelation.getTargetMappingConfiguration())) {
				// cycle detected
				// we had a second phase load because cycle can hardly be supported by simply joining things together because at one time we will
				// fall into infinite loop (think to SQL generation of a cycling graph ...)
				Class<TRGT> targetEntityType = oneToOneRelation.getTargetMappingConfiguration().getEntityType();
				// adding the relation to an eventually already existing cycle configurer for the entity
				OneToOneCycleConfigurer<TRGT> cycleSolver = (OneToOneCycleConfigurer<TRGT>)
						Iterables.find(currentBuilderContext.getPostInitializers(), p -> p instanceof OneToOneCycleConfigurer && p.getEntityType() == targetEntityType);
				if (cycleSolver == null) {
					cycleSolver = new OneToOneCycleConfigurer<>(targetEntityType);
					currentBuilderContext.addPostInitializers(cycleSolver);
				}
				cycleSolver.addCycleSolver(relationName, oneToOneRelationConfigurer);
			} else {
				oneToOneRelationConfigurer.configure(relationName, new PersisterBuilderImpl<>(oneToOneRelation.getTargetMappingConfiguration()), oneToOneRelation.isFetchSeparately());
			}
			// Registering relation to EntityCriteria so one can use it as a criteria. Declared as a lazy initializer to work with lazy persister building such as cycling ones
			currentBuilderContext.addPostInitializers(new GraphLoadingRelationRegisterer<>(oneToOneRelation.getTargetMappingConfiguration().getEntityType(),
					oneToOneRelation.getTargetProvider()));
		}
		for (OneToManyRelation<C, TRGT, TRGTID, ? extends Collection<TRGT>> oneToManyRelation : entityMappingConfiguration.<TRGT, TRGTID>getOneToManys()) {
			OneToManyRelationConfigurer oneToManyRelationConfigurer = new OneToManyRelationConfigurer<>(oneToManyRelation,
					sourcePersister,
					dialect,
					connectionConfiguration,
					persisterRegistry,
					foreignKeyNamingStrategy,
					joinColumnNamingStrategy,
					associationTableNamingStrategy,
					indexColumnNamingStrategy);
			
			String relationName = AccessorDefinition.giveDefinition(oneToManyRelation.getCollectionProvider()).getName();
			
			if (currentBuilderContext.isCycling(oneToManyRelation.getTargetMappingConfiguration())) {
				// cycle detected
				// we had a second phase load because cycle can hardly be supported by simply joining things together because at one time we will
				// fall into infinite loop (think to SQL generation of a cycling graph ...)
				Class<TRGT> targetEntityType = oneToManyRelation.getTargetMappingConfiguration().getEntityType();
				// adding the relation to an eventually already existing cycle configurer for the entity
				OneToManyCycleConfigurer<TRGT> cycleSolver = (OneToManyCycleConfigurer<TRGT>)
						Iterables.find(currentBuilderContext.getPostInitializers(), p -> p instanceof OneToManyCycleConfigurer && p.getEntityType() == targetEntityType);
				if (cycleSolver == null) {
					cycleSolver = new OneToManyCycleConfigurer<>(targetEntityType);
					currentBuilderContext.addPostInitializers(cycleSolver);
				}
				cycleSolver.addCycleSolver(relationName, oneToManyRelationConfigurer);
			} else {
				oneToManyRelationConfigurer.configure(new PersisterBuilderImpl<>(oneToManyRelation.getTargetMappingConfiguration()));
			}
			// Registering relation to EntityCriteria so one can use it as a criteria. Declared as a lazy initializer to work with lazy persister building such as cycling ones
			currentBuilderContext.addPostInitializers(new GraphLoadingRelationRegisterer<>(oneToManyRelation.getTargetMappingConfiguration().getEntityType(),
					oneToManyRelation.getCollectionProvider()));
		}
		
		for (ManyToManyRelation<C, TRGT, TRGTID, Collection<TRGT>, Collection<C>> manyToManyRelation : entityMappingConfiguration.<TRGT, TRGTID>getManyToManyRelations()) {
			ManyToManyRelationConfigurer<C, TRGT, I, TRGTID, Collection<TRGT>, Collection<C>> manyRelationConfigurer = new ManyToManyRelationConfigurer<>(manyToManyRelation,
					sourcePersister,
					dialect,
					connectionConfiguration,
					persisterRegistry,
					foreignKeyNamingStrategy,
					joinColumnNamingStrategy,
					indexColumnNamingStrategy,
					associationTableNamingStrategy
			);
			
			String relationName = AccessorDefinition.giveDefinition(manyToManyRelation.getCollectionProvider()).getName();
			
			if (currentBuilderContext.isCycling(manyToManyRelation.getTargetMappingConfiguration())) {
				// cycle detected
				// we had a second phase load because cycle can hardly be supported by simply joining things together because at one time we will
				// fall into infinite loop (think to SQL generation of a cycling graph ...)
				Class<TRGT> targetEntityType = manyToManyRelation.getTargetMappingConfiguration().getEntityType();
				// adding the relation to an eventually already existing cycle configurer for the entity
				ManyToManyCycleConfigurer<TRGT> cycleSolver = (ManyToManyCycleConfigurer<TRGT>)
						Iterables.find(currentBuilderContext.getPostInitializers(), p -> p instanceof ManyToManyCycleConfigurer && p.getEntityType() == targetEntityType);
				if (cycleSolver == null) {
					cycleSolver = new ManyToManyCycleConfigurer<>(targetEntityType);
					currentBuilderContext.addPostInitializers(cycleSolver);
				}
				cycleSolver.addCycleSolver(relationName, manyRelationConfigurer);
			} else {
				manyRelationConfigurer.configure(new PersisterBuilderImpl<>(manyToManyRelation.getTargetMappingConfiguration()));
			}
		}
		
		// taking element collections into account
		for (ElementCollectionRelation<C, ?, ? extends Collection> elementCollection : entityMappingConfiguration.getElementCollections()) {
			ElementCollectionRelationConfigurer<C, ?, I, ? extends Collection> elementCollectionRelationConfigurer = new ElementCollectionRelationConfigurer<>(
					elementCollection,
					sourcePersister,
					foreignKeyNamingStrategy,
					columnNamingStrategy,
					elementCollectionTableNamingStrategy,
					dialect,
					connectionConfiguration);
			elementCollectionRelationConfigurer.configure();
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
			sourcePersister.getCriteriaSupport().registerRelation(targetEntityAccessor, targetPersister.getMapping());
		}
	}
}
