package org.gama.stalactite.persistence.engine.configurer;

import java.util.Collection;

import org.gama.lang.collection.Iterables;
import org.gama.reflection.AccessorDefinition;
import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.persistence.engine.AssociationTableNamingStrategy;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.ElementCollectionTableNamingStrategy;
import org.gama.stalactite.persistence.engine.EntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.gama.stalactite.persistence.engine.PersisterRegistry;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.PostInitializer;
import org.gama.stalactite.persistence.engine.runtime.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.cycle.OneToOneCycleConfigurer;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.IConnectionConfiguration;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public class RelationConfigurer<C, I, T extends Table<T>> {
	
	private final Dialect dialect;
	private final IConnectionConfiguration connectionConfiguration;
	private final PersisterRegistry persisterRegistry;
	private final JoinedTablesPersister<C, I, T> sourcePersister;
	private final ColumnNamingStrategy columnNamingStrategy;
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private final ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy;
	private final ColumnNamingStrategy joinColumnNamingStrategy;
	private final ColumnNamingStrategy indexColumnNamingStrategy;
	private final AssociationTableNamingStrategy associationTableNamingStrategy;
	
	public RelationConfigurer(Dialect dialect,
							  IConnectionConfiguration connectionConfiguration,
							  PersisterRegistry persisterRegistry,
							  JoinedTablesPersister<C, I, T> sourcePersister,
							  ColumnNamingStrategy columnNamingStrategy,
							  ForeignKeyNamingStrategy foreignKeyNamingStrategy,
							  ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy,
							  ColumnNamingStrategy joinColumnNamingStrategy,
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
	
	<TRGT, TRGTID> void registerRelationCascades(EntityMappingConfiguration<C, I> entityMappingConfiguration) {
		
		PersisterBuilderContext currentBuilderContext = PersisterBuilderContext.CURRENT.get();
		
		for (CascadeOne<C, TRGT, TRGTID> cascadeOne : entityMappingConfiguration.<TRGT, TRGTID>getOneToOnes()) {
			CascadeOneConfigurer<C, TRGT, I, TRGTID> cascadeOneConfigurer = new CascadeOneConfigurer<>(cascadeOne,
					sourcePersister,
					dialect,
					connectionConfiguration,
					persisterRegistry,
					foreignKeyNamingStrategy,
					joinColumnNamingStrategy);
			
			String relationName = AccessorDefinition.giveDefinition(cascadeOne.getTargetProvider()).getName();
			
			if (currentBuilderContext.isCycling(cascadeOne.getTargetMappingConfiguration())) {
				// cycle detected
				// we had a second phase load because cycle can hardly be supported by simply joining things together because at one time we will
				// fall into infinite loop (think to SQL generation of a cycling graph ...)
				Class<TRGT> targetEntityType = cascadeOne.getTargetMappingConfiguration().getEntityType();
				// adding the relation to an eventually already existing cycle configurer for the entity
				OneToOneCycleConfigurer<TRGT> cycleSolver = (OneToOneCycleConfigurer<TRGT>)
						Iterables.find(currentBuilderContext.getPostInitializers(), p -> p instanceof OneToOneCycleConfigurer && p.getEntityType() == targetEntityType);
				if (cycleSolver == null) {
					cycleSolver = new OneToOneCycleConfigurer<>(targetEntityType);
					currentBuilderContext.addPostInitializers(cycleSolver);
				}
				cycleSolver.addCycleSolver(relationName, cascadeOneConfigurer);
			} else {
				cascadeOneConfigurer.appendCascades(relationName, new PersisterBuilderImpl<>(cascadeOne.getTargetMappingConfiguration()));
			}
			// Registering relation to EntityCriteria so one can use it as a criteria. Declared as a lazy initializer to work with lazy persister building such as cycling ones
			currentBuilderContext.addPostInitializers(new GraphLoadingRelationRegisterer<>(cascadeOne.getTargetMappingConfiguration().getEntityType(),
					cascadeOne.getTargetProvider()));
		}
		for (CascadeMany<C, TRGT, TRGTID, ? extends Collection<TRGT>> cascadeMany : entityMappingConfiguration.<TRGT, TRGTID>getOneToManys()) {
			CascadeManyConfigurer cascadeManyConfigurer = new CascadeManyConfigurer<>(
					dialect,
					connectionConfiguration,
					persisterRegistry);
			if (currentBuilderContext.isCycling(cascadeMany.getTargetMappingConfiguration())) {
				// cycle detected
				// we had a second phase load because cycle can hardly be supported by simply joining things together because at one time we will
				// fall into infinite loop (think to SQL generation of a cycling graph ...)
				class OneToManyCycleSolver extends PostInitializer<TRGT> {
					OneToManyCycleSolver(Class<TRGT> entityType) {
						super(entityType);
					}
					
					@Override
					public void consume(IEntityConfiguredJoinedTablesPersister<TRGT, Object> targetPersister) {
						cascadeManyConfigurer.appendCascade(cascadeMany,
								sourcePersister,
								foreignKeyNamingStrategy,
								joinColumnNamingStrategy,
								indexColumnNamingStrategy,
								associationTableNamingStrategy,
								true,
								targetPersister);
					}
				}
				PostInitializer<TRGT> postInitializer = new OneToManyCycleSolver(cascadeMany.getTargetMappingConfiguration().getEntityType());
				currentBuilderContext.addPostInitializers(postInitializer);
			} else {
				cascadeManyConfigurer.appendCascade(cascadeMany, sourcePersister,
						foreignKeyNamingStrategy,
						joinColumnNamingStrategy,
						indexColumnNamingStrategy,
						associationTableNamingStrategy,
						new PersisterBuilderImpl<>(cascadeMany.getTargetMappingConfiguration()));
			}
			// Registering relation to EntityCriteria so one can use it as a criteria. Declared as a lazy initializer to work with lazy persister building such as cycling ones
			currentBuilderContext.addPostInitializers(new GraphLoadingRelationRegisterer<>(cascadeMany.getTargetMappingConfiguration().getEntityType(),
					cascadeMany.getCollectionProvider()));
		}
		
		// taking element collections into account
		for (ElementCollectionLinkage<C, ?, ? extends Collection> elementCollection : entityMappingConfiguration.getElementCollections()) {
			ElementCollectionCascadeConfigurer elementCollectionCascadeConfigurer = new ElementCollectionCascadeConfigurer(dialect, connectionConfiguration);
			elementCollectionCascadeConfigurer.appendCascade(elementCollection, sourcePersister, foreignKeyNamingStrategy, columnNamingStrategy,
					elementCollectionTableNamingStrategy);
		}
	}
	
	// Small container aimed at lazily registering a relation into persister so it can be targeted by EntityCriteria
	class GraphLoadingRelationRegisterer<TRGT> extends PostInitializer<TRGT> {
		
		private final IReversibleAccessor reversibleAccessor;
		
		GraphLoadingRelationRegisterer(Class<TRGT> entityType, IReversibleAccessor reversibleAccessor) {
			super(entityType);
			this.reversibleAccessor = reversibleAccessor;
		}
		
		@Override
		public void consume(IEntityConfiguredJoinedTablesPersister<TRGT, Object> targetPersister) {
			sourcePersister.getCriteriaSupport().getRootConfiguration().registerRelation(reversibleAccessor, targetPersister.getMappingStrategy());
		}
	}
}
