package org.codefilarete.stalactite.engine.configurer;

import java.util.Collection;

import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.PostInitializer;
import org.codefilarete.stalactite.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.cycle.OneToManyCycleConfigurer;
import org.codefilarete.stalactite.engine.runtime.cycle.OneToOneCycleConfigurer;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.EntityPersister.EntityCriteria;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
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
	private final ColumnNamingStrategy joinColumnNamingStrategy;
	private final ColumnNamingStrategy indexColumnNamingStrategy;
	private final AssociationTableNamingStrategy associationTableNamingStrategy;
	
	public RelationConfigurer(Dialect dialect,
							  ConnectionConfiguration connectionConfiguration,
							  PersisterRegistry persisterRegistry,
							  SimpleRelationalEntityPersister<C, I, T> sourcePersister,
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
				cascadeOneConfigurer.appendCascades(relationName, new PersisterBuilderImpl<>(cascadeOne.getTargetMappingConfiguration()), cascadeOne.isFetchSeparately());
			}
			// Registering relation to EntityCriteria so one can use it as a criteria. Declared as a lazy initializer to work with lazy persister building such as cycling ones
			currentBuilderContext.addPostInitializers(new GraphLoadingRelationRegisterer<>(cascadeOne.getTargetMappingConfiguration().getEntityType(),
					cascadeOne.getTargetProvider()));
		}
		for (CascadeMany<C, TRGT, TRGTID, ? extends Collection<TRGT>> cascadeMany : entityMappingConfiguration.<TRGT, TRGTID>getOneToManys()) {
			CascadeManyConfigurer cascadeManyConfigurer = new CascadeManyConfigurer<>(cascadeMany,
					sourcePersister,
					dialect,
					connectionConfiguration,
					persisterRegistry,
					foreignKeyNamingStrategy,
					joinColumnNamingStrategy,
					associationTableNamingStrategy,
					indexColumnNamingStrategy);
			
			String relationName = AccessorDefinition.giveDefinition(cascadeMany.getCollectionProvider()).getName();
			
			if (currentBuilderContext.isCycling(cascadeMany.getTargetMappingConfiguration())) {
				// cycle detected
				// we had a second phase load because cycle can hardly be supported by simply joining things together because at one time we will
				// fall into infinite loop (think to SQL generation of a cycling graph ...)
				Class<TRGT> targetEntityType = cascadeMany.getTargetMappingConfiguration().getEntityType();
				// adding the relation to an eventually already existing cycle configurer for the entity
				OneToManyCycleConfigurer<TRGT> cycleSolver = (OneToManyCycleConfigurer<TRGT>)
						Iterables.find(currentBuilderContext.getPostInitializers(), p -> p instanceof OneToManyCycleConfigurer && p.getEntityType() == targetEntityType);
				if (cycleSolver == null) {
					cycleSolver = new OneToManyCycleConfigurer<>(targetEntityType);
					currentBuilderContext.addPostInitializers(cycleSolver);
				}
				cycleSolver.addCycleSolver(relationName, cascadeManyConfigurer);
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
		public void consume(EntityConfiguredJoinedTablesPersister<TRGT, ?> targetPersister) {
			sourcePersister.getCriteriaSupport().getRootConfiguration().registerRelation(targetEntityAccessor, targetPersister.getMapping());
		}
	}
}
