package org.gama.stalactite.persistence.engine.configurer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.ValueAccessPointSet;
import org.gama.stalactite.persistence.engine.AssociationTableNamingStrategy;
import org.gama.stalactite.persistence.engine.CascadeManyConfigurer;
import org.gama.stalactite.persistence.engine.CascadeOne;
import org.gama.stalactite.persistence.engine.CascadeOneConfigurer;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.ElementCollectionTableNamingStrategy;
import org.gama.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.gama.stalactite.persistence.engine.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.IEntityConfiguredPersister;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.gama.stalactite.persistence.engine.SubEntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.builder.ElementCollectionLinkage;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.Identification;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.MappingPerTable.Mapping;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.PolymorphismBuilder;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
class SingleTablePolymorphismBuilder<C, I, T extends Table, D> implements PolymorphismBuilder<C, I, T> {
	
	private final SingleTablePolymorphism<C, I, D> polymorphismPolicy;
	private final JoinedTablesPersister<C, I, T> mainPersister;
	private final Mapping mainMapping;
	private final ColumnBinderRegistry columnBinderRegistry;
	private final ColumnNameProvider columnNameProvider;
	
	private final ColumnNamingStrategy columnNamingStrategy;
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private final ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy;
	private final ColumnNamingStrategy joinColumnNamingStrategy;
	private final AssociationTableNamingStrategy associationTableNamingStrategy;
	
	private final Identification identification;
	
	SingleTablePolymorphismBuilder(SingleTablePolymorphism<C, I, D> polymorphismPolicy,
								   Identification identification,
								   JoinedTablesPersister<C, I, T> mainPersister,
								   Mapping mainMapping,
								   ColumnBinderRegistry columnBinderRegistry,
								   ColumnNameProvider columnNameProvider,
								   ColumnNamingStrategy columnNamingStrategy,
								   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
								   ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy,
								   ColumnNamingStrategy joinColumnNamingStrategy,
								   AssociationTableNamingStrategy associationTableNamingStrategy
	) {
		this.polymorphismPolicy = polymorphismPolicy;
		this.identification = identification;
		this.mainPersister = mainPersister;
		this.mainMapping = mainMapping;
		this.columnBinderRegistry = columnBinderRegistry;
		this.columnNameProvider = columnNameProvider;
		this.columnNamingStrategy = columnNamingStrategy;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		this.elementCollectionTableNamingStrategy = elementCollectionTableNamingStrategy;
		this.joinColumnNamingStrategy = joinColumnNamingStrategy;
		this.associationTableNamingStrategy = associationTableNamingStrategy;
	}
	
	@Override
	public IEntityConfiguredPersister<C, I> build(PersistenceContext persistenceContext) {
		Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass = new HashMap<>();
		
		BeanMappingBuilder beanMappingBuilder = new BeanMappingBuilder();
		for (SubEntityMappingConfiguration<? extends C, I> subConfiguration : polymorphismPolicy.getSubClasses()) {
			Map<IReversibleAccessor, Column> subEntityPropertiesMapping = beanMappingBuilder.build(subConfiguration.getPropertiesMapping(), mainPersister.getMainTable(),
					this.columnBinderRegistry, this.columnNameProvider);
			// in single-table polymorphism, main properties must be given to sub-entities ones, because CRUD operations are dipatched to them
			// by a proxy and main persister is not so much used
			subEntityPropertiesMapping.putAll(mainMapping.getMapping());
			ClassMappingStrategy<? extends C, I, T> classMappingStrategy = PersisterBuilderImpl.createClassMappingStrategy(
					false,
					(T) mainPersister.getMainTable(),
					subEntityPropertiesMapping,
					new ValueAccessPointSet(),	// TODO: implement properties set by constructor feature in single-table polymorphism
					identification,
					subConfiguration.getPropertiesMapping().getBeanType(),
					null);
			
			// no primary key to add nor foreign key since table is the same as main one (single table strategy)
			JoinedTablesPersister subclassPersister = new JoinedTablesPersister(persistenceContext, classMappingStrategy);
			
			persisterPerSubclass.put(subConfiguration.getEntityType(), subclassPersister);
		}
		
		Column<T, D> discriminatorColumn = createDiscriminatorToSelect();
		// NB: persisters are not registered into PersistenceContext because it may break implicit polymorphism principle (persisters are then
		// available by PersistenceContext.getPersister(..)) and it is one sure that they are perfect ones (all their features should be tested)
		SingleTablePolymorphicPersister<C, I, ?, ?> surrogate = new SingleTablePolymorphicPersister(
				mainPersister, persisterPerSubclass, persistenceContext.getConnectionProvider(), persistenceContext.getDialect(),
				discriminatorColumn, polymorphismPolicy);
		PersisterListenerWrapper<C, I> result = new PersisterListenerWrapper<>(surrogate);
		
		for (SubEntityMappingConfiguration<? extends C, I> subConfiguration : polymorphismPolicy.getSubClasses()) {
			JoinedTablesPersister<C, I, T> subEntityPersister = persisterPerSubclass.get(subConfiguration.getEntityType());
			
			// We register relation of sub class persister to take into account its specific one-to-ones, one-to-manys and element collection mapping
			registerRelationCascades(subConfiguration, persistenceContext, subEntityPersister);
		}
		
		return result;
		
	}
	
	private Column<T, D> createDiscriminatorToSelect() {
		Column<T, D> result = mainPersister.getMainTable().addColumn(
				polymorphismPolicy.getDiscriminatorColumn(),
				polymorphismPolicy.getDiscrimintorType());
		result.setNullable(false);
		return result;
	}
	
	private <D extends C> void registerRelationCascades(SubEntityMappingConfiguration<D, I> entityMappingConfiguration,
														PersistenceContext persistenceContext,
														IEntityConfiguredJoinedTablesPersister<C, I> sourcePersister) {
		for (CascadeOne<D, ?, ?> cascadeOne : entityMappingConfiguration.getOneToOnes()) {
			CascadeOneConfigurer cascadeOneConfigurer = new CascadeOneConfigurer<>(persistenceContext, new PersisterBuilderImpl<>(cascadeOne.getTargetMappingConfiguration()));
			cascadeOneConfigurer.appendCascade(cascadeOne, sourcePersister,
					this.foreignKeyNamingStrategy,
					this.joinColumnNamingStrategy);
		}
		for (CascadeMany<D, ?, ?, ? extends Collection> cascadeMany : entityMappingConfiguration.getOneToManys()) {
			CascadeManyConfigurer cascadeManyConfigurer = new CascadeManyConfigurer<>(persistenceContext, new PersisterBuilderImpl<>(cascadeMany.getTargetMappingConfiguration()));
			cascadeManyConfigurer.appendCascade(cascadeMany, sourcePersister,
					this.foreignKeyNamingStrategy,
					this.joinColumnNamingStrategy,
					this.associationTableNamingStrategy);
		}
		// Please not that as a difference with PersisterBuilderImpl, we don't need to register relation in select because polymorphic selection
		// is made in two phases, see JoinedTablesPolymorphismEntitySelectExecutor (instanciated in JoinedTablesPolymorphicPersister)
		
		// taking element collections into account
		for (ElementCollectionLinkage<D, ?, ? extends Collection> elementCollection : entityMappingConfiguration.getElementCollections()) {
			ElementCollectionCascadeConfigurer elementCollectionCascadeConfigurer = new ElementCollectionCascadeConfigurer(persistenceContext);
			elementCollectionCascadeConfigurer.appendCascade(elementCollection, sourcePersister, foreignKeyNamingStrategy, columnNamingStrategy,
					elementCollectionTableNamingStrategy);
		}
	}
	
}
