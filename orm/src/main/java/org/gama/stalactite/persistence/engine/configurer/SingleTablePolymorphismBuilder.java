package org.gama.stalactite.persistence.engine.configurer;

import java.util.HashMap;
import java.util.Map;

import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.ValueAccessPointSet;
import org.gama.stalactite.persistence.engine.AssociationTableNamingStrategy;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.ElementCollectionTableNamingStrategy;
import org.gama.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.gama.stalactite.persistence.engine.PersisterRegistry;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.gama.stalactite.persistence.engine.SubEntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.Identification;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.MappingPerTable.Mapping;
import org.gama.stalactite.persistence.engine.runtime.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.PersisterListenerWrapper;
import org.gama.stalactite.persistence.engine.runtime.SingleTablePolymorphicPersister;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.IConnectionConfiguration;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
class SingleTablePolymorphismBuilder<C, I, T extends Table, D> extends AbstractPolymorphicPersisterBuilder<C, I, T> {
	
	private final SingleTablePolymorphism<C, I, D> polymorphismPolicy;
	private final Mapping mainMapping;
	
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
								   ColumnNamingStrategy indexColumnNamingStrategy,
								   AssociationTableNamingStrategy associationTableNamingStrategy
	) {
		super(polymorphismPolicy, identification, mainPersister, columnBinderRegistry, columnNameProvider, columnNamingStrategy, foreignKeyNamingStrategy,
				elementCollectionTableNamingStrategy, joinColumnNamingStrategy, indexColumnNamingStrategy, associationTableNamingStrategy);
		this.polymorphismPolicy = polymorphismPolicy;
		this.mainMapping = mainMapping;
	}
	
	@Override
	public IEntityConfiguredJoinedTablesPersister<C, I> build(Dialect dialect, IConnectionConfiguration connectionConfiguration, PersisterRegistry persisterRegistry) {
		Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass = new HashMap<>();
		
		BeanMappingBuilder beanMappingBuilder = new BeanMappingBuilder();
		for (SubEntityMappingConfiguration<? extends C, I> subConfiguration : polymorphismPolicy.getSubClasses()) {
			// first we'll use table of columns defined in embedded override
			// then the one defined by inheritance
			// if both are null we'll create a new one
			Table tableDefinedByColumnOverride = BeanMappingBuilder.giveTargetTable(subConfiguration.getPropertiesMapping());
			Table tableDefinedByInheritanceConfiguration = mainPersister.getMainTable();
			
			assertAllAreEqual(tableDefinedByColumnOverride, tableDefinedByInheritanceConfiguration);
			
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
			JoinedTablesPersister subclassPersister = new JoinedTablesPersister(classMappingStrategy, dialect, connectionConfiguration);
			
			persisterPerSubclass.put(subConfiguration.getEntityType(), subclassPersister);
		}
		
		Column<T, D> discriminatorColumn = createDiscriminatorToSelect();
		// NB: persisters are not registered into PersistenceContext because it may break implicit polymorphism principle (persisters are then
		// available by PersistenceContext.getPersister(..)) and it is one sure that they are perfect ones (all their features should be tested)
		SingleTablePolymorphicPersister<C, I, ?, ?> surrogate = new SingleTablePolymorphicPersister<>(
				mainPersister, persisterPerSubclass, connectionConfiguration.getConnectionProvider(), dialect,
				discriminatorColumn, polymorphismPolicy);
		PersisterListenerWrapper<C, I> result = new PersisterListenerWrapper<>(surrogate);
		
		for (SubEntityMappingConfiguration<? extends C, I> subConfiguration : polymorphismPolicy.getSubClasses()) {
			JoinedTablesPersister<C, I, T> subEntityPersister = persisterPerSubclass.get(subConfiguration.getEntityType());
			
			// We register relation of sub class persister to take into account its specific one-to-ones, one-to-manys and element collection mapping
			registerRelationCascades(subConfiguration, dialect, connectionConfiguration, persisterRegistry, subEntityPersister);
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
	
}
