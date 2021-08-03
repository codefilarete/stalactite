package org.gama.stalactite.persistence.engine.configurer;

import java.util.HashMap;
import java.util.Map;

import org.gama.lang.Reflections;
import org.gama.lang.exception.NotImplementedException;
import org.gama.reflection.ReversibleAccessor;
import org.gama.reflection.ValueAccessPointSet;
import org.gama.stalactite.persistence.engine.AssociationTableNamingStrategy;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.ElementCollectionTableNamingStrategy;
import org.gama.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.gama.stalactite.persistence.engine.PersisterRegistry;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.gama.stalactite.persistence.engine.SubEntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.TableNamingStrategy;
import org.gama.stalactite.persistence.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.Identification;
import org.gama.stalactite.persistence.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.SingleTablePolymorphicPersister;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ConnectionConfiguration;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
class SingleTablePolymorphismBuilder<C, I, T extends Table, D> extends AbstractPolymorphicPersisterBuilder<C, I, T> {
	
	private final SingleTablePolymorphism<C, D> polymorphismPolicy;
	private final Map<ReversibleAccessor, Column> mainMapping;
	
	SingleTablePolymorphismBuilder(SingleTablePolymorphism<C, D> polymorphismPolicy,
								   Identification identification,
								   EntityConfiguredJoinedTablesPersister<C, I> mainPersister,
								   Map<ReversibleAccessor, Column> mainMapping,
								   ColumnBinderRegistry columnBinderRegistry,
								   ColumnNameProvider columnNameProvider,
								   TableNamingStrategy tableNamingStrategy, ColumnNamingStrategy columnNamingStrategy,
								   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
								   ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy,
								   ColumnNamingStrategy joinColumnNamingStrategy,
								   ColumnNamingStrategy indexColumnNamingStrategy,
								   AssociationTableNamingStrategy associationTableNamingStrategy
	) {
		super(polymorphismPolicy, identification, mainPersister, columnBinderRegistry, columnNameProvider, columnNamingStrategy, foreignKeyNamingStrategy,
				elementCollectionTableNamingStrategy, joinColumnNamingStrategy, indexColumnNamingStrategy, associationTableNamingStrategy, tableNamingStrategy);
		this.polymorphismPolicy = polymorphismPolicy;
		this.mainMapping = mainMapping;
	}
	
	@Override
	public EntityConfiguredJoinedTablesPersister<C, I> build(Dialect dialect, ConnectionConfiguration connectionConfiguration, PersisterRegistry persisterRegistry) {
		Map<Class<? extends C>, EntityConfiguredJoinedTablesPersister<C, I>> persisterPerSubclass = new HashMap<>();
		
		BeanMappingBuilder beanMappingBuilder = new BeanMappingBuilder();
		T mainTable = (T) mainPersister.getMappingStrategy().getTargetTable();
		for (SubEntityMappingConfiguration<? extends C> subConfiguration : polymorphismPolicy.getSubClasses()) {
			// first we'll use table of columns defined in embedded override
			// then the one defined by inheritance
			// if both are null we'll create a new one
			Table tableDefinedByColumnOverride = BeanMappingBuilder.giveTargetTable(subConfiguration.getPropertiesMapping());
			
			assertNullOrEqual(tableDefinedByColumnOverride, mainTable);
			
			Map<ReversibleAccessor, Column> subEntityPropertiesMapping = beanMappingBuilder.build(subConfiguration.getPropertiesMapping(), mainTable,
					this.columnBinderRegistry, this.columnNameProvider);
			// in single-table polymorphism, main properties must be given to sub-entities ones, because CRUD operations are dipatched to them
			// by a proxy and main persister is not so much used
			subEntityPropertiesMapping.putAll(mainMapping);
			ClassMappingStrategy<? extends C, I, T> classMappingStrategy = PersisterBuilderImpl.createClassMappingStrategy(
					false,
					mainTable,
					subEntityPropertiesMapping,
					new ValueAccessPointSet(),	// TODO: implement properties set by constructor feature in single-table polymorphism
					identification,
					subConfiguration.getPropertiesMapping().getBeanType(),
					null);
			// we need to copy also shadow columns, made in particular for one-to-one owned by source side because foreign key is maintained through it
			classMappingStrategy.addShadowColumns((ClassMappingStrategy) mainPersister.getMappingStrategy());
			
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
		
		registerCascades(persisterPerSubclass, dialect, connectionConfiguration, persisterRegistry);
		
		return surrogate;
	}
	
	@Override
	protected void assertSubPolymorphismIsSupported(PolymorphismPolicy<? extends C> subPolymorphismPolicy) {
		// Everything else than joined-tables is not implemented
		// - single-table with single-table is non sensence
		// - single-table with table-per-class is not implemented
		// Written as a negative condition to explicitly say what we support
		if (!(subPolymorphismPolicy instanceof PolymorphismPolicy.JoinedTablesPolymorphism)) {
			throw new NotImplementedException("Combining joined-tables polymorphism policy with " + Reflections.toString(subPolymorphismPolicy.getClass()));
		}
	}
	
	private Column<T, D> createDiscriminatorToSelect() {
		Column<T, D> result = mainPersister.getMappingStrategy().getTargetTable().addColumn(
				polymorphismPolicy.getDiscriminatorColumn(),
				polymorphismPolicy.getDiscrimintorType());
		result.setNullable(false);
		return result;
	}
	
}
