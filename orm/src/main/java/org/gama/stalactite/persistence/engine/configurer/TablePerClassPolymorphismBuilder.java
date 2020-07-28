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
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.TablePerClassPolymorphism;
import org.gama.stalactite.persistence.engine.SubEntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.TableNamingStrategy;
import org.gama.stalactite.persistence.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.Identification;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.MappingPerTable.Mapping;
import org.gama.stalactite.persistence.engine.runtime.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.PersisterListenerWrapper;
import org.gama.stalactite.persistence.engine.runtime.TablePerClassPolymorphismPersister;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.IConnectionConfiguration;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.Nullable.nullable;

/**
 * @author Guillaume Mary
 */
abstract class TablePerClassPolymorphismBuilder<C, I, T extends Table> extends AbstractPolymorphicPersisterBuilder<C, I, T> {
	
	private final TablePerClassPolymorphism<C, I> polymorphismPolicy;
	private final Mapping mainMapping;
	private final TableNamingStrategy tableNamingStrategy;
	
	TablePerClassPolymorphismBuilder(TablePerClassPolymorphism<C, I> polymorphismPolicy,
									 Identification identification,
									 JoinedTablesPersister<C, I, T> mainPersister,
									 Mapping mainMapping,
									 ColumnBinderRegistry columnBinderRegistry,
									 ColumnNameProvider columnNameProvider,
									 TableNamingStrategy tableNamingStrategy,
									 ColumnNamingStrategy columnNamingStrategy,
									 ForeignKeyNamingStrategy foreignKeyNamingStrategy,
									 ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy,
									 ColumnNamingStrategy joinColumnNamingStrategy,
									 ColumnNamingStrategy indexColumnNamingStrategy,
									 AssociationTableNamingStrategy associationTableNamingStrategy) {
		super(polymorphismPolicy, identification, mainPersister, columnBinderRegistry, columnNameProvider, columnNamingStrategy, foreignKeyNamingStrategy,
				elementCollectionTableNamingStrategy, joinColumnNamingStrategy, indexColumnNamingStrategy, associationTableNamingStrategy);
		this.polymorphismPolicy = polymorphismPolicy;
		this.mainMapping = mainMapping;
		this.tableNamingStrategy = tableNamingStrategy;
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
			Table tableDefinedByInheritanceConfiguration = polymorphismPolicy.giveTable(subConfiguration);
			
			assertAllAreEqual(tableDefinedByColumnOverride, tableDefinedByInheritanceConfiguration);
			
			Table subTable = nullable(tableDefinedByColumnOverride)
					.elseSet(tableDefinedByInheritanceConfiguration)
					.getOr(() -> new Table(tableNamingStrategy.giveName(subConfiguration.getEntityType())));
			
			Map<IReversibleAccessor, Column> subEntityPropertiesMapping = beanMappingBuilder.build(subConfiguration.getPropertiesMapping(), subTable,
					this.columnBinderRegistry, this.columnNameProvider);
			// in table-per-class polymorphism, main properties must be transfered to sub-entities ones, because CRUD operations are dipatched to them
			// by a proxy and main persister is not so much used
			Map<IReversibleAccessor, Column> projectedMainMapping = BeanMappingBuilder.projectColumns(mainMapping.getMapping(), subTable, (accessor, c) -> c.getName());
			subEntityPropertiesMapping.putAll(projectedMainMapping);
			addPrimarykey(identification, subTable);
			Mapping subEntityMapping = new Mapping(subConfiguration, subTable, subEntityPropertiesMapping, false);
			addIdentificationToMapping(identification, subEntityMapping);
			ClassMappingStrategy<? extends C, I, Table> classMappingStrategy = PersisterBuilderImpl.createClassMappingStrategy(
					false,
					subTable,
					subEntityMapping.getMapping(),
					new ValueAccessPointSet(),	// TODO: implement properties set by constructor feature in table-per-class polymorphism 
					identification,
					subConfiguration.getPropertiesMapping().getBeanType(),
					null);
			
			JoinedTablesPersister subclassPersister = new JoinedTablesPersister(classMappingStrategy, dialect, connectionConfiguration);
			persisterPerSubclass.put(subConfiguration.getEntityType(), subclassPersister);
		}
		
		// NB : we don't yet manage relations in table-per-class polymorphism because it causes problems of referential integrity when relation
		// is owned by target table since one column has to reference all tables of the hierarchy ! 
		// registerSubEntitiesRelations(dialect, connectionConfiguration, persisterRegistry, persisterPerSubclass);
		
		return new PersisterListenerWrapper<>(new TablePerClassPolymorphismPersister<>(
				mainPersister, persisterPerSubclass, connectionConfiguration.getConnectionProvider(), dialect));
	}
	
	
	abstract void addPrimarykey(Identification identification, Table table);
	
	abstract void addIdentificationToMapping(Identification identification, Mapping mapping);
}
