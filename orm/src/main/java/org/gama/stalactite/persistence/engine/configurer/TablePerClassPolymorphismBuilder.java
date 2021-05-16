package org.gama.stalactite.persistence.engine.configurer;

import java.util.HashMap;
import java.util.Map;

import org.gama.lang.collection.Arrays;
import org.gama.lang.exception.NotImplementedException;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.ValueAccessPointSet;
import org.gama.stalactite.persistence.engine.AssociationTableNamingStrategy;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.ElementCollectionTableNamingStrategy;
import org.gama.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.gama.stalactite.persistence.engine.PersisterRegistry;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.TablePerClassPolymorphism;
import org.gama.stalactite.persistence.engine.SubEntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.TableNamingStrategy;
import org.gama.stalactite.persistence.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.Identification;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.MappingPerTable.Mapping;
import org.gama.stalactite.persistence.engine.runtime.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.TablePerClassPolymorphismPersister;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IMappingStrategy.ShadowColumnValueProvider;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.IConnectionConfiguration;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.Nullable.nullable;

/**
 * @author Guillaume Mary
 */
class TablePerClassPolymorphismBuilder<C, I, T extends Table> extends AbstractPolymorphicPersisterBuilder<C, I, T> {
	
	private final TablePerClassPolymorphism<C> polymorphismPolicy;
	private final Map<IReversibleAccessor, Column> mainMapping;
	
	TablePerClassPolymorphismBuilder(TablePerClassPolymorphism<C> polymorphismPolicy,
									 Identification identification,
									 IEntityConfiguredJoinedTablesPersister<C, I> mainPersister,
									 Map<IReversibleAccessor, Column> mainMapping,
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
				elementCollectionTableNamingStrategy, joinColumnNamingStrategy, indexColumnNamingStrategy, associationTableNamingStrategy, tableNamingStrategy);
		this.polymorphismPolicy = polymorphismPolicy;
		this.mainMapping = mainMapping;
	}
	
	@Override
	public IEntityConfiguredJoinedTablesPersister<C, I> build(Dialect dialect, IConnectionConfiguration connectionConfiguration, PersisterRegistry persisterRegistry) {
		Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass = new HashMap<>();
		
		BeanMappingBuilder beanMappingBuilder = new BeanMappingBuilder();
		for (SubEntityMappingConfiguration<? extends C> subConfiguration : polymorphismPolicy.getSubClasses()) {
			// first we'll use table of columns defined in embedded override
			// then the one defined by inheritance
			// if both are null we'll create a new one
			Table tableDefinedByColumnOverride = BeanMappingBuilder.giveTargetTable(subConfiguration.getPropertiesMapping());
			Table tableDefinedByInheritanceConfiguration = polymorphismPolicy.giveTable(subConfiguration);
			
			assertNullOrEqual(tableDefinedByColumnOverride, tableDefinedByInheritanceConfiguration);
			
			Table subTable = nullable(tableDefinedByColumnOverride)
					.elseSet(tableDefinedByInheritanceConfiguration)
					.getOr(() -> new Table(tableNamingStrategy.giveName(subConfiguration.getEntityType())));
			
			Map<IReversibleAccessor, Column> subEntityPropertiesMapping = beanMappingBuilder.build(subConfiguration.getPropertiesMapping(), subTable,
					this.columnBinderRegistry, this.columnNameProvider);
			// in table-per-class polymorphism, main properties must be transfered to sub-entities ones, because CRUD operations are dipatched to them
			// by a proxy and main persister is not so much used
			Map<IReversibleAccessor, Column> projectedMainMapping = BeanMappingBuilder.projectColumns(mainMapping, subTable, (accessor, c) -> c.getName());
			subEntityPropertiesMapping.putAll(projectedMainMapping);
			addPrimarykey(subTable);
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
			
			// we need to copy also shadow columns, made in particular for one-to-one owned by source side because foreign key is maintained through it
			((ClassMappingStrategy<C, I, T>) mainPersister.getMappingStrategy()).getShadowColumnsForInsert().forEach(columnValueProvider -> {
				Column projectedColumn = subTable.addColumn(columnValueProvider.getColumn().getName(), columnValueProvider.getColumn().getJavaType());
				classMappingStrategy.addShadowColumnInsert(new ShadowColumnValueProvider<>(projectedColumn, columnValueProvider.getValueProvider()));
			});
			((ClassMappingStrategy<C, I, T>) mainPersister.getMappingStrategy()).getShadowColumnsForUpdate().forEach(columnValueProvider -> {
				Column projectedColumn = subTable.addColumn(columnValueProvider.getColumn().getName(), columnValueProvider.getColumn().getJavaType());
				classMappingStrategy.addShadowColumnUpdate(new ShadowColumnValueProvider<>(projectedColumn, columnValueProvider.getValueProvider()));
			});
			
			JoinedTablesPersister subclassPersister = new JoinedTablesPersister(classMappingStrategy, dialect, connectionConfiguration);
			persisterPerSubclass.put(subConfiguration.getEntityType(), subclassPersister);
		}
		
		// NB : we don't yet manage relations in table-per-class polymorphism because it causes problems of referential integrity when relation
		// is owned by target table since one column has to reference all tables of the hierarchy ! 
		// registerSubEntitiesRelations(dialect, connectionConfiguration, persisterRegistry, persisterPerSubclass);
		
		return new TablePerClassPolymorphismPersister<>(
				mainPersister, persisterPerSubclass, connectionConfiguration.getConnectionProvider(), dialect);
	}
	
	
	private void addPrimarykey(Table table) {
		PersisterBuilderImpl.propagatePrimarykey(this.mainPersister.getMappingStrategy().getTargetTable().getPrimaryKey(), Arrays.asSet(table));
	}
	
	private void addIdentificationToMapping(Identification identification, Mapping mapping) {
		PersisterBuilderImpl.addIdentificationToMapping(identification, Arrays.asSet(mapping));
	}
	
	@Override
	void assertSubPolymorphismIsSupported(PolymorphismPolicy<? extends C> subPolymorphismPolicy) {
		// Currently we don't support any kind of polymorphism with table-per-class
		// NOTE THAT THIS METHOD IS NOT CALLED because the one that calls it is not invoked by TablePerClassPolymorphismBuilder (because it doesn't
		// support relations)
		throw new NotImplementedException("Combining table-per-class polymorphism is not supported");
	}
}
