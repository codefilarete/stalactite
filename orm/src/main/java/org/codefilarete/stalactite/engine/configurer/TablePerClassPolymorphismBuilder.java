package org.codefilarete.stalactite.engine.configurer;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.TablePerClassPolymorphism;
import org.codefilarete.stalactite.engine.SubEntityMappingConfiguration;
import org.codefilarete.stalactite.engine.TableNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.Identification;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.MappingPerTable.Mapping;
import org.codefilarete.stalactite.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.TablePerClassPolymorphismPersister;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.Mapping.ShadowColumnValueProvider;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.exception.NotImplementedException;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * @author Guillaume Mary
 */
class TablePerClassPolymorphismBuilder<C, I, T extends Table> extends AbstractPolymorphicPersisterBuilder<C, I, T> {
	
	private final Map<ReversibleAccessor<C, ?>, Column<T, ?>> mainMapping;
	
	TablePerClassPolymorphismBuilder(TablePerClassPolymorphism<C> polymorphismPolicy,
									 Identification<C, I> identification,
									 EntityConfiguredJoinedTablesPersister<C, I> mainPersister,
									 Map<ReversibleAccessor<C, ?>, Column<T, ?>> mainMapping,
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
		this.mainMapping = mainMapping;
	}
	
	@Override
	public EntityConfiguredJoinedTablesPersister<C, I> build(Dialect dialect, ConnectionConfiguration connectionConfiguration, PersisterRegistry persisterRegistry) {
		Map<Class<? extends C>, SimpleRelationalEntityPersister<C, I, T>> persisterPerSubclass = new HashMap<>();
		
		BeanMappingBuilder beanMappingBuilder = new BeanMappingBuilder();
		for (SubEntityMappingConfiguration<? extends C> subConfiguration : polymorphismPolicy.getSubClasses()) {
			SimpleRelationalEntityPersister subclassPersister = buildSubclassPersister(dialect, connectionConfiguration,
																								   beanMappingBuilder, subConfiguration);
			persisterPerSubclass.put(subConfiguration.getEntityType(), subclassPersister);
		}
		
		// NB : we don't yet manage relations in table-per-class polymorphism because it causes problems of referential integrity when relation
		// is owned by target table since one column has to reference all tables of the hierarchy ! 
		// registerSubEntitiesRelations(dialect, connectionConfiguration, persisterRegistry, persisterPerSubclass);
		
		return new TablePerClassPolymorphismPersister<>(
				mainPersister, persisterPerSubclass, connectionConfiguration.getConnectionProvider(), dialect);
	}
	
	private <D> SimpleRelationalEntityPersister<D, I, Table> buildSubclassPersister(Dialect dialect,
																			ConnectionConfiguration connectionConfiguration,
																			BeanMappingBuilder beanMappingBuilder,
																			SubEntityMappingConfiguration<D> subConfiguration) {
		// first we'll use table of columns defined in embedded override
		// then the one defined by inheritance
		// if both are null we'll create a new one
		Table tableDefinedByColumnOverride = BeanMappingBuilder.giveTargetTable(subConfiguration.getPropertiesMapping());
		Table tableDefinedByInheritanceConfiguration = ((TablePerClassPolymorphism<D>) polymorphismPolicy).giveTable(subConfiguration);
		
		assertNullOrEqual(tableDefinedByColumnOverride, tableDefinedByInheritanceConfiguration);
		
		Table subTable = nullable(tableDefinedByColumnOverride)
				.elseSet(tableDefinedByInheritanceConfiguration)
				.getOr(() -> new Table(tableNamingStrategy.giveName(subConfiguration.getEntityType())));
		
		Map<ReversibleAccessor, Column> subEntityPropertiesMapping = beanMappingBuilder.build(subConfiguration.getPropertiesMapping(), subTable,
																							  this.columnBinderRegistry, this.columnNameProvider);
		// in table-per-class polymorphism, main properties must be transferred to sub-entities ones, because CRUD operations are dispatched to them
		// by a proxy and main persister is not so much used
		addPrimarykey(subTable);
		Map<ReversibleAccessor, Column> projectedMainMapping = BeanMappingBuilder.projectColumns(mainMapping, subTable, (accessor, c) -> c.getName());
		subEntityPropertiesMapping.putAll(projectedMainMapping);
		Mapping subEntityMapping = new Mapping(subConfiguration, subTable, subEntityPropertiesMapping, false);
		addIdentificationToMapping(identification, subEntityMapping);
		ClassMapping<D, I, Table> classMappingStrategy = PersisterBuilderImpl.createClassMappingStrategy(
			true,	// given Identification (which is parent one) contains identifier policy
			subTable,
			subEntityMapping.getMapping(),
			new ValueAccessPointSet(),    // TODO: implement properties set by constructor feature in table-per-class polymorphism 
			(Identification<D, I>) identification,
			subConfiguration.getPropertiesMapping().getBeanType(),
			null);
		
		// we need to copy also shadow columns, made in particular for one-to-one owned by source side because foreign key is maintained through it
		((ClassMapping<C, I, T>) mainPersister.getMapping()).getShadowColumnsForInsert().forEach(columnValueProvider -> {
			Column projectedColumn = subTable.addColumn(columnValueProvider.getColumn().getName(), columnValueProvider.getColumn().getJavaType());
			classMappingStrategy.addShadowColumnInsert(new ShadowColumnValueProvider<>(projectedColumn, columnValueProvider.getValueProvider()));
		});
		((ClassMapping<C, I, T>) mainPersister.getMapping()).getShadowColumnsForUpdate().forEach(columnValueProvider -> {
			Column projectedColumn = subTable.addColumn(columnValueProvider.getColumn().getName(), columnValueProvider.getColumn().getJavaType());
			classMappingStrategy.addShadowColumnUpdate(new ShadowColumnValueProvider<>(projectedColumn, columnValueProvider.getValueProvider()));
		});
		
		return new SimpleRelationalEntityPersister<>(classMappingStrategy, dialect, connectionConfiguration);
	}
	
	
	private void addPrimarykey(Table table) {
		PersisterBuilderImpl.propagatePrimarykey(this.mainPersister.getMapping().getTargetTable().getPrimaryKey(), Arrays.asSet(table));
	}
	
	private void addIdentificationToMapping(Identification<C, I> identification, Mapping mapping) {
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
