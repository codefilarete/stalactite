package org.codefilarete.stalactite.engine.configurer.polymorphism;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ColumnOptions;
import org.codefilarete.stalactite.engine.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.TablePerClassPolymorphism;
import org.codefilarete.stalactite.engine.SubEntityMappingConfiguration;
import org.codefilarete.stalactite.engine.TableNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification.Identification;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.MappingPerTable.Mapping;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.TablePerClassPolymorphismPersister;
import org.codefilarete.stalactite.mapping.ClassMapping;
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
class TablePerClassPolymorphismBuilder<C, I, T extends Table<T>> extends AbstractPolymorphicPersisterBuilder<C, I, T> {
	
	private final Map<ReversibleAccessor<C, Object>, Column<T, Object>> mainMapping;
	
	TablePerClassPolymorphismBuilder(TablePerClassPolymorphism<C> polymorphismPolicy,
									 AbstractIdentification<C, I> identification,
									 ConfiguredRelationalPersister<C, I> mainPersister,
									 Map<? extends ReversibleAccessor<C, Object>, Column<T, Object>> mainMapping,
									 ColumnBinderRegistry columnBinderRegistry,
									 ColumnNameProvider columnNameProvider,
									 TableNamingStrategy tableNamingStrategy,
									 ColumnNamingStrategy columnNamingStrategy,
									 ForeignKeyNamingStrategy foreignKeyNamingStrategy,
									 ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy,
									 JoinColumnNamingStrategy joinColumnNamingStrategy,
									 ColumnNamingStrategy indexColumnNamingStrategy,
									 AssociationTableNamingStrategy associationTableNamingStrategy) {
		super(polymorphismPolicy, identification, mainPersister, columnBinderRegistry, columnNameProvider, columnNamingStrategy, foreignKeyNamingStrategy,
				elementCollectionTableNamingStrategy, joinColumnNamingStrategy, indexColumnNamingStrategy, associationTableNamingStrategy, tableNamingStrategy);
		this.mainMapping = (Map<ReversibleAccessor<C, Object>, Column<T, Object>>) mainMapping;
	}
	
	@Override
	public ConfiguredRelationalPersister<C, I> build(Dialect dialect, ConnectionConfiguration connectionConfiguration, PersisterRegistry persisterRegistry) {
		if (this.identification instanceof Identification && ((Identification<C, I>) this.identification).getIdentifierPolicy() instanceof ColumnOptions.AfterInsertIdentifierPolicy) {
			throw new UnsupportedOperationException("Table-per-class polymorphism is not compatible with auto-incremented primary key");
		}
		Map<Class<? extends C>, SimpleRelationalEntityPersister<? extends C, I, ?>> persisterPerSubclass =
				(Map) collectSubClassPersister(dialect, connectionConfiguration);
		
		// NB : we don't yet manage relations in table-per-class polymorphism because it causes problems of referential integrity when relation
		// is owned by target table since one column has to reference all tables of the hierarchy ! 
		// registerSubEntitiesRelations(dialect, connectionConfiguration, persisterRegistry, persisterPerSubclass);
		registerCascades((Map) persisterPerSubclass, dialect, connectionConfiguration, persisterRegistry);
		
		TablePerClassPolymorphismPersister<C, I, T> result = new TablePerClassPolymorphismPersister<>(
				mainPersister, persisterPerSubclass, connectionConfiguration.getConnectionProvider(), dialect);
		
		// we propagate shadow columns through TablePerClassPolymorphismPersister.getMapping() because it has a
		// mechanism that projects them to sub-persisters (but columns were added in buildSubclassPersister()
		// which is not very consistent)
		((ClassMapping<C, I, T>) mainPersister.getMapping()).getShadowColumnsForInsert().forEach(columnProvider -> {
			result.getMapping().addShadowColumnInsert(columnProvider);
		});
		((ClassMapping<C, I, T>) mainPersister.getMapping()).getShadowColumnsForUpdate().forEach(columnProvider -> {
			result.getMapping().addShadowColumnUpdate(columnProvider);
		});
		
		return result;
	}
	
	private <D extends C> Map<Class<D>, ConfiguredRelationalPersister<D, I>> collectSubClassPersister(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		Map<Class<D>, ConfiguredRelationalPersister<D, I>> persisterPerSubclass = new HashMap<>();
		
		BeanMappingBuilder<D, T> beanMappingBuilder = new BeanMappingBuilder<>();
		for (SubEntityMappingConfiguration<D> subConfiguration : ((Set<SubEntityMappingConfiguration<D>>) (Set) polymorphismPolicy.getSubClasses())) {
			SimpleRelationalEntityPersister<D, I, ?> subclassPersister = buildSubclassPersister(dialect, connectionConfiguration,
					beanMappingBuilder, subConfiguration);
			persisterPerSubclass.put(subConfiguration.getEntityType(), subclassPersister);
		}
		
		return persisterPerSubclass;
	}
	
	private <D, SUBTABLE extends Table<SUBTABLE>> SimpleRelationalEntityPersister<D, I, SUBTABLE>
	buildSubclassPersister(Dialect dialect,
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
		addPrimaryKey(subTable);
		Map<ReversibleAccessor, Column> projectedMainMapping = BeanMappingBuilder.projectColumns(mainMapping, subTable, (accessor, c) -> c.getName());
		subEntityPropertiesMapping.putAll(projectedMainMapping);
		Mapping subEntityMapping = new Mapping(subConfiguration, subTable, subEntityPropertiesMapping, false);
		addIdentificationToMapping(identification, subEntityMapping);
		ClassMapping<D, I, SUBTABLE> classMappingStrategy = PersisterBuilderImpl.createClassMappingStrategy(
			true,	// given Identification (which is parent one) contains identifier policy
			subTable,
			subEntityMapping.getMapping(),
			new ValueAccessPointSet(),    // TODO: implement properties set by constructor feature in table-per-class polymorphism 
			(Identification<D, I>) identification,
			subConfiguration.getPropertiesMapping().getBeanType(),
			null);
		
		((ClassMapping<C, I, T>) mainPersister.getMapping()).getShadowColumnsForInsert().forEach(columnProvider -> {
			columnProvider.getColumns().forEach(c -> {
				Column column = subTable.addColumn(c.getName(), c.getJavaType(), c.getSize());
				column.setNullable(c.isNullable());
			});
		});
		((ClassMapping<C, I, T>) mainPersister.getMapping()).getShadowColumnsForUpdate().forEach(columnProvider -> {
			columnProvider.getColumns().forEach(c -> {
				Column column = subTable.addColumn(c.getName(), c.getJavaType(), c.getSize());
				column.setNullable(c.isNullable());
			});
		});
		
		return new SimpleRelationalEntityPersister<>(classMappingStrategy, dialect, connectionConfiguration);
	}
	
	private void addPrimaryKey(Table table) {
		PersisterBuilderImpl.propagatePrimaryKey(this.mainPersister.getMapping().getTargetTable().getPrimaryKey(), Arrays.asSet(table));
	}
	
	private void addIdentificationToMapping(AbstractIdentification<C, I> identification, Mapping mapping) {
		PersisterBuilderImpl.addIdentificationToMapping(identification, Arrays.asSet(mapping));
	}
	
	@Override
	void assertSubPolymorphismIsSupported(PolymorphismPolicy<? extends C> subPolymorphismPolicy) {
		// Currently we don't support any kind of polymorphism with table-per-class
		throw new NotImplementedException("Combining table-per-class polymorphism is not supported");
	}
}
