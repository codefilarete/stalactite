package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.PropertyAccessPoint;
import org.codefilarete.reflection.ReadWriteAccessorChain;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilder;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.PartialRepresentation;
import org.codefilarete.stalactite.engine.configurer.model.Entity.AbstractPropertyMapping;
import org.codefilarete.stalactite.engine.configurer.model.Entity.PropertyMapping;
import org.codefilarete.stalactite.engine.configurer.model.Entity.ReadOnlyPropertyMapping;
import org.codefilarete.stalactite.engine.model.AbstractCountry;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.King;
import org.codefilarete.stalactite.engine.model.Realm;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.function.Functions;
import org.codefilarete.trace.ObjectPrinterBuilder;
import org.codefilarete.trace.ObjectPrinterBuilder.ObjectPrinter;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.reflection.Accessors.mutatorByMethodReference;
import static org.codefilarete.reflection.Accessors.readWriteAccessPoint;
import static org.codefilarete.stalactite.dsl.FluentMappings.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;

class PropertyMappingResolverTest {
	
	// we use a printer to compare our results because there's some complexity on fields of AbstractPropertyMapping
	// especially with ValueAccessPoint comparison. Moreover, AssertJ output is not so helpful because it's based
	// on toString() which is not overridden by classes implied in the AbstractPropertyMapping structure.
	static final ObjectPrinter<AbstractPropertyMapping<?, ?, ?>> ABSTRACT_PROPERTY_MAPPING_OBJECT_PRINTER = new ObjectPrinterBuilder<AbstractPropertyMapping<?, ?, ?>>()
			.addProperty(AbstractPropertyMapping::getAccessPoint)
			.addProperty(AbstractPropertyMapping::getColumn)
			.addProperty(AbstractPropertyMapping::isSetByConstructor)
			.addProperty(AbstractPropertyMapping::isUnique)
			.addProperty(AbstractPropertyMapping::getReadConverter)
			.<PropertyMapping<Country, ?, ?>>addProperty(PropertyMapping::getWriteConverter)
			.withPrinter(PropertyAccessPoint.class, AccessorDefinition::toString)
			.withPrinter(Column.class, Column::toString)
			.build();
	
	static final Comparator<AbstractPropertyMapping<?, ?, ?>> ABSTRACT_PROPERTY_MAPPING_COMPARATOR = Comparator.comparing(ABSTRACT_PROPERTY_MAPPING_OBJECT_PRINTER::toString);
	
	static final PartialRepresentation<AbstractPropertyMapping<?, ?, ?>> ABSTRACT_PROPERTY_MAPPING_REPRESENTATION = new PartialRepresentation<>((Class<AbstractPropertyMapping<?, ?, ?>>) (Class) AbstractPropertyMapping.class, ABSTRACT_PROPERTY_MAPPING_OBJECT_PRINTER);
	
	@Test
	<T extends Table<T>> void directMapping() {
		FluentEmbeddableMappingBuilder<Country> embeddableCountryMapping = embeddableBuilder(Country.class)
				.map(Country::getName)
				.map(Country::setDescription).readonly();
		
		T countryTable = (T) new Table("Country");
		PropertyMappingResolver<Country, T> testInstance = new PropertyMappingResolver<>(new ColumnBinderRegistry());
		
		ResolvedPropertyMapping<Country, T> actualResult = new ResolvedPropertyMapping<>(testInstance.resolve(embeddableCountryMapping.getConfiguration(), countryTable, ColumnNamingStrategy.DEFAULT), countryTable);
		
		assertThat(actualResult.getMappings())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						new PropertyMapping<>(readWriteAccessPoint(Country::getName), countryTable.getColumn("name"), false, null, null, false),
						new ReadOnlyPropertyMapping<>(mutatorByMethodReference(Country::setDescription), countryTable.getColumn("description"), false, null, false)
				));
	}
	
	@Test
	<T extends Table<T>> void directMapping_setByConstructor() {
		FluentEmbeddableMappingBuilder<Country> embeddableCountryMapping = embeddableBuilder(Country.class)
				.map(Country::getName).setByConstructor()
				.map(Country::getDescription);
		
		T countryTable = (T) new Table("Country");
		PropertyMappingResolver<Country, T> testInstance = new PropertyMappingResolver<>(new ColumnBinderRegistry());
		
		ResolvedPropertyMapping<Country, T> actualResult = new ResolvedPropertyMapping<>(testInstance.resolve(embeddableCountryMapping.getConfiguration(), countryTable, ColumnNamingStrategy.DEFAULT), countryTable);
		
		assertThat(actualResult.getMappings())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						new PropertyMapping<>(readWriteAccessPoint(Country::getName), countryTable.getColumn("name"), true, null, null, false),
						new PropertyMapping<>(readWriteAccessPoint(Country::getDescription), countryTable.getColumn("description"), false, null, null, false)
				));
	}
	
	@Test
	<T extends Table<T>> void directMapping_unique() {
		FluentEmbeddableMappingBuilder<Country> embeddableCountryMapping = embeddableBuilder(Country.class)
				.map(Country::getName).unique()
				.map(Country::getDescription);
		
		T countryTable = (T) new Table("Country");
		PropertyMappingResolver<Country, T> testInstance = new PropertyMappingResolver<>(new ColumnBinderRegistry());
		
		ResolvedPropertyMapping<Country, T> actualResult = new ResolvedPropertyMapping<>(testInstance.resolve(embeddableCountryMapping.getConfiguration(), countryTable, ColumnNamingStrategy.DEFAULT), countryTable);
		
		assertThat(actualResult.getMappings())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						new PropertyMapping<>(readWriteAccessPoint(Country::getName), countryTable.getColumn("name"), false, null, null, true),
						new PropertyMapping<>(readWriteAccessPoint(Country::getDescription), countryTable.getColumn("description"), false, null, null, false)
				));
	}
	
	@Test
	<T extends Table<T>> void directMapping_converters() {
		Converter<Object, String> readConverter = input -> input.toString().toUpperCase() + " !";
		Converter<String, String> writeConverter = input -> input.toUpperCase() + " !";
		FluentEmbeddableMappingBuilder<Country> embeddableCountryMapping = embeddableBuilder(Country.class)
				.map(Country::getName)
					.readConverter(readConverter)
					.writeConverter(writeConverter)
				.map(Country::getDescription);
		
		T countryTable = (T) new Table("Country");
		PropertyMappingResolver<Country, T> testInstance = new PropertyMappingResolver<>(new ColumnBinderRegistry());
		
		ResolvedPropertyMapping<Country, T> actualResult = new ResolvedPropertyMapping<>(testInstance.resolve(embeddableCountryMapping.getConfiguration(), countryTable, ColumnNamingStrategy.DEFAULT), countryTable);
		
		assertThat(actualResult.getMappings())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						new PropertyMapping<>(readWriteAccessPoint(Country::getName), countryTable.getColumn("name"), false, readConverter, writeConverter, false),
						new PropertyMapping<>(readWriteAccessPoint(Country::getDescription), countryTable.getColumn("description"), false, null, null, false)
				));
	}
	
	@Test
	<T extends Table<T>> void directMapping_withColumnOverride() {
		FluentEmbeddableMappingBuilder<Country> embeddableCountryMapping = embeddableBuilder(Country.class)
				.map(Country::getName)
				.map(Country::getDescription)
					.columnName("xx")
					.columnSize(Size.length(42));
		
		T countryTable = (T) new Table("Country");
		PropertyMappingResolver<Country, T> testInstance = new PropertyMappingResolver<>(new ColumnBinderRegistry());
		
		ResolvedPropertyMapping<Country, T> actualResult = new ResolvedPropertyMapping<>(testInstance.resolve(embeddableCountryMapping.getConfiguration(), countryTable, ColumnNamingStrategy.DEFAULT), countryTable);
		
		Column<T, String> descriptionColumn = countryTable.getColumn("xx");
		assertThat(actualResult.getMappings())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						new PropertyMapping<>(readWriteAccessPoint(Country::getName), countryTable.getColumn("name"), false, null, null, false),
						new PropertyMapping<>(readWriteAccessPoint(Country::getDescription), descriptionColumn, false, null, null, false)
				));
		
		assertThat(descriptionColumn.getSize())
				.usingRecursiveComparison()
				.isEqualTo(Size.length(42));
	}
	
	@Test
	<T extends Table<T>> void mappedSuperClass() {
		FluentEmbeddableMappingBuilder<Country> embeddableCountryMapping = embeddableBuilder(Country.class)
				.map(Country::getName)
				.map(Country::getDescription).columnName("xx")
				.mapSuperClass(
						embeddableBuilder(AbstractCountry.class)
								.map(AbstractCountry::getDummyProperty).columnName("myProperty").setByConstructor()
				);
		
		T countryTable = (T) new Table("Country");
		PropertyMappingResolver<Country, T> testInstance = new PropertyMappingResolver<>(new ColumnBinderRegistry());
		
		ResolvedPropertyMapping<Country, T> actualResult = new ResolvedPropertyMapping<>(testInstance.resolve(embeddableCountryMapping.getConfiguration(), countryTable, ColumnNamingStrategy.DEFAULT), countryTable);
		
		assertThat(actualResult.getMappings())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						new PropertyMapping<>(readWriteAccessPoint(Country::getName), countryTable.getColumn("name"), false, null, null, false),
						new PropertyMapping<>(readWriteAccessPoint(Country::getDescription), countryTable.getColumn("xx"), false, null, null, false),
						new PropertyMapping<>(readWriteAccessPoint(AbstractCountry::getDummyProperty), countryTable.getColumn("myProperty"), true, null, null, false)
				));
	}
	
	@Test
	<T extends Table<T>> void embeddedBean() {
		FluentEmbeddableMappingBuilder<Country> embeddableCountryMapping = embeddableBuilder(Country.class)
				.map(Country::getName)
				.embed(Country::getTimestamp, embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::setModificationDate).setByConstructor()
				)
					.overrideName(Timestamp::getCreationDate, "creation_date")
					.overrideSize(Timestamp::getCreationDate, Size.length(42))
				;
		
		T countryTable = (T) new Table("Country");
		PropertyMappingResolver<Country, T> testInstance = new PropertyMappingResolver<>(new ColumnBinderRegistry());
		
		ResolvedPropertyMapping<Country, T> actualResult = new ResolvedPropertyMapping<>(testInstance.resolve(embeddableCountryMapping.getConfiguration(), countryTable, ColumnNamingStrategy.DEFAULT), countryTable);
		
		List<ReadWritePropertyAccessPoint<Country, Timestamp>> embeddablePrefix = Arrays.asList(readWriteAccessPoint(Country::getTimestamp));
		Column<T, Date> creationDateColumn = countryTable.getColumn("creation_date");
		assertThat(actualResult.getMappings())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						new PropertyMapping<>(new ReadWriteAccessorChain<>(embeddablePrefix, readWriteAccessPoint(Timestamp::getCreationDate)), creationDateColumn, false, null, null, false),
						new PropertyMapping<>(new ReadWriteAccessorChain<>(embeddablePrefix, readWriteAccessPoint(Timestamp::setModificationDate)), countryTable.getColumn("modificationDate"), true, null, null, false),
						new PropertyMapping<>(readWriteAccessPoint(Country::getName), countryTable.getColumn("name"), false, null, null, false)
				));
		
		assertThat(creationDateColumn.getSize())
				.usingRecursiveComparison()
				.isEqualTo(Size.length(42));
	}

	@Test
	<T extends Table<T>> void extraTable() {
		FluentEntityMappingBuilder<Country, Integer> embeddableCountryMapping = entityBuilder(Country.class, int.class)
				.mapKey(Country::getVersion, IdentifierPolicy.databaseAutoIncrement())
				// Only this property will be collected because we'll ask for the mapping of properties that are on an extra table only
				// Table name is not taken into account here, it acts as a marker for EmbeddableMappingResolver
				.map(Country::setDescription).readonly().extraTable("This table name doesn't matter at this stage");
		
		T countryTable = (T) new Table("Extended_Country");
		PropertyMappingResolver<Country, T> testInstance = new PropertyMappingResolver<>(new ColumnBinderRegistry());
		
		ResolvedPropertyMapping<Country, T> actualResult = new ResolvedPropertyMapping<>(testInstance.resolve(embeddableCountryMapping.getConfiguration().getPropertiesMapping(), countryTable, ColumnNamingStrategy.DEFAULT), countryTable);
		Map<String, Table> extraTables = Iterables.map(actualResult.collectExtraTables(), Table::getName);
		
		assertThat(actualResult.getExtraTableMappings())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						// even if the mapping declares some other properties, since there are not declared on an
						// extra table, they are not mapped
						new ReadOnlyPropertyMapping<>(mutatorByMethodReference(Country::setDescription), extraTables.get("This table name doesn't matter at this stage").getColumn("description"), false, null, false)
				));
	}
	
	/**
	 * This test demonstrates that {@link PropertyMappingResolver} collects properties of an entity, which includes
	 * embeddable ones coming from an upper class, which is a different behavior than for an entity inheriting from another
	 * entity, see {@link #resolve_mapSuperClass_superClassIsEntity()}
	 */
	@Test
	<T extends Table<T>> void resolve_mapSuperClass_superClassIsEmbeddable() {
		FluentEmbeddableMappingBuilder<Realm> entityMappingBuilder = embeddableBuilder(Realm.class)
				.embed(Realm::getKing, embeddableBuilder(King.class)
						.map(King::getName).columnName("kingName")
				)
				.map(Country::setDescription).readonly()
				.mapSuperClass(embeddableBuilder(Country.class)
						.embed(Country::getTimestamp, embeddableBuilder(Timestamp.class)
								.map(Timestamp::getCreationDate).columnName("creation_date")
								.map(Timestamp::setModificationDate).setByConstructor()
						)
						.map(Country::setName).readonly());
		
		T countryTable = (T) new Table("Extended_Country");
		PropertyMappingResolver<Realm, T> testInstance = new PropertyMappingResolver<>(new ColumnBinderRegistry());
		
		ResolvedPropertyMapping<Realm, T> actualResult = new ResolvedPropertyMapping<>(testInstance.resolve(entityMappingBuilder.getConfiguration(), countryTable, ColumnNamingStrategy.DEFAULT), countryTable);
		
		List<ReadWritePropertyAccessPoint<Country, Timestamp>> embeddablePrefix = Arrays.asList(readWriteAccessPoint(Country::getTimestamp));
		assertThat(actualResult.getMappings())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						new PropertyMapping<>(new ReadWriteAccessorChain<>(Arrays.asList(readWriteAccessPoint(Realm::getKing)), readWriteAccessPoint(King::getName)), countryTable.getColumn("kingName"), false, null, null, false),
						new PropertyMapping<>(new ReadWriteAccessorChain<>(embeddablePrefix, readWriteAccessPoint(Timestamp::getCreationDate)), countryTable.getColumn("creation_date"), false, null, null, false),
						new PropertyMapping<>(new ReadWriteAccessorChain<>(embeddablePrefix, readWriteAccessPoint(Timestamp::setModificationDate)), countryTable.getColumn("modificationDate"), true, null, null, false),
						new ReadOnlyPropertyMapping<>(mutatorByMethodReference(Country::setDescription), countryTable.getColumn("description"), false, null, false),
						new ReadOnlyPropertyMapping<>(mutatorByMethodReference(Country::setName), countryTable.getColumn("name"), false, null, false)
				));
	}
	
	/**
	 * This test demonstrates that {@link PropertyMappingResolver} collects only properties of an entity and doesn't
	 * go up to a parent one (it will do for an embeddable one, see {@link #resolve_mapSuperClass_superClassIsEmbeddable()}
	 */
	@Test
	<T extends Table<T>> void resolve_mapSuperClass_superClassIsEntity() {
		Table extraTable2 = new Table("extraTable2");
		FluentEntityMappingBuilder<Country, Integer> superConfigurationProvider = entityBuilder(Country.class, int.class)
				.mapKey(Country::getVersion, IdentifierPolicy.databaseAutoIncrement()).columnName("myProperty")
				.embed(Country::getTimestamp, embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate).columnName("creation_date")
						.map(Timestamp::setModificationDate).setByConstructor()
				)
				.map(Country::setName).readonly().extraTable(extraTable2);
		
		Table extraTable1 = new Table("extraTable1");
		FluentEntityMappingBuilder<Realm, Integer> entityMappingBuilder = entityBuilder(Realm.class, int.class)
				.embed(Realm::getKing, embeddableBuilder(King.class)
						.map(King::getName).columnName("kingName")
				)
				// Only this property will be collected because we'll ask for the mapping of properties that are on an extra table only
				// Table name is not taken into account here, it acts as a marker for EmbeddableMappingResolver
				.map(Country::setDescription).readonly().extraTable(extraTable1)
				.mapSuperClass(superConfigurationProvider);
		
		T countryTable = (T) new Table("Country");
		PropertyMappingResolver<Realm, T> testInstance = new PropertyMappingResolver<>(new ColumnBinderRegistry());
		
		ResolvedPropertyMapping<Realm, T> actualResult1 = new ResolvedPropertyMapping<>(testInstance.resolve(entityMappingBuilder.getConfiguration().getPropertiesMapping(), countryTable, ColumnNamingStrategy.DEFAULT), countryTable);
		
		assertThat(actualResult1.getMappings())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						new PropertyMapping<>(new ReadWriteAccessorChain<>(Arrays.asList(readWriteAccessPoint(Realm::getKing)), readWriteAccessPoint(King::getName)), countryTable.getColumn("kingName"), false, null, null, false)
				));
		
		assertThat(actualResult1.getExtraTableMappings())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						// even if the mapping declares some other properties, since there are not declared on an
						// extra table, they are not mapped
						new ReadOnlyPropertyMapping<>(mutatorByMethodReference(Country::setDescription), extraTable1.getColumn("description"), false, null, false)
				));
		
		PropertyMappingResolver<Country, T> testInstance2 = new PropertyMappingResolver<>(new ColumnBinderRegistry());
		
		ResolvedPropertyMapping<Country, T> actualResult2 = new ResolvedPropertyMapping<>(testInstance2.resolve(superConfigurationProvider.getConfiguration().getPropertiesMapping(), countryTable, ColumnNamingStrategy.DEFAULT), countryTable);
		
		List<ReadWritePropertyAccessPoint<Country, Timestamp>> embeddablePrefix = Arrays.asList(readWriteAccessPoint(Country::getTimestamp));
		assertThat(actualResult2.getMappings())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						new PropertyMapping<>(new ReadWriteAccessorChain<>(embeddablePrefix, readWriteAccessPoint(Timestamp::getCreationDate)), countryTable.getColumn("creation_date"), false, null, null, false),
						new PropertyMapping<>(new ReadWriteAccessorChain<>(embeddablePrefix, readWriteAccessPoint(Timestamp::setModificationDate)), countryTable.getColumn("modificationDate"), true, null, null, false)
				));
		
		assertThat(actualResult2.getExtraTableMappings())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						// even if the mapping declares some other properties, since there are not declared on an
						// extra table, they are not mapped
						new ReadOnlyPropertyMapping<>(mutatorByMethodReference(Country::setName), extraTable2.getColumn("name"), false, null, false)
				));
	}
	
	
	public static class ResolvedPropertyMapping<C, T extends Table<T>> {
		
		private final Set<AbstractPropertyMapping<C, ?, T>> mappings = new KeepOrderSet<>();
		
		private final Set<AbstractPropertyMapping<C, ?, ?>> extraTableMappings = new KeepOrderSet<>();
		
		public ResolvedPropertyMapping(Set<AbstractPropertyMapping<C, ?, T>> mapping, T targetTable) {
			mapping.forEach(mappingPawn -> {
				if (mappingPawn.getColumn().getTable() == targetTable) {
					mappings.add(mappingPawn);
				} else {
					extraTableMappings.add(mappingPawn);
				}
			});
		}
		
		public Set<AbstractPropertyMapping<C, ?, T>> getMappings() {
			return mappings;
		}
		
		public <EXTRATABLE extends Table<EXTRATABLE>> Set<AbstractPropertyMapping<C, ?, EXTRATABLE>> getExtraTableMappings() {
			return (Set) extraTableMappings;
		}
		
		void addAllMapping(Collection<AbstractPropertyMapping<C, ?, T>> propertyMapping) {
			mappings.addAll(propertyMapping);
		}
		
		void addMapping(AbstractPropertyMapping<C, ?, T> propertyMapping) {
			mappings.add(propertyMapping);
		}
		
		void addAllExtraTableMapping(Collection<AbstractPropertyMapping<C, ?, T>> propertyMapping) {
			extraTableMappings.addAll(propertyMapping);
		}
		
		void addExtraTableMapping(AbstractPropertyMapping<C, ?, ?> propertyMapping) {
			extraTableMappings.add(propertyMapping);
		}
		
		public Set<Table> collectExtraTables() {
			return this.extraTableMappings.stream().map(Functions.chain(AbstractPropertyMapping::getColumn, Column::getTable))
					.collect(Collectors.toSet());
		}
	}
}
