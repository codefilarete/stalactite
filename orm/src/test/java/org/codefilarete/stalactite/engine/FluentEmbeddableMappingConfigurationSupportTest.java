package org.codefilarete.stalactite.engine;

import java.util.Map;

import org.codefilarete.stalactite.dsl.MappingEase;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilder;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.configurer.embeddable.FluentEmbeddableMappingConfigurationSupport;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.PersonWithGender;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.device.Device;
import org.codefilarete.stalactite.engine.model.device.Location;
import org.codefilarete.stalactite.engine.model.device.Review;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.hsqldb.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Iterables;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.MappingEase.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;

/**
 * @author Guillaume Mary
 */
class FluentEmbeddableMappingConfigurationSupportTest {
	/**
	 * Test to check that the API returns right Object which means:
	 * - interfaces are well written to return right types, so one can chain others methods
	 * - at runtime instance of the right type is also returned
	 * (avoid "java.lang.ClassCastException: com.sun.proxy.$Proxy10 cannot be cast to org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilder")
	 * <p>
	 * As many as possible combinations of method chaining should be done here, but since all combinations seem impossible, this test must be
	 * considered as a best effort, and any regression found in user code should be added here
	 */
	
	@Test
	void apiUsage() {
		try {
			MappingEase.embeddableBuilder(Country.class)
					.map(Country::getName)
					.embed(Country::getPresident, MappingEase.embeddableBuilder(Person.class)
							.map(Person::getName)
							.embed(Person::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
									.map(Timestamp::getCreationDate)
									.map(Timestamp::getModificationDate).unique()))
					.overrideName(Person::getId, "personId")
					.overrideName(Person::getName, "personName")
					
					.embed(Country::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.map(Country::getId)
					.map(Country::setDescription).columnName("zxx").fieldName("tutu")
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class));
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			MappingEase.embeddableBuilder(Country.class)
					.map(Country::getName)
					.embed(Country::getPresident, MappingEase.embeddableBuilder(Person.class)
							.map(Person::getName)
							.embed(Person::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
									.map(Timestamp::getCreationDate).mandatory()
									.map(Timestamp::getModificationDate)))
					.embed(Country::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.map(Country::getId).columnName("zz")
					.mapSuperClass(MappingEase.embeddableBuilder(Object.class))
					.map(Country::getDescription).columnName("xx");
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			MappingEase.embeddableBuilder(Country.class)
					.map(Country::getName)
					.map(Country::getId).columnName("zz")
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class))
					// embed with setter
					.embed(Country::setPresident, MappingEase.embeddableBuilder(Person.class)
							.map(Person::getName)// inner embed with setter
							.embed(Person::setTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
									.map(Timestamp::getCreationDate)
									.map(Timestamp::getModificationDate)))
					// embed with setter
					.embed(Country::setTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.map(Country::getDescription).columnName("xx")
					.map(Country::getDummyProperty).columnName("dd");
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			EmbeddableMappingConfigurationProvider<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
					.map(Person::getName).unique();
			
			MappingEase.embeddableBuilder(Country.class)
					.map(Country::getName)
					.map(Country::getId).columnName("zz")
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class))
					// embed with setter
					.embed(Country::getPresident, personMappingBuilder);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			EmbeddableMappingConfigurationProvider<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
					.map(Person::getName);
			
			MappingEase.embeddableBuilder(Country.class)
					.map(Country::getName)
					.map(Country::getId).columnName("zz")
					.embed(Country::getPresident, personMappingBuilder)
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class))
					// reusing embeddable ...
					.embed(Country::getPresident, personMappingBuilder)
					// with getter override
					.overrideName(Person::getName, "toto")
					// with setter override
					.overrideName(Person::setName, "tata");
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			MappingEase.embeddableBuilder(PersonWithGender.class)
					.map(Person::getName)
					.map("name")
					.mapEnum(PersonWithGender::getGender).byOrdinal()
					.mapEnum("gender").byOrdinal()
					.embed(Person::setTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.overrideName(Timestamp::getCreationDate, "myDate")
					.mapEnum(PersonWithGender::getGender).columnName("MM").byOrdinal()
					.mapEnum(PersonWithGender::getGender).columnName("MM").mandatory()
					.map(PersonWithGender::getId).columnName("zz")
					.mapEnum(PersonWithGender::setGender).byName()
					.embed(Person::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.mapEnum(PersonWithGender::setGender).columnName("MM").byName();
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		try {
			MappingEase.embeddableBuilder(PersonWithGender.class)
					.map(Person::getName)
					.mapEnum(PersonWithGender::getGender).byOrdinal()
					.embed(Person::setTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.overrideName(Timestamp::getCreationDate, "myDate")
					.mapOneToOne(Person::getCountry, MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
							.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(Country::getName)
							.columnName("zz"))
					.mapEnum(PersonWithGender::setGender).byName()
					.embed(Person::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.mapEnum(PersonWithGender::setGender).columnName("MM").byName();
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
	}
	
	@Test
	void nullable() {
		Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		
		
		FluentEntityMappingBuilder<Review, Identifier<Long>> reviewConfiguration = entityBuilder(Review.class, Identifier.LONG_TYPE)
				.mapKey(Review::getId, ALREADY_ASSIGNED)
				.map(Review::getRanking).nullable();
		
		FluentEmbeddableMappingBuilder<Location> locationMappingBuilder = embeddableBuilder(Location.class)
				.mapOneToMany(Location::getReviews, reviewConfiguration);
		
		entityBuilder(Device.class, Identifier.LONG_TYPE)
				.mapKey(Device::getId, ALREADY_ASSIGNED)
				.map(Device::getName)
				.embed(Device::setLocation, locationMappingBuilder)
				.build(persistenceContext);
		
		Map<String, Table<?>> tablePerName = Iterables.map(DDLDeployer.collectTables(persistenceContext), Table::getName);
		Table<?> deviceTable = tablePerName.get("Review");
		assertThat(deviceTable.getColumn("ranking").isNullable()).isTrue();
	}
	
	
	@Test
	void mandatory() {
		Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		
		
		FluentEntityMappingBuilder<Review, Identifier<Long>> reviewConfiguration = entityBuilder(Review.class, Identifier.LONG_TYPE)
				.mapKey(Review::getId, ALREADY_ASSIGNED)
				.map(Review::getDate).mandatory();
		
		FluentEmbeddableMappingBuilder<Location> locationMappingBuilder = embeddableBuilder(Location.class)
				.mapOneToMany(Location::getReviews, reviewConfiguration);
		
		entityBuilder(Device.class, Identifier.LONG_TYPE)
				.mapKey(Device::getId, ALREADY_ASSIGNED)
				.map(Device::getName)
				.embed(Device::setLocation, locationMappingBuilder)
				.build(persistenceContext);
		
		Map<String, Table<?>> tablePerName = Iterables.map(DDLDeployer.collectTables(persistenceContext), Table::getName);
		Table<?> deviceTable = tablePerName.get("Review");
		assertThat(deviceTable.getColumn("date").isNullable()).isFalse();
	}
}
