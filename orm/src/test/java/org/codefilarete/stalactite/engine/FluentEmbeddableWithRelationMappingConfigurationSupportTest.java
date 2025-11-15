package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilder;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.device.Address;
import org.codefilarete.stalactite.engine.model.device.Device;
import org.codefilarete.stalactite.engine.model.device.Location;
import org.codefilarete.stalactite.engine.model.device.Review;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.MappingEase.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;

public class FluentEmbeddableWithRelationMappingConfigurationSupportTest {
	
	private final Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	public void initTest() {
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		persistenceContext = new PersistenceContext(dataSource, dialect);
	}
	
	@Nested
	class OneToOne_MappedSuperClass {
		
		@Test
		void foreignKeyIsCreated() {
			FluentEmbeddableMappingBuilder<Location> locationMappingBuilder = embeddableBuilder(Location.class)
					.mapOneToOne(Location::getCountry, entityBuilder(Country.class, Identifier.LONG_TYPE)
							.mapKey(Country::getId, ALREADY_ASSIGNED)
							.map(Country::getName).mandatory());
			
			FluentEntityMappingBuilder<Address, Identifier<Long>> addressMappingBuilder = entityBuilder(Address.class, Identifier.LONG_TYPE)
					.mapKey(Location::getId, ALREADY_ASSIGNED)
					.map(Address::getStreet).mandatory()
					.mapOneToOne(Address::getCity, entityBuilder(City.class, Identifier.LONG_TYPE)
							.mapKey(City::getId, ALREADY_ASSIGNED)
							.map(City::getName).mandatory())
					.mapSuperClass(locationMappingBuilder);
			
			ConfiguredPersister<Address, Identifier<Long>> addressPersister = (ConfiguredPersister) addressMappingBuilder.build(persistenceContext);
			
			// ensuring that the foreign key is present on table
			JdbcForeignKey expectedForeignKey1 = new JdbcForeignKey("FK_Address_cityId_City_id", "Address", "cityId", "City", "id");
			JdbcForeignKey expectedForeignKey2 = new JdbcForeignKey("FK_Address_countryId_Country_id", "Address", "countryId", "Country", "id");
			Comparator<JdbcForeignKey> comparing = Comparator.comparing(JdbcForeignKey::getSignature, Comparator.naturalOrder());
			assertThat((Set<? extends ForeignKey<?, ?, ?>>) addressPersister.getMapping().getTargetTable().getForeignKeys()).extracting(JdbcForeignKey::new)
					.usingElementComparator(comparing)
					.containsExactlyInAnyOrder(expectedForeignKey1, expectedForeignKey2);
		}
		
		@Test
		void crud() {
			FluentEntityMappingBuilder<Country, Identifier<Long>> countryConfiguration = entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getName).mandatory();
			
			FluentEmbeddableMappingBuilder<Location> locationMappingBuilder = embeddableBuilder(Location.class)
					.mapOneToOne(Location::getCountry, countryConfiguration)
					.cascading(RelationMode.ALL_ORPHAN_REMOVAL);
			
			FluentEntityMappingBuilder<Address, Identifier<Long>> addressMappingBuilder = entityBuilder(Address.class, Identifier.LONG_TYPE)
					.mapKey(Location::getId, ALREADY_ASSIGNED)
					.map(Address::getStreet).mandatory()
					.mapOneToOne(Address::getCity, entityBuilder(City.class, Identifier.LONG_TYPE)
							.mapKey(City::getId, ALREADY_ASSIGNED)
							.map(City::getName).mandatory())
					.mapSuperClass(locationMappingBuilder);
			
			EntityPersister<Country, Identifier<Long>> countryPersister = countryConfiguration.build(persistenceContext);
			ConfiguredPersister<Address, Identifier<Long>> addressPersister = (ConfiguredPersister) addressMappingBuilder.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Address address = new Address(42);
			address.setStreet("221B Baker Street");
			Country country = new Country(11);
			country.setName("France");
			address.setCountry(country);
			City city = new City(111);
			city.setName("Grenoble");
			address.setCity(city);
			
			addressPersister.insert(address);
			Address loadedAddress;
			loadedAddress = addressPersister.select(address.getId());
			assertThat(loadedAddress).usingRecursiveComparison().isEqualTo(address);
			
			Country country1 = new Country(22);
			country1.setName("France");
			address.setCountry(country1);
			
			addressPersister.update(address);
			
			loadedAddress = addressPersister.select(address.getId());
			assertThat(loadedAddress).usingRecursiveComparison().isEqualTo(address);
			
			addressPersister.delete(address);
			assertThat(addressPersister.select(address.getId())).isNull();
			
			assertThat(countryPersister.select(country1.getId())).isNull();
		}
	}
	
	@Nested
	class Embedded {

		@Test
		void foreignKeyIsCreated() {
			FluentEmbeddableMappingBuilder<Address> addressMappingBuilder = embeddableBuilder(Address.class)
					.map(Address::getStreet)
					.mapOneToOne(Address::getCity, entityBuilder(City.class, Identifier.LONG_TYPE)
							.mapKey(City::getId, ALREADY_ASSIGNED)
							.map(City::getName).mandatory());

			EntityPersister<Device, Identifier<Long>> devicePersister = entityBuilder(Device.class, Identifier.LONG_TYPE)
					.mapKey(Device::getId, ALREADY_ASSIGNED)
					.map(Device::getName)
					.embed(Device::setLocation, addressMappingBuilder)
					.build(persistenceContext);

			ConfiguredPersister<Device, Identifier<Long>> addressPersister = (ConfiguredPersister) devicePersister;

			// ensuring that the foreign key is present on table
			JdbcForeignKey expectedForeignKey1 = new JdbcForeignKey("FK_Device_location_cityId_City_id", "Device", "location_cityId", "City", "id");
			Comparator<JdbcForeignKey> comparing = Comparator.comparing(JdbcForeignKey::getSignature, Comparator.naturalOrder());
			assertThat((Set<? extends ForeignKey<?, ?, ?>>) addressPersister.getMapping().getTargetTable().getForeignKeys()).extracting(JdbcForeignKey::new)
					.usingElementComparator(comparing)
					.containsExactlyInAnyOrder(expectedForeignKey1);
		}
		
		@Test
		void crud() {
			FluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration = entityBuilder(City.class, Identifier.LONG_TYPE)
					.mapKey(City::getId, ALREADY_ASSIGNED)
					.map(City::getName).mandatory();
			
			EntityPersister<Device, Identifier<Long>> devicePersister = entityBuilder(Device.class, Identifier.LONG_TYPE)
					.mapKey(Device::getId, ALREADY_ASSIGNED)
					.map(Device::getName)
					.embed(Device::setLocation, embeddableBuilder(Address.class)
							.map(Address::getStreet)
							.mapOneToOne(Address::getCity, cityConfiguration).cascading(RelationMode.ALL_ORPHAN_REMOVAL))
					.build(persistenceContext);
			
			EntityPersister<City, Identifier<Long>> cityPersister = cityConfiguration.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			
			Device dummyDevice = new Device(13);
			dummyDevice.setName("UPS");
			Address address = new Address();
			address.setStreet("221B Baker Street");
			City city = new City(111);
			city.setName("France");
			address.setCity(city);
			dummyDevice.setLocation(address);
			
			devicePersister.insert(dummyDevice);
			Device loadedDevice;
			loadedDevice = devicePersister.select(dummyDevice.getId());
			assertThat(loadedDevice).usingRecursiveComparison().isEqualTo(dummyDevice);
			
			City city1 = new City(22);
			city1.setName("Spain");
			address.setCity(city1);
			
			devicePersister.update(dummyDevice);
			
			loadedDevice = devicePersister.select(dummyDevice.getId());
			assertThat(loadedDevice).usingRecursiveComparison().isEqualTo(dummyDevice);
			
			devicePersister.delete(dummyDevice);
			assertThat(devicePersister.select(dummyDevice.getId())).isNull();
			
			assertThat(cityPersister.select(city1.getId())).isNull();
		}
	}
	
	@Nested
	class OneToMany_MappedSuperClass {
		
		@Test
		void foreignKeyIsCreated() {
			FluentEntityMappingBuilder<Review, Identifier<Long>> reviewConfiguration = entityBuilder(Review.class, Identifier.LONG_TYPE)
					.mapKey(Review::getId, ALREADY_ASSIGNED)
					.map(Review::getRanking).mandatory();
			
			FluentEmbeddableMappingBuilder<Location> locationMappingBuilder = embeddableBuilder(Location.class)
					.mapOneToMany(Location::getReviews, reviewConfiguration).mappedBy(Review::getLocation);
			
			FluentEntityMappingBuilder<Address, Identifier<Long>> addressMappingBuilder = entityBuilder(Address.class, Identifier.LONG_TYPE)
					.mapKey(Location::getId, ALREADY_ASSIGNED)
					.map(Address::getStreet).mandatory()
					.mapOneToOne(Address::getCity, entityBuilder(City.class, Identifier.LONG_TYPE)
							.mapKey(City::getId, ALREADY_ASSIGNED)
							.map(City::getName).mandatory())
					.mapSuperClass(locationMappingBuilder);
			
			addressMappingBuilder.build(persistenceContext);
			
			Map<String, Table<?>> tablePerName = Iterables.map(DDLDeployer.collectTables(persistenceContext), Table::getName);
			
			// ensuring that the foreign key is present on table
			JdbcForeignKey expectedForeignKey1 = new JdbcForeignKey("FK_Address_cityId_City_id", "Address", "cityId", "City", "id");
			Comparator<JdbcForeignKey> comparing = Comparator.comparing(JdbcForeignKey::getSignature, Comparator.naturalOrder());
			assertThat((Set<? extends ForeignKey<?, ?, ?>>) tablePerName.get("Address").getForeignKeys()).extracting(JdbcForeignKey::new)
					.usingElementComparator(comparing)
					.containsExactlyInAnyOrder(expectedForeignKey1);
			
			JdbcForeignKey expectedForeignKey2 = new JdbcForeignKey("FK_Review_locationId_Address_id", "Review", "locationId", "Address", "id");
			assertThat((Set<? extends ForeignKey<?, ?, ?>>) tablePerName.get("Review").getForeignKeys()).extracting(JdbcForeignKey::new)
					.usingElementComparator(comparing)
					.containsExactlyInAnyOrder(expectedForeignKey2);
		}
		
		@Test
		void crud() {
			FluentEntityMappingBuilder<Review, Identifier<Long>> reviewConfiguration = entityBuilder(Review.class, Identifier.LONG_TYPE)
					.mapKey(Review::getId, ALREADY_ASSIGNED)
					.map(Review::getRanking).mandatory();
			
			FluentEmbeddableMappingBuilder<Location> locationMappingBuilder = embeddableBuilder(Location.class)
					.mapOneToMany(Location::getReviews, reviewConfiguration)
					.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mappedBy(Review::getLocation);
			
			FluentEntityMappingBuilder<Address, Identifier<Long>> addressMappingBuilder = entityBuilder(Address.class, Identifier.LONG_TYPE)
					.mapKey(Location::getId, ALREADY_ASSIGNED)
					.map(Address::getStreet).mandatory()
					.mapOneToOne(Address::getCity, entityBuilder(City.class, Identifier.LONG_TYPE)
							.mapKey(City::getId, ALREADY_ASSIGNED)
							.map(City::getName).mandatory())
					.mapSuperClass(locationMappingBuilder);
			
			EntityPersister<Address, Identifier<Long>> addressPersister = addressMappingBuilder.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			// Strange behavior trick : in debug mode (and only in debug mode), if this the reviewPersister is build before that DDDeployer is run
			// (the below line is pushed above), then, because DDLDeployer finds the Review Table of reviewPersister instead of the one of address,
			// it lacks the reverse foreign key "locationId" (it misses it because the review configuration is "alone"). Then, while inserting an
			// address, the insert order contains the locationId but not the schema, therefore insertion fails. The trick then is to ask the schema
			// deployment before building the reviewPersister.
			ConfiguredPersister<Review, Identifier<Long>> reviewPersister = (ConfiguredPersister) reviewConfiguration.build(persistenceContext);
			
			Address address = new Address(42);
			address.setStreet("221B Baker Street");
			City city = new City(111);
			city.setName("Grenoble");
			address.setCity(city);
			address.setReviews(Arrays.asHashSet(new Review(1), new Review(2), new Review(3)));
			
			addressPersister.insert(address);
			Address loadedAddress;
			loadedAddress = addressPersister.select(address.getId());
			// AssertJ badly handle bi-directionality, so we exclude it from the comparison
			assertThat(loadedAddress).usingRecursiveComparison().ignoringFields("reviews.location").isEqualTo(address);
			assertThat(loadedAddress.getReviews().stream().map(Review::getLocation).collect(Collectors.toSet())).containsOnly(loadedAddress);
			
			address.getReviews().add(new Review(4));
			
			addressPersister.update(address);
			
			loadedAddress = addressPersister.select(address.getId());
			// AssertJ badly handle bi-directionality, so we exclude it from the comparison
			assertThat(loadedAddress).usingRecursiveComparison().ignoringFields("reviews.location").isEqualTo(address);
			assertThat(loadedAddress.getReviews().stream().map(Review::getLocation).collect(Collectors.toSet())).containsOnly(loadedAddress);
			
			// we ensure that orphan removal is respected
			addressPersister.delete(address);
			assertThat(reviewPersister.select(address.getReviews().stream().map(Review::getId).collect(Collectors.toSet()))).isEmpty();
		}
	}
}
