package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.util.Comparator;
import java.util.Set;

import org.codefilarete.stalactite.dsl.MappingEase;
import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilder;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.device.Address;
import org.codefilarete.stalactite.engine.model.device.Location;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
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
	class MappedSuperClass {
		
		@Test
		void foreignKeyIsCreated() {
			FluentEmbeddableMappingBuilder<Location> locationMappingBuilder = MappingEase.embeddableBuilder(Location.class)
					.mapOneToOne(Location::getCountry, MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
							.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(Country::getName).mandatory());
			
			FluentEntityMappingBuilder<Address, Identifier<Long>> addressMappingBuilder = MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
					.mapKey(Location::getId, ALREADY_ASSIGNED)
					.map(Address::getStreet).mandatory()
					.mapOneToOne(Address::getCity, MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
							.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
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
			FluentEntityMappingBuilder<Country, Identifier<Long>> countryConfiguration = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getName).mandatory();
			
			FluentEmbeddableMappingBuilder<Location> locationMappingBuilder = MappingEase.embeddableBuilder(Location.class)
					.mapOneToOne(Location::getCountry, countryConfiguration)
					.cascading(RelationMode.ALL_ORPHAN_REMOVAL);
			
			FluentEntityMappingBuilder<Address, Identifier<Long>> addressMappingBuilder = MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
					.mapKey(Location::getId, ALREADY_ASSIGNED)
					.map(Address::getStreet).mandatory()
					.mapOneToOne(Address::getCity, MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
							.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
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
}
