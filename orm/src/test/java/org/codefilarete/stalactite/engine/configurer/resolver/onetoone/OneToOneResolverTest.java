package org.codefilarete.stalactite.engine.configurer.resolver.onetoone;

import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;

import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver;
import org.codefilarete.stalactite.engine.idprovider.LongProvider;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.ALL;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;

class OneToOneResolverTest {
	
	@Test
	void multiple_oneToOne() throws SQLException {
		
		DataSource inMemoryDataSource = new HSQLDBInMemoryDataSource();
		PersistenceContext persistenceContext = new PersistenceContext(inMemoryDataSource);
		
		persistenceContext.getDialect().getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		persistenceContext.getDialect().getSqlTypeRegistry().put(Identifier.class, "int");
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personMappingBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.map(Person::getName);
		
		FluentEntityMappingBuilder<City, Identifier<Long>> cityMappingBuilder = entityBuilder(City.class, Identifier.LONG_TYPE)
				.mapKey(City::getId, ALREADY_ASSIGNED)
				.map(City::getName);
		
		FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, ALREADY_ASSIGNED)
				.map(Country::getDescription)
				.mapOneToOne(Country::getPresident, personMappingBuilder).cascading(ALL)
				.mapOneToOne(Country::getCapital, cityMappingBuilder).cascading(ALL);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("French president");
		dummyCountry.setPresident(person);
		
		City capital = new City(new LongProvider().giveNewIdentifier());
		capital.setName("Paris");
		dummyCountry.setCapital(capital);
		
		// testing insert cascade
		countryPersister.insert(dummyCountry);
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		assertThat(persistedCountry.getId()).isEqualTo(new PersistedIdentifier<>(0L));
		assertThat(persistedCountry.getPresident().getName()).isEqualTo("French president");
		assertThat(persistedCountry.getCapital().getName()).isEqualTo("Paris");
		assertThat(persistedCountry.getPresident().getId().isPersisted()).isTrue();
		assertThat(persistedCountry.getCapital().getId().isPersisted()).isTrue();
		
		// testing insert cascade with another Country reusing OneToOne entities
		Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry2.setName("France 2");
		dummyCountry2.setPresident(person);
		dummyCountry2.setCapital(capital);
		countryPersister.insert(dummyCountry2);
		// database must be up to date
		Country persistedCountry2 = countryPersister.select(dummyCountry2.getId());
		assertThat(persistedCountry2.getId()).isEqualTo(new PersistedIdentifier<>(1L));
		assertThat(persistedCountry2.getPresident().getName()).isEqualTo("French president");
		assertThat(persistedCountry2.getPresident().getId().getDelegate()).isEqualTo(persistedCountry.getPresident().getId().getDelegate());
		assertThat(persistedCountry2.getCapital().getId().getDelegate()).isEqualTo(persistedCountry.getCapital().getId().getDelegate());
		assertThat(persistedCountry2.getPresident()).isNotSameAs(persistedCountry.getPresident());
		assertThat(persistedCountry2.getCapital()).isNotSameAs(persistedCountry.getCapital());
		
		// testing update cascade
		persistedCountry2.getPresident().setName("French president renamed");
		persistedCountry2.getCapital().setName("Paris renamed");
		countryPersister.update(persistedCountry2, dummyCountry2, true);
		// database must be up to date
		ResultSet resultSet;
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select name from Person");
		resultSet.next();
		assertThat(resultSet.getString("name")).isEqualTo("French president renamed");
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select name from City");
		resultSet.next();
		assertThat(resultSet.getString("name")).isEqualTo("Paris renamed");
		assertThat(resultSet.next()).isFalse();
		
		// testing delete cascade
		// but we have to remove first the other country that points to the same president, else will get a constraint violation
		assertThat(persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate(
				"update Country set presidentId = null, capitalId = null where id = " + dummyCountry2.getId().getDelegate())).isEqualTo(1);
		countryPersister.delete(persistedCountry);
		// database must be up to date
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from Country where id = " + persistedCountry.getId().getDelegate());
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from Person where id = " + persistedCountry.getPresident().getId().getDelegate());
		assertThat(resultSet.next()).isTrue();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from City where id = " + persistedCountry.getCapital().getId().getDelegate());
		assertThat(resultSet.next()).isTrue();
	}
	
}
