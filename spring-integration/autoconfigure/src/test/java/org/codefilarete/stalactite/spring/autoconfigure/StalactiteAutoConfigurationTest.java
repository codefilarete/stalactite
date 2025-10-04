package org.codefilarete.stalactite.spring.autoconfigure;

import javax.sql.DataSource;

import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.dsl.MappingEase;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.autoconfigure.DummyStalactiteRepository.DummyData;
import org.codefilarete.stalactite.spring.autoconfigure.StalactiteAutoConfigurationTest.StalactiteAutoConfigurationTestConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.DialectResolver;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;

@SpringBootTest(classes = StalactiteAutoConfigurationTestConfiguration.class)
@SpringBootApplication(scanBasePackages = "org.codefilarete.stalactite.spring.autoconfigure")
class StalactiteAutoConfigurationTest {
	
	@Autowired
	private DataSource dataSource;
	
	@Autowired
	private RepublicRepository republicRepository;
	
	@Autowired
	private Dialect dialect;
	
	@Autowired
	private DialectResolver dialectResolver;
	
	@Test
	void stalactiteAutoConfiguration_isTriggered() {
		// Note that, in the absolute, it's unnecessary to check that fields are not null (injected) because the whole test fails if Spring Boot
		// didn't manage to inject them (for any reason)
		assertThat(republicRepository).isNotNull();
		
		// checking that we managed to customize the Dialect, this means that StalactiteAutoConfiguration made the right things
		assertThat(dataSource).isInstanceOf(HSQLDBInMemoryDataSource.class);
		assertThat(dialect.getColumnBinderRegistry().getBinder(Identifier.class)).isNotNull();
		assertThat(dialectResolver).isInstanceOf(ServiceLoaderDialectResolver.class);
	}
	
	/**
	 * Class that provides some elements to make {@link StalactiteAutoConfiguration} run correctly
	 * @author Guillaume Mary
	 */
	static class StalactiteAutoConfigurationTestConfiguration {
		
		/**
		 * The {@link DataSource} is a mandatory element to make {@link StalactiteAutoConfiguration} runs
		 * @return a dataSource
		 */
		@Bean
		public DataSource dataSource() {
			return new HSQLDBInMemoryDataSource();
		}
		
		/**
		 * Optional action.
		 * @return a customizer of Stalactite Dialect
		 */
		@Bean
		public DialectCustomizer dialectCustomizer() {
			return dialect -> {
				dialect.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
				dialect.getSqlTypeRegistry().put(Identifier.class, "int");
				
				dialect.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
				dialect.getSqlTypeRegistry().put(Color.class, "int");
			};
		}
		
		/**
		 * @param persistenceContext mandatory object to build an {@link EntityPersister}
		 * @return the expected {@link EntityPersister} by {@link DummyStalactiteRepository}
		 */
		@Bean
		public EntityPersister<DummyData, Long> dummyDataPersister(PersistenceContext persistenceContext) {
			return entityBuilder(DummyData.class, long.class)
					.mapKey(DummyData::getId, IdentifierPolicy.databaseAutoIncrement())
					.map(DummyData::getName)
					.build(persistenceContext);
		}
		
		/**
		 * @param persistenceContext mandatory object to build an {@link EntityPersister}
		 * @return the expected {@link EntityPersister} by {@link RepublicRepository}
		 */
		@Bean
		public EntityPersister<Republic, Identifier<Long>> countryPersister(PersistenceContext persistenceContext) {
			// Because this test class inherits from an abstract one that instantiates Republic entities (because it is shared with other
			// polymorphic test classes), we map Republic instead of Country, else, we get some exception because a persister can only persist
			// instance of its defined type
			return entityBuilder(Republic.class, LONG_TYPE)
					.mapKey(Republic::getId, IdentifierPolicy.<Country, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
					.map(Republic::getName)
					.map(Republic::getDescription)
					.map(Republic::isEuMember)
					.embed(Republic::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.mapOneToOne(Republic::getPresident, entityBuilder(Person.class, LONG_TYPE)
							.mapKey(Person::getId, IdentifierPolicy.<Person, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
							.map(Person::getName)
							.mapCollection(Person::getNicknames, String.class)
							.mapOneToOne(Person::getVehicle, entityBuilder(Vehicle.class, LONG_TYPE)
									.mapKey(Vehicle::getId, IdentifierPolicy.<Vehicle, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
									.map(Vehicle::getColor)))
					.build(persistenceContext);
		}
	}
}