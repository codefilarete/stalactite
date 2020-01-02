package org.gama.stalactite.persistence.engine;

import java.util.Collections;
import java.util.stream.Collectors;

import org.gama.lang.collection.Arrays;
import org.gama.lang.trace.ModifiableInt;
import org.gama.reflection.Accessors;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.Gender;
import org.gama.stalactite.persistence.engine.model.Person;
import org.gama.stalactite.persistence.engine.model.PersonWithGender;
import org.gama.stalactite.persistence.engine.model.State;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.Test;
import org.mockito.internal.stubbing.defaultanswers.ReturnsMocks;

import static org.gama.stalactite.persistence.engine.MappingEase.entityBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class EntityMappingBuilderTest {
	
	@Test
	void build_addsPrimaryKeyToTargetTable() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		
		// building mapping manually
		IReversibleAccessor<Person, Identifier<Long>> identifierAccessor = Accessors.propertyAccessor(Person.class, "id");
		IReversibleAccessor<Person, String> nameAccessor = Accessors.propertyAccessor(Person.class, "name");
		
		EmbeddableMappingConfiguration<Person> personPropertiesMapping = mock(EmbeddableMappingConfiguration.class);
		// declaring mapping
		when(personPropertiesMapping.getPropertiesMapping()).thenReturn(Arrays.asList(
				new EntityLinkageByColumnName<>(identifierAccessor, (Class<Identifier<Long>>) (Class) Identifier.class, "id"),
				new EntityLinkageByColumnName<>(nameAccessor, String.class, "name")
		));
		// preventing NullPointerException
		when(personPropertiesMapping.getInsets()).thenReturn(Collections.emptyList());
		when(personPropertiesMapping.getColumnNamingStrategy()).thenReturn(ColumnNamingStrategy.DEFAULT);
		
		EntityMappingConfiguration<Person, Identifier<Long>> configuration = mock(EntityMappingConfiguration.class);
		// declaring mapping
		when(configuration.getPropertiesMapping()).thenReturn(personPropertiesMapping);
		when(configuration.getIdentifierPolicy()).thenReturn(IdentifierPolicy.AFTER_INSERT);
		// preventing NullPointerException
		when(configuration.getEntityType()).thenReturn(Person.class);
		when(configuration.getTableNamingStrategy()).thenReturn(TableNamingStrategy.DEFAULT);
		when(configuration.getIdentifierAccessor()).thenReturn(identifierAccessor);
		when(configuration.getOneToOnes()).thenReturn(Collections.emptyList());
		when(configuration.getOneToManys()).thenReturn(Collections.emptyList());
		when(configuration.inheritanceIterable()).thenAnswer(CALLS_REAL_METHODS);
		
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, new ReturnsMocks());
		EntityMappingBuilder<Person, Identifier<Long>> testInstance = new EntityMappingBuilder<>(configuration, new MethodReferenceCapturer());
		
		Table dummyTable = new Table<>("dummyTable");
		testInstance.build(new PersistenceContext(connectionProviderMock, dialect), dummyTable);
		assertNotNull(dummyTable.getPrimaryKey());
	}
	
	@Test
	void addByColumn_columnIsNotInTargetClass_throwsException() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class,
				Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		
		Table<?> targetTable = new Table<>("person");
		Column<Table, String> unkownColumnInTargetTable = new Column<>(new Table<>("xx"), "aa", String.class);
		EntityMappingConfiguration<PersonWithGender, Identifier> configuration = entityBuilder(PersonWithGender.class, Identifier.class)
						.add(Person::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						.add(Person::getName, unkownColumnInTargetTable)
						.getConfiguration();
		
		EntityMappingBuilder<PersonWithGender, Identifier> testInstance = new EntityMappingBuilder<>(configuration, new MethodReferenceCapturer());
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				testInstance.build(new PersistenceContext(mock(ConnectionProvider.class), dialect), targetTable)
		);
		assertEquals("Column specified for mapping Person::getName is not in target table : column xx.aa is not in table person",
				thrownException.getMessage());
	}
	
	@Test
	void enumMappedByColumn_columnIsNotInTargetClass_throwsException() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		
		Table<?> targetTable = new Table<>("person");
		Column<Table, Gender> unkownColumnInTargetTable = new Column<>(new Table<>("xx"), "aa", Gender.class);
		EntityMappingConfiguration<PersonWithGender, Identifier> configuration = entityBuilder(PersonWithGender.class, Identifier.class)
				.add(Person::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Person::getName)
				.addEnum(PersonWithGender::getGender, unkownColumnInTargetTable).byOrdinal()
				.getConfiguration();
		
		EntityMappingBuilder<PersonWithGender, Identifier> testInstance = new EntityMappingBuilder<>(configuration, new MethodReferenceCapturer());
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				testInstance.build(new PersistenceContext(mock(ConnectionProvider.class), dialect), targetTable)
		);
		assertEquals("Column specified for mapping PersonWithGender::getGender is not in target table : column xx.aa is not in table person",
				thrownException.getMessage());
	}
	
	@Test
	void build_mustRegisterWholeEntityGraphForSelect_startingWithOneToOne() {
		HSQLDBDialect dialect = new HSQLDBDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		dialect.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
		
		PersistenceContext persistenceContext = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), dialect);
		
		// creating a complex graph
		IEntityPersister<Country, Identifier> persister = entityBuilder(Country.class, Identifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.addOneToOne(Country::getCapital,
						entityBuilder(City.class, Identifier.class)
						.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						.addOneToManySet(City::getPersons, entityBuilder(Person.class, Identifier.class)
							.add(Person::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
							.add(Person::getName))
						.cascading(RelationMode.ALL))
				.cascading(RelationMode.ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		City capital = new City(new PersistableIdentifier<>(42L));
		capital.setName("Paris");
		Person person1 = new Person(new PersistableIdentifier<>(666L));
		person1.setName("Guillaume");
		Person person2 = new Person(new PersistableIdentifier<>(667L));
		person2.setName("John");
		capital.setPersons(Arrays.asSet(person1, person2));
		
		Country country = new Country(new PersistableIdentifier<>(1L));
		country.setName("France");
		country.setCapital(capital);
		
		persister.persist(country);
		
		ModifiableInt citySelectListenerCounter = new ModifiableInt();
		persistenceContext.getPersister(City.class).addSelectListener(new SelectListener<City, Object>() {
			@Override
			public void beforeSelect(Iterable<Object> ids) {
				citySelectListenerCounter.increment();
			}
		});
		ModifiableInt personSelectListenerCounter = new ModifiableInt();
		persistenceContext.getPersister(Person.class).addSelectListener(new SelectListener<Person, Object>() {
			@Override
			public void beforeSelect(Iterable<Object> ids) {
				personSelectListenerCounter.increment();
			}
		});
		
		Country loadedCountry = persister.select(country.getId());

		assertEquals(Arrays.asHashSet("John", "Guillaume"), loadedCountry.getCapital().getPersons().stream().map(Person::getName).collect(Collectors.toSet()));
		assertEquals(1, citySelectListenerCounter.getValue());
		assertEquals(1, personSelectListenerCounter.getValue());
	}
	
	
	@Test
	void build_mustRegisterWholeEntityGraphForSelect_startingWithOneToMany() {
		HSQLDBDialect dialect = new HSQLDBDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		dialect.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
		
		PersistenceContext persistenceContext = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), dialect);
		
		// creating a complex graph
		IEntityPersister<State, Identifier> persister = entityBuilder(State.class, Identifier.class)
				.add(State::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(State::getName)
				.addOneToManySet(State::getCities, entityBuilder(City.class, Identifier.class)
						.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						.add(City::getName)
						.addOneToOne(City::getCountry, entityBuilder(Country.class, Identifier.class)
								.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
								.add(Country::getName))
						.cascading(RelationMode.ALL))
				.cascading(RelationMode.ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Country france = new Country(new PersistableIdentifier<>(1L));
		france.setName("France");
		
		City grenoble = new City(new PersistableIdentifier<>(42L));
		grenoble.setName("Grenoble");
		grenoble.setCountry(france);
		City latronche = new City(new PersistableIdentifier<>(43L));
		latronche.setName("La Tronche");
		latronche.setCountry(france);
		
		State state = new State(new PersistableIdentifier<>(1L));
		state.setName("Isere");
		state.setCountry(france);
		state.setCities(Arrays.asSet(grenoble, latronche));
		
		persister.persist(state);
		
		ModifiableInt citySelectListenerCounter = new ModifiableInt();
		persistenceContext.getPersister(City.class).addSelectListener(new SelectListener<City, Object>() {
			@Override
			public void beforeSelect(Iterable<Object> ids) {
				citySelectListenerCounter.increment();
			}
		});
		ModifiableInt countrySelectListenerCounter = new ModifiableInt();
		persistenceContext.getPersister(Country.class).addSelectListener(new SelectListener<Country, Object>() {
			@Override
			public void beforeSelect(Iterable<Object> ids) {
				countrySelectListenerCounter.increment();
			}
		});
		
		State loadedState = persister.select(state.getId());
		
		assertEquals(Arrays.asSet("Grenoble", "La Tronche"), loadedState.getCities().stream().map(City::getName).collect(Collectors.toSet()));
		assertEquals(Arrays.asSet("France", "France"), loadedState.getCities().stream().map(City::getCountry).map(Country::getName).collect(Collectors.toSet()));
		assertEquals(1, citySelectListenerCounter.getValue());
		assertEquals(1, countrySelectListenerCounter.getValue());
	}
}