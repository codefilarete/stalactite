package org.codefilarete.stalactite.engine.configurer.resolver.elementcollection;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import javax.sql.DataSource;

import org.codefilarete.stalactite.dsl.FluentMappings;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PartialRepresentation;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver;
import org.codefilarete.stalactite.engine.idprovider.LongProvider;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.id.AbstractIdentifier;
import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.hsqldb.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.function.Functions;
import org.codefilarete.trace.ObjectPrinterBuilder;
import org.codefilarete.trace.ObjectPrinterBuilder.ObjectPrinter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.FluentMappings.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.ALL;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;

class ElementCollectionResolverTest {
	
	private static final Class<Identifier<UUID>> UUID_TYPE = (Class) Identifier.class;
	
	private final Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private FluentEntityMappingBuilder<Person, Identifier<Long>> personConfiguration;
	private FluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration;
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	public void initTest() {
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		dialect.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
		dialect.getSqlTypeRegistry().put(Color.class, "int");
		persistenceContext = new PersistenceContext(dataSource, dialect);
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personMappingBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.map(Person::getName);
		personConfiguration = personMappingBuilder;
		
		FluentEntityMappingBuilder<City, Identifier<Long>> cityMappingBuilder = entityBuilder(City.class, LONG_TYPE)
				.mapKey(City::getId, ALREADY_ASSIGNED)
				.map(City::getName);
		cityConfiguration = cityMappingBuilder;
	}
	
	@Nested
	class Complex_CRUD {
		
		@Test
		void crudEnum() {
			Table totoTable = new Table("Toto");
			Column idColumn = totoTable.addColumn("id", UUID_TYPE);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			FluentEntityMappingBuilder<Toto, Identifier<UUID>> totoPersisterConfiguration = entityBuilder(Toto.class, UUID_TYPE)
					.onTable(totoTable)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.mapCollection(Toto::getPossibleStates, State.class);
			
			AggregateResolver testInstance = new AggregateResolver(persistenceContext);
			EntityPersister<Toto, Identifier<UUID>> personPersister = testInstance.resolve(totoPersisterConfiguration.getConfiguration());
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto person = new Toto();
			person.setName("toto");
			person.getPossibleStates().add(State.DONE);
			person.getPossibleStates().add(State.IN_PROGRESS);
			
			personPersister.insert(person);
			
			Toto loadedPerson = personPersister.select(person.getId());
			assertThat(loadedPerson.getPossibleStates()).containsExactlyInAnyOrder(State.DONE, State.IN_PROGRESS);
		}
		
		@Test
		void crudComplexType() {
			Table totoTable = new Table("Toto");
			Column idColumn = totoTable.addColumn("id", UUID_TYPE);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			FluentEntityMappingBuilder<Toto, Identifier<UUID>> totoPersisterConfiguration = entityBuilder(Toto.class, UUID_TYPE)
					.onTable(totoTable)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.mapCollection(Toto::getTimes, Timestamp.class, embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate));
			
			AggregateResolver testInstance = new AggregateResolver(persistenceContext);
			EntityPersister<Toto, Identifier<UUID>> personPersister = testInstance.resolve(totoPersisterConfiguration.getConfiguration());
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto person = new Toto();
			person.setName("toto");
			Timestamp timestamp1 = new Timestamp(LocalDateTime.now().plusDays(1), LocalDateTime.now().plusDays(1));
			Timestamp timestamp2 = new Timestamp(LocalDateTime.now().plusDays(2), LocalDateTime.now().plusDays(2));
			person.setTimes(Arrays.asSet(timestamp1, timestamp2));
			
			personPersister.insert(person);
			
			Toto loadedPerson = personPersister.select(person.getId());
			assertThat(loadedPerson.getTimes()).usingRecursiveFieldByFieldElementComparator().containsExactlyInAnyOrder(timestamp1, timestamp2);
		}
		
		@Test
		void crudComplexType_ordered() {
			Table totoTable = new Table("Toto");
			Column idColumn = totoTable.addColumn("id", UUID_TYPE);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
					.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapCollection(Person::getNicknames, String.class);
			
			FluentEntityMappingBuilder<City, Identifier<Long>> cityBuilder =
					entityBuilder(City.class, Identifier.LONG_TYPE)
							.mapKey(City::getId, ALREADY_ASSIGNED)
							.map(City::getName)
							.mapOneToMany(City::getPersons, personBuilder);
			
			FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterBuilder = FluentMappings.entityBuilder(Country.class, LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, cityBuilder).mappedBy(City::setCountry).cascading(ALL)
					;
			
			
			AggregateResolver testInstance = new AggregateResolver(persistenceContext);
			EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterBuilder.getConfiguration());
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			LongProvider countryIdProvider = new LongProvider();
			Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
			dummyCountry.setName("France");
			dummyCountry.setDescription("Smelly cheese !");
			LongProvider cityIdProvider = new LongProvider();
			City paris = new City(cityIdProvider.giveNewIdentifier());
			paris.setName("Paris");
			dummyCountry.addCity(paris);
			
			LongProvider personIdProvider = new LongProvider();
			Person someone1 = new Person(personIdProvider.giveNewIdentifier());
			someone1.setName("dummy person 1");
			paris.setPersons(Arrays.asHashSet(someone1));
			someone1.initNicknames();
			someone1.addNickname("tonton");
			someone1.addNickname("tintin");
			
			City lyon = new City(cityIdProvider.giveNewIdentifier());
			lyon.setName("Lyon");
			dummyCountry.addCity(lyon);
			
			Person someone2 = new Person(personIdProvider.giveNewIdentifier());
			someone2.setName("dummy person 2");
			lyon.setPersons(Arrays.asHashSet(someone2));
			
			countryPersister.insert(dummyCountry);
			
			Country persistedCountry = countryPersister.select(dummyCountry.getId());
			
			ObjectPrinter<Person> personPrinter = new ObjectPrinterBuilder<Person>()
					.addProperty(Person::getId)
					.addProperty(Person::getName)
					.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
					.build();
			ObjectPrinter<City> cityPrinter = new ObjectPrinterBuilder<City>()
					.addProperty(City::getId)
					.addProperty(City::getName)
					.addProperty(City::getState)
					.addProperty(City::getPersons, Person.class)
					.withPrinter(Person.class, personPrinter::toString)
					.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
					.build();
			ObjectPrinter<Country> countryPrinter = new ObjectPrinterBuilder<Country>()
					.addProperty(Country::getId)
					.addProperty(Country::getName)
					.addProperty(Country::getCities, City.class)
					.withPrinter(City.class, cityPrinter::toString)
					.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
					.build();
			
			assertThat(persistedCountry)
					.usingComparator(Comparator.comparing(countryPrinter::toString))
					.withRepresentation(new PartialRepresentation<>(Country.class, countryPrinter))
					.isEqualTo(dummyCountry);
		}
	}
	
	protected abstract static class AbstractToto implements Identified<UUID> {
		
		protected final Identifier<UUID> id;
		
		private String prop1;
		
		public AbstractToto() {
			id = new PersistableIdentifier<>(UUID.randomUUID());
		}
		
		public AbstractToto(PersistedIdentifier<UUID> id) {
			this.id = id;
		}
		
		@Override
		public Identifier<UUID> getId() {
			return id;
		}
		
		public String getProp1() {
			return prop1;
		}
		
		public void setProp1(String prop1) {
			this.prop1 = prop1;
		}
	}
	
	protected static class Toto extends AbstractToto {
		
		public static Toto newInstance() {
			Toto newInstance = new Toto();
			newInstance.firstName = "set by static factory";
			return newInstance;
		}
		
		private Identifier<UUID> identifier;
		
		private UUID uid;
		
		private String name;
		
		private String firstName;
		
		private Timestamp timestamp;
		
		private Set<State> possibleStates = new HashSet<>();
		
		private Set<Timestamp> times;
		
		private boolean setIdWasCalled;
		private boolean setUuidWasCalled;
		private boolean constructorWithIdWasCalled;
		private boolean constructorWith2ArgsWasCalled;
		
		private String fieldWithoutAccessor;
		
		public Toto() {
			super();
		}
		
		public Toto(PersistedIdentifier<UUID> id) {
			super(id);
			this.constructorWithIdWasCalled = true;
		}
		
		public Toto(UUID id) {
			this(new PersistedIdentifier(id));
			this.uid = id;
		}
		
		public Toto(PersistedIdentifier<UUID> id, String name) {
			super(id);
			this.name = name + " by constructor";
			this.constructorWith2ArgsWasCalled = true;
		}
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		public String getFirstName() {
			return firstName;
		}
		
		public void setFirstName(String firstName) {
			this.firstName = firstName;
		}
		
		public Identifier<UUID> getIdentifier() {
			return identifier;
		}
		
		public void setIdentifier(Identifier<UUID> id) {
			this.identifier = id;
		}
		
		public UUID getUUID() {
			return uid;
		}
		
		public void setUUID(UUID uuid) {
			// this method is a lure for default ReversibleAccessor mechanism because it matches getter by its name but does nothing special about id
			setUuidWasCalled = true;
		}
		
		public boolean isSetUuidWasCalled() {
			return setUuidWasCalled;
		}
		
		public Long getNoMatchingField() {
			return null;
		}
		
		public void setNoMatchingField(Long s) {
		}
		
		public long getNoMatchingFieldPrimitive() {
			return 0;
		}
		
		public void setNoMatchingFieldPrimitive(long s) {
		}
		
		@Override
		public Identifier<UUID> getId() {
			return id;
		}
		
		public void setId(Identifier<UUID> id) {
			// this method is a lure for default ReversibleAccessor mechanism because it matches getter by its name but does nothing special about id
			setIdWasCalled = true;
		}
		
		public boolean isSetIdWasCalled() {
			return setIdWasCalled;
		}
		
		public boolean isConstructorWithIdWasCalled() {
			return constructorWithIdWasCalled;
		}
		
		public boolean isConstructorWith2ArgsWasCalled() {
			return constructorWith2ArgsWasCalled;
		}
		
		public Timestamp getTimestamp() {
			return timestamp;
		}
		
		public void setTimestamp(Timestamp timestamp) {
			this.timestamp = timestamp;
		}
		
		public Set<State> getPossibleStates() {
			return possibleStates;
		}
		
		public void setPossibleStates(Set<State> possibleStates) {
			this.possibleStates = possibleStates;
		}
		
		public Set<Timestamp> getTimes() {
			return times;
		}
		
		public void setTimes(Set<Timestamp> times) {
			this.times = times;
		}
	}
	
	enum State {
		TODO,
		IN_PROGRESS,
		DONE
	}
	
	static class TimestampWithLocale extends Timestamp {
		
		private Locale locale;
		
		TimestampWithLocale() {
			this(null, null, null);
		}
		
		public TimestampWithLocale(Date creationDate, Date modificationDate, Locale locale) {
			super(creationDate, modificationDate);
			this.locale = locale;
		}
		
		public Locale getLocale() {
			return locale;
		}
		
		public TimestampWithLocale setLocale(Locale locale) {
			this.locale = locale;
			return this;
		}
	}
	
}
