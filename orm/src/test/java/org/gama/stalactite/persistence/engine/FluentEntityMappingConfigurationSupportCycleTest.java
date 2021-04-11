package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.gama.lang.collection.Arrays;
import org.gama.lang.function.Hanger.Holder;
import org.gama.lang.test.Assertions;
import org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.Query;
import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.gama.stalactite.persistence.id.StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * @author Guillaume Mary
 */
public class FluentEntityMappingConfigurationSupportCycleTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	
	@BeforeAll
	public static void initAllTests() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
	}
	
	@Nested
	public class OneToOne {
		
		private PersistenceContext persistenceContext;
		
		
		@BeforeEach
		public void initTest() {
			persistenceContext = new PersistenceContext(new JdbcConnectionProvider(dataSource), DIALECT);
			
		}
		
		@Test
		void crud_cycleWithIntermediary_ownedBySource() {
			Holder<IFluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.add(Person::getId).identifier(ALREADY_ASSIGNED)
					.add(Person::getName)
					.addOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, Identifier.LONG_TYPE)
							.add(House::getId).identifier(ALREADY_ASSIGNED)
							.addOneToOne(House::getAddress, MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
									.add(Address::getId).identifier(ALREADY_ASSIGNED)
									.add(Address::getLocation))
							.addOneToOne(House::getGardener, () -> personMappingConfiguration.get().getConfiguration())
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
			);
			
			IEntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person johnDo = new Person(42);
			johnDo.setName("John Do");
			House house = new House(123);
			house.setAddress(new Address(456).setLocation("Somewhere in the world"));
			johnDo.setHouse(house);
			Person myGardener = new Person(888);
			myGardener.setName("Poppy");
			house.setGardener(myGardener);
			
			personPersister.insert(johnDo);
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(johnDo.getHouse(), loadedPerson.getHouse());
			assertEquals(johnDo.getHouse().getGardener(), loadedPerson.getHouse().getGardener());
			
			Person newGardener = new Person(999);
			newGardener.setName("Dandelion");
			johnDo.getHouse().setGardener(newGardener);
			personPersister.update(johnDo, loadedPerson, true);
			loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(johnDo.getHouse(), loadedPerson.getHouse());
			assertEquals(999, loadedPerson.getHouse().getGardener().getId().getSurrogate());
			assertEquals("Dandelion", loadedPerson.getHouse().getGardener().getName());
			
			personPersister.delete(johnDo);
			List<Long> allPersons = persistenceContext.newQuery("select id from Person", Long.class)
					.mapKey(Long::new, "id", long.class)
					.execute();
			assertEquals(Collections.emptyList(), allPersons);
		}
		
		@Test
		void crud_cycleWithIntermediary_ownedByTarget() {
			Table personTable = new Table("Person");
			Column<Table, Identifier<Long>> reverseGardenerId = personTable.addColumn("reverseGardenerId", Identifier.LONG_TYPE);
			
			Holder<IFluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.add(Person::getId).identifier(ALREADY_ASSIGNED)
					.add(Person::getName)
					.addOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, Identifier.LONG_TYPE)
							.add(House::getId).identifier(ALREADY_ASSIGNED)
							.addOneToOne(House::getAddress, MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
									.add(Address::getId).identifier(ALREADY_ASSIGNED)
									.add(Address::getLocation))
							.addOneToOne(House::getGardener, () -> personMappingConfiguration.get().getConfiguration()).mappedBy(reverseGardenerId)
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
			);
			
			IEntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext, personTable);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person johnDo = new Person(42);
			johnDo.setName("John Do");
			House house = new House(123);
			house.setAddress(new Address(456).setLocation("Somewhere in the world"));
			johnDo.setHouse(house);
			Person myGardener = new Person(888);
			myGardener.setName("Poppy");
			house.setGardener(myGardener);
			
			personPersister.insert(johnDo);
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(johnDo.getHouse(), loadedPerson.getHouse());
			assertEquals(johnDo.getHouse().getGardener(), loadedPerson.getHouse().getGardener());
			
			Person newGardener = new Person(999);
			newGardener.setName("Dandelion");
			johnDo.getHouse().setGardener(newGardener);
			personPersister.update(johnDo, loadedPerson, true);
			loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(johnDo.getHouse(), loadedPerson.getHouse());
			assertEquals(999, loadedPerson.getHouse().getGardener().getId().getSurrogate());
			assertEquals("Dandelion", loadedPerson.getHouse().getGardener().getName());
			
			personPersister.delete(johnDo);
			List<Long> allPersons = persistenceContext.newQuery("select id from Person", Long.class)
					.mapKey(Long::new, "id", long.class)
					.execute();
			assertEquals(Collections.emptyList(), allPersons);
		}
		
		@Test
		void insertSelect_cycleIsDirect_ownedBySource() {
			// we need a holder to skip final variable problem 
			Holder<IFluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.add(Person::getId).identifier(ALREADY_ASSIGNED)
					.add(Person::getName)
					.addOneToOne(Person::getPartner, () -> personMappingConfiguration.get().getConfiguration()));
			
			IEntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person johnDo = new Person(42);
			johnDo.setName("John Do");
			Person partner = new Person(666);
			partner.setName("Saca Do");
			johnDo.setPartner(partner);
			
			personPersister.insert(johnDo);
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(johnDo.getPartner(), loadedPerson.getPartner());
		}
		
		@Test
		void insertSelect_cycleIsDirect_ownedByTarget() {
			Table personTable = new Table("Person");
			Column<Table, Identifier<Long>> reversePartnerId = personTable.addColumn("reversePartnerId", Identifier.LONG_TYPE);
			// we need a holder to skip final variable problem
			Holder<IFluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.add(Person::getId).identifier(ALREADY_ASSIGNED)
					.add(Person::getName)
					.addOneToOne(Person::getPartner, () -> personMappingConfiguration.get().getConfiguration()).mappedBy(reversePartnerId));
			
			IEntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext, personTable);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person johnDo = new Person(42);
			johnDo.setName("John Do");
			Person partner = new Person(666);
			partner.setName("Saca Do");
			johnDo.setPartner(partner);
			
			personPersister.insert(johnDo);
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(johnDo, loadedPerson);
			assertEquals(johnDo.getPartner(), loadedPerson.getPartner());
		}
		
		@Test
		void crud_2cycles_ownedBySource() {
			// we need a holder to skip final variable problem 
			Holder<IFluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.add(Person::getId).identifier(ALREADY_ASSIGNED)
					.add(Person::getName)
					.addOneToOne(Person::getPartner, () -> personMappingConfiguration.get().getConfiguration())
					.addOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, Identifier.LONG_TYPE)
							.add(House::getId).identifier(ALREADY_ASSIGNED)
							.addOneToOne(House::getAddress, MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
									.add(Address::getId).identifier(ALREADY_ASSIGNED)
									.add(Address::getLocation))
							.addOneToOne(House::getGardener, () -> personMappingConfiguration.get().getConfiguration())
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
			);
			
			IEntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person johnDo = new Person(42);
			johnDo.setName("John Do");
			Person partner = new Person(666);
			partner.setName("Saca Do");
			johnDo.setPartner(partner);
			House house = new House(123);
			house.setAddress(new Address(456).setLocation("Somewhere in the world"));
			johnDo.setHouse(house);
			Person myGardener = new Person(888);
			myGardener.setName("Poppy");
			house.setGardener(myGardener);
			
			personPersister.insert(johnDo);
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(johnDo.getPartner(), loadedPerson.getPartner());
			assertEquals(johnDo.getHouse(), loadedPerson.getHouse());
			assertEquals(johnDo.getHouse().getGardener(), loadedPerson.getHouse().getGardener());
			
			Person newGardener = new Person(999);
			newGardener.setName("Dandelion");
			johnDo.getHouse().setGardener(newGardener);
			personPersister.update(johnDo, loadedPerson, true);
			loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(johnDo.getHouse(), loadedPerson.getHouse());
			assertEquals(999, loadedPerson.getHouse().getGardener().getId().getSurrogate());
			assertEquals("Dandelion", loadedPerson.getHouse().getGardener().getName());
			
			personPersister.delete(johnDo);
			List<Long> allPersons = persistenceContext.newQuery("select id from Person", Long.class)
					.mapKey(Long::new, "id", long.class)
					.execute();
			// previous partner is the only Person remainig because we asked for orphan removal
			assertEquals(Arrays.asList(666L), allPersons);
		}
		
		@Test
		void crud_2cycles_ownedBySource_entityCycle() {
			// we need a holder to skip final variable problem 
			Holder<IFluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.add(Person::getId).identifier(ALREADY_ASSIGNED)
					.add(Person::getName)
					.addOneToOne(Person::getPartner, () -> personMappingConfiguration.get().getConfiguration())
					.addOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, Identifier.LONG_TYPE)
							.add(House::getId).identifier(ALREADY_ASSIGNED)
							.addOneToOne(House::getAddress, MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
									.add(Address::getId).identifier(ALREADY_ASSIGNED)
									.add(Address::getLocation))
							.addOneToOne(House::getGardener, () -> personMappingConfiguration.get().getConfiguration())
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
			);
			
			IEntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person johnDo = new Person(42);
			johnDo.setName("John Do");
			Person partner = new Person(666);
			partner.setName("Saca Do");
			johnDo.setPartner(partner);
			House house = new House(123);
			house.setAddress(new Address(456).setLocation("Somewhere in the world"));
			johnDo.setHouse(house);
			// partner is also the gardener
			house.setGardener(partner);
			
			personPersister.insert(johnDo);
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(johnDo.getPartner(), loadedPerson.getPartner());
			assertEquals(johnDo.getHouse(), loadedPerson.getHouse());
			assertEquals(johnDo.getHouse().getGardener(), loadedPerson.getHouse().getGardener());
			// partner and gardeneer must be exactly same instance since its a cycle
			assertSame(loadedPerson.getPartner(), loadedPerson.getHouse().getGardener());
		}
		
		@Test
		void crud_2cycles_ownedByTarget() {
			Table personTable = new Table("Person");
			Column<Table, Identifier<Long>> reversePartnerId = personTable.addColumn("reversePartnerId", Identifier.LONG_TYPE);
			
			// we need a holder to skip final variable problem 
			Holder<IFluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.add(Person::getId).identifier(ALREADY_ASSIGNED)
					.add(Person::getName)
					.addOneToOne(Person::getPartner, () -> personMappingConfiguration.get().getConfiguration()).mappedBy(reversePartnerId)
					.addOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, Identifier.LONG_TYPE)
							.add(House::getId).identifier(ALREADY_ASSIGNED)
							.addOneToOne(House::getAddress, MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
									.add(Address::getId).identifier(ALREADY_ASSIGNED)
									.add(Address::getLocation))
							.addOneToOne(House::getGardener, () -> personMappingConfiguration.get().getConfiguration())
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
			);
			
			IEntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext, personTable);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person johnDo = new Person(42);
			johnDo.setName("John Do");
			Person partner = new Person(666);
			partner.setName("Saca Do");
			johnDo.setPartner(partner);
			House house = new House(123);
			house.setAddress(new Address(456).setLocation("Somewhere in the world"));
			johnDo.setHouse(house);
			Person myGardener = new Person(888);
			myGardener.setName("Poppy");
			house.setGardener(myGardener);
			
			personPersister.insert(johnDo);
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(johnDo.getPartner(), loadedPerson.getPartner());
			assertEquals(johnDo.getHouse(), loadedPerson.getHouse());
			assertEquals(johnDo.getHouse().getGardener(), loadedPerson.getHouse().getGardener());
			
			Person newGardener = new Person(999);
			newGardener.setName("Dandelion");
			johnDo.getHouse().setGardener(newGardener);
			personPersister.update(johnDo, loadedPerson, true);
			loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(johnDo.getHouse(), loadedPerson.getHouse());
			assertEquals(999, loadedPerson.getHouse().getGardener().getId().getSurrogate());
			assertEquals("Dandelion", loadedPerson.getHouse().getGardener().getName());
			
			personPersister.delete(johnDo);
			List<Long> allPersons = persistenceContext.newQuery("select id from Person", Long.class)
					.mapKey(Long::new, "id", long.class)
					.execute();
			// previous partner is the only Person remainig because we asked for orphan removal
			assertEquals(Arrays.asList(666L), allPersons);
		}
		
		@Test
		void crud_2cycles_ownedByTarget_entityCycle() {
			Table personTable = new Table("Person");
			Column<Table, Identifier<Long>> reversePartnerId = personTable.addColumn("reversePartnerId", Identifier.LONG_TYPE);
			
			// we need a holder to skip final variable problem 
			Holder<IFluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.add(Person::getId).identifier(ALREADY_ASSIGNED)
					.add(Person::getName)
					.addOneToOne(Person::getPartner, () -> personMappingConfiguration.get().getConfiguration()).mappedBy(reversePartnerId)
					.addOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, Identifier.LONG_TYPE)
							.add(House::getId).identifier(ALREADY_ASSIGNED)
							.addOneToOne(House::getAddress, MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
									.add(Address::getId).identifier(ALREADY_ASSIGNED)
									.add(Address::getLocation))
							.addOneToOne(House::getGardener, () -> personMappingConfiguration.get().getConfiguration())
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
			);
			
			IEntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext, personTable);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person johnDo = new Person(42);
			johnDo.setName("John Do");
			Person partner = new Person(666);
			partner.setName("Saca Do");
			johnDo.setPartner(partner);
			House house = new House(123);
			house.setAddress(new Address(456).setLocation("Somewhere in the world"));
			johnDo.setHouse(house);
			// partner is also the gardener
			house.setGardener(partner);
			
			personPersister.insert(johnDo);
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(johnDo.getPartner(), loadedPerson.getPartner());
			assertEquals(johnDo.getHouse(), loadedPerson.getHouse());
			assertEquals(johnDo.getHouse().getGardener(), loadedPerson.getHouse().getGardener());
			// partner and gardeneer must be exactly same instance since its a cycle
			assertSame(loadedPerson.getPartner(), loadedPerson.getHouse().getGardener());
		}
		
		@Test
		void crud_2cycles_sibling() {
			new Query();
			Table personTable = new Table("Person");
			
			// we need a holder to skip final variable problem 
			Holder<IFluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			IFluentEntityMappingBuilder<House, Identifier<Long>> houseMapping = MappingEase.entityBuilder(House.class,
					Identifier.LONG_TYPE)
					.add(House::getId).identifier(ALREADY_ASSIGNED)
					.add(House::getName);
			EntityMappingConfiguration<House, Identifier<Long>> houseMappingConfiguration = houseMapping.getConfiguration();
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.add(Person::getId).identifier(ALREADY_ASSIGNED)
					.add(Person::getName)
					.addOneToOne(Person::getHouse, houseMapping).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.addOneToOne(Person::getHouse1, houseMapping).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
			);
			
			IEntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext, personTable);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person johnDo = new Person(42);
			johnDo.setName("John Do");
			House house = new House(123);
			house.setName("main house");
			johnDo.setHouse(house);
			
			// adding a secondary house
			House house1 = new House(456);
			house1.setName("secondary house");
			johnDo.setHouse1(house1);
			
			personPersister.insert(johnDo);
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(johnDo.getHouse(), loadedPerson.getHouse());
			assertEquals(johnDo.getHouse1(), loadedPerson.getHouse1());
			
			johnDo.getHouse().setName("new main house name");
			personPersister.update(johnDo, loadedPerson, true);
			loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(johnDo.getHouse(), loadedPerson.getHouse());
		}
		
		@Test
		void crud_2cycles_sibling_ownedByTarget() {
			Table personTable = new Table("Person");
			Column<Table, Identifier<Long>> reversePartnerId = personTable.addColumn("reversePartnerId", Identifier.LONG_TYPE);
			
			// we need a holder to skip final variable problem 
			Holder<IFluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			IFluentEntityMappingBuilder<House, Identifier<Long>> houseMapping = MappingEase.entityBuilder(House.class, Identifier.LONG_TYPE)
					.add(House::getId).identifier(ALREADY_ASSIGNED)
					.addOneToOne(House::getAddress, MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
							.add(Address::getId).identifier(ALREADY_ASSIGNED)
							.add(Address::getLocation))
					.addOneToOne(House::getGardener, () -> personMappingConfiguration.get().getConfiguration())
					.cascading(RelationMode.ALL_ORPHAN_REMOVAL);
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.add(Person::getId).identifier(ALREADY_ASSIGNED)
					.add(Person::getName)
					.addOneToOne(Person::getPartner, () -> personMappingConfiguration.get().getConfiguration()).mappedBy(reversePartnerId)
					.addOneToOne(Person::getHouse, houseMapping).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.addOneToOne(Person::getHouse1, houseMapping).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
			);
			
			IEntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext, personTable);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person johnDo = new Person(42);
			johnDo.setName("John Do");
			Person partner = new Person(666);
			partner.setName("Saca Do");
			johnDo.setPartner(partner);
			House house = new House(123);
			house.setAddress(new Address(321).setLocation("Somewhere in the world"));
			johnDo.setHouse(house);
			Person myGardener = new Person(888);
			myGardener.setName("Poppy");
			house.setGardener(myGardener);
			
			// adding a secondary house
			House house1 = new House(456);
			house1.setAddress(new Address(654).setLocation("Somewhere else in the world"));
			johnDo.setHouse1(house1);
			Person myGardener1 = new Person(999);
			myGardener1.setName("Daffodil");
			house1.setGardener(myGardener1);
			
			personPersister.insert(johnDo);
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(johnDo.getPartner(), loadedPerson.getPartner());
			assertEquals(johnDo.getHouse(), loadedPerson.getHouse());
			assertEquals(johnDo.getHouse1(), loadedPerson.getHouse1());
			assertEquals(johnDo.getHouse().getGardener(), loadedPerson.getHouse().getGardener());
			
			Person newGardener = new Person(new PersistedIdentifier<>(999L));
			newGardener.setName("Dandelion");
			johnDo.getHouse1().setGardener(newGardener);
			personPersister.update(johnDo, loadedPerson, true);
			loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(johnDo.getHouse(), loadedPerson.getHouse());
			assertEquals(999, loadedPerson.getHouse1().getGardener().getId().getSurrogate());
			assertEquals("Dandelion", loadedPerson.getHouse1().getGardener().getName());
			
			personPersister.delete(johnDo);
			List<Long> allPersons = persistenceContext.newQuery("select id from Person", Long.class)
					.mapKey(Long::new, "id", long.class)
					.execute();
			// previous partner is the only Person remainig because we asked for orphan removal
			assertEquals(Arrays.asList(666L), allPersons);
		}
	}
	
	@Nested
	public class OneToMany {
		
		private PersistenceContext persistenceContext;
		
		private IFluentEntityMappingBuilder<Person, Identifier<Long>> personMappingConfiguration;
		
		@BeforeEach
		public void initTest() {
			persistenceContext = new PersistenceContext(new JdbcConnectionProvider(dataSource), DIALECT);
			
			personMappingConfiguration = MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.add(Person::getId).identifier(ALREADY_ASSIGNED)
					.add(Person::getName)
					.addOneToManySet(Person::getChildren, () -> personMappingConfiguration.getConfiguration());
		}
		
		@Test
		void select_withAssociationTable_1Parent_1Child() {
			IEntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person johnDo = new Person(42);
			johnDo.setName("John Do");
			Person child1 = new Person(666);
			child1.setName("Saca Do");
			johnDo.addChild(child1);
			
			personPersister.insert(johnDo);
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(johnDo.getChildren(), loadedPerson.getChildren());
		}
		
		@Test
		void select_withAssociationTable_1Parent_2Children() {
			IEntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person johnDo = new Person(42);
			johnDo.setName("John Do");
			Person child1 = new Person(666);
			child1.setName("Saca Do");
			johnDo.addChild(child1);
			Person child2 = new Person(888);
			child2.setName("Ban Do");
			johnDo.addChild(child2);
			
			personPersister.insert(johnDo);
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(johnDo.getChildren(), loadedPerson.getChildren());
		}
		
		@Test
		void select_ownedByReverseSide_1Parent_2Children() {
			personMappingConfiguration = MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.add(Person::getId).identifier(ALREADY_ASSIGNED)
					.add(Person::getName)
					.addOneToManySet(Person::getChildren, () -> personMappingConfiguration.getConfiguration())
					.mappedBy(Person::getFather);
			
			IEntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person johnDo = new Person(42);
			johnDo.setName("John Do");
			Person child1 = new Person(666);
			child1.setName("Saca Do");
			johnDo.addChild(child1);
			child1.setFather(johnDo);
			Person child2 = new Person(888);
			child2.setName("Ban Do");
			johnDo.addChild(child2);
			child2.setFather(johnDo);
			
			personPersister.insert(johnDo);
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			Assertions.assertAllEquals(johnDo.getChildren(), loadedPerson.getChildren());
		}
	}
	
	public static class Person implements Identified<Long> {
		
		private Identifier<Long> id;
		
		private String name;
		
		private Person partner;
		
		private Person father;
		
		private final Set<Person> children = new HashSet<>();
		
		private House house;
		
		private House house1;
		
		public Person() {
		}
		
		public Person(long id) {
			this(new PersistableIdentifier<>(id));
		}
		
		public Person(Identifier<Long> id) {
			this.id = id;
		}
		
		@Override
		public Identifier<Long> getId() {
			return id;
		}
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		public Person getPartner() {
			return partner;
		}
		
		public Person setPartner(Person partner) {
			this.partner = partner;
			return this;
		}
		
		public Person getFather() {
			return father;
		}
		
		public Person setFather(Person father) {
			this.father = father;
			return this;
		}
		
		public Set<Person> getChildren() {
			return children;
		}
		
		public void addChild(Person person) {
			this.children.add(person);
		}
		
		public House getHouse() {
			return house;
		}
		
		public Person setHouse(House house) {
			this.house = house;
			return this;
		}
		
		public House getHouse1() {
			return this.house1;
		}
		
		public Person setHouse1(House house) {
			this.house1 = house;
			return this;
		}
		
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof Person)) return false;
			
			Person person = (Person) o;
			
			// we don't take children (nor house) into account because its equals/hashCode (HashSet) is not steady because loaded instance is built gradually
			if (!id.equals(person.id)) return false;
			if (!Objects.equals(name, person.name)) return false;
			if (!Objects.equals(partner, person.partner)) return false;
			return Objects.equals(father, person.father);
		}
		
		@Override
		public int hashCode() {
			int result = id.hashCode();
			// we don't take children (nor house) into account because its equals/hashCode (HashSet) is not steady because loaded instance is built gradually
			result = 31 * result + (name != null ? name.hashCode() : 0);
			result = 31 * result + (partner != null ? partner.hashCode() : 0);
			result = 31 * result + (father != null ? father.hashCode() : 0);
			return result;
		}
		
		/**
		 * Implemented for easier debug
		 *
		 * @return a simple representation of this
		 */
		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}
	}
	
	public static class House implements Identified<Long> {
		
		private Identifier<Long> id;
		
		private Person gardener;
		
		private Address address;
		
		private String name;
		
		private Set<Person> inhabitants = new HashSet<>(); 
		
		public House() {
		}
		
		public House(long id) {
			this(new PersistableIdentifier<>(id));
		}
		
		public House(Identifier<Long> id) {
			this.id = id;
		}
		
		@Override
		public Identifier<Long> getId() {
			return id;
		}
		
		public Person getGardener() {
			return gardener;
		}
		
		public House setGardener(Person gardener) {
			this.gardener = gardener;
			return this;
		}
		
		public House addInhabitant(Person person) {
			this.inhabitants.add(person);
			return this;
		}
		
		public Address getAddress() {
			return address;
		}
		
		public House setAddress(Address address) {
			this.address = address;
			return this;
		}
		
		public String getName() {
			return name;
		}
		
		public House setName(String name) {
			this.name = name;
			return this;
		}
		
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof House)) return false;
			
			House house = (House) o;
			
			if (!id.equals(house.id)) return false;
			if (!Objects.equals(gardener, house.gardener)) return false;
			if (!Objects.equals(address, house.address)) return false;
			if (!Objects.equals(name, house.name)) return false;
			return inhabitants.equals(house.inhabitants);
		}
		
		@Override
		public int hashCode() {
			int result = id.hashCode();
			result = 31 * result + (gardener != null ? gardener.hashCode() : 0);
			result = 31 * result + (address != null ? address.hashCode() : 0);
			result = 31 * result + (name != null ? name.hashCode() : 0);
			result = 31 * result + inhabitants.hashCode();
			return result;
		}
		
		/**
		 * Implemented for easier debug
		 *
		 * @return a simple representation of this
		 */
		@Override
		public String toString() {
			return "House{" +
					"id=" + id +
					", gardener=" + gardener +
					", address=" + address +
					", name=" + name +
					", inhabitants=" + inhabitants +
					'}';
		}
	}
	
	public static class Address implements Identified<Long> {
		
		private Identifier<Long> id;
		
		private String location;
		
		public Address() {
		}
		
		public Address(long id) {
			this(new PersistableIdentifier<>(id));
		}
		
		public Address(Identifier<Long> id) {
			this.id = id;
		}
		
		@Override
		public Identifier<Long> getId() {
			return id;
		}
		
		public String getLocation() {
			return location;
		}
		
		public Address setLocation(String location) {
			this.location = location;
			return this;
		}
		
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof Address)) return false;
			
			Address address = (Address) o;
			
			if (!id.equals(address.id)) return false;
			return location.equals(address.location);
		}
		
		@Override
		public int hashCode() {
			int result = id.hashCode();
			result = 31 * result + location.hashCode();
			return result;
		}
		
		/**
		 * Implemented for easier debug
		 *
		 * @return a simple representation of this
		 */
		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}
	} 
}
