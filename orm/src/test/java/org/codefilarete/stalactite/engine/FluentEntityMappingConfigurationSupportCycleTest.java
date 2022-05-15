package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.runtime.PersisterWrapper;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifier;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.function.Hanger.Holder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;

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
		DIALECT.getSqlTypeRegistry().put(Identifier.class, "int");
	}
	
	@Nested
	public class OneToOne {
		
		private PersistenceContext persistenceContext;
		
		
		@BeforeEach
		public void initTest() {
			persistenceContext = new PersistenceContext(dataSource, DIALECT);
			
		}
		
		/**
		 * Person -> House -> Gardener
		 */
		@Test
		void crud_cycleWithIntermediary_ownedBySource() {
			Holder<FluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, Identifier.LONG_TYPE)
							.mapKey(House::getId, ALREADY_ASSIGNED)
							.mapOneToOne(House::getAddress, MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
									.mapKey(Address::getId, ALREADY_ASSIGNED)
									.map(Address::getLocation))
							.mapOneToOne(House::getGardener, () -> personMappingConfiguration.get().getConfiguration())
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
			);
			
			EntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext);
			
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
			assertThat(loadedPerson.getHouse()).isEqualTo(johnDo.getHouse());
			assertThat(loadedPerson.getHouse().getGardener()).isEqualTo(johnDo.getHouse().getGardener());
			
			Person newGardener = new Person(999);
			newGardener.setName("Dandelion");
			johnDo.getHouse().setGardener(newGardener);
			personPersister.update(johnDo, loadedPerson, true);
			loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedPerson.getHouse()).isEqualTo(johnDo.getHouse());
			assertThat(loadedPerson.getHouse().getGardener().getId().getSurrogate()).isEqualTo(999);
			assertThat(loadedPerson.getHouse().getGardener().getName()).isEqualTo("Dandelion");
			
			personPersister.delete(johnDo);
			List<Long> allPersons = persistenceContext.newQuery("select id from Person", Long.class)
					.mapKey(Long::new, "id", long.class)
					.execute();
			assertThat(allPersons).isEqualTo(Collections.emptyList());
		}
		
		/**
		 * Person -> House -> Gardener
		 */
		@Test
		void crud_cycleWithIntermediary_ownedByTarget() {
			Table personTable = new Table("Person");
			Column<Table, Identifier<Long>> reverseGardenerId = personTable.addColumn("reverseGardenerId", Identifier.LONG_TYPE);
			
			Holder<FluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, Identifier.LONG_TYPE)
							.mapKey(House::getId, ALREADY_ASSIGNED)
							.mapOneToOne(House::getAddress, MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
									.mapKey(Address::getId, ALREADY_ASSIGNED)
									.map(Address::getLocation))
							.mapOneToOne(House::getGardener, () -> personMappingConfiguration.get().getConfiguration()).mappedBy(reverseGardenerId)
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
			);
			
			EntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext, personTable);
			
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
			assertThat(loadedPerson.getHouse()).isEqualTo(johnDo.getHouse());
			assertThat(loadedPerson.getHouse().getGardener()).isEqualTo(johnDo.getHouse().getGardener());
			
			Person newGardener = new Person(999);
			newGardener.setName("Dandelion");
			johnDo.getHouse().setGardener(newGardener);
			personPersister.update(johnDo, loadedPerson, true);
			loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedPerson.getHouse()).isEqualTo(johnDo.getHouse());
			assertThat(loadedPerson.getHouse().getGardener().getId().getSurrogate()).isEqualTo(999);
			assertThat(loadedPerson.getHouse().getGardener().getName()).isEqualTo("Dandelion");
			
			personPersister.delete(johnDo);
			List<Long> allPersons = persistenceContext.newQuery("select id from Person", Long.class)
					.mapKey(Long::new, "id", long.class)
					.execute();
			assertThat(allPersons).isEqualTo(Collections.emptyList());
		}
		
		/**
		 * Person -> Partner
		 */
		@Test
		void insertSelect_cycleIsDirect_ownedBySource() {
			// we need a holder to skip final variable problem 
			Holder<FluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapOneToOne(Person::getPartner, () -> personMappingConfiguration.get().getConfiguration()));
			
			EntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person johnDo = new Person(42);
			johnDo.setName("John Do");
			Person partner = new Person(666);
			partner.setName("Saca Do");
			johnDo.setPartner(partner);
			
			personPersister.insert(johnDo);
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedPerson.getPartner()).isEqualTo(johnDo.getPartner());
		}
		
		/**
		 * Person -> Partner
		 */
		@Test
		void insertSelect_cycleIsDirect_ownedByTarget() {
			Table personTable = new Table("Person");
			Column<Table, Identifier<Long>> reversePartnerId = personTable.addColumn("reversePartnerId", Identifier.LONG_TYPE);
			// we need a holder to skip final variable problem
			Holder<FluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapOneToOne(Person::getPartner, () -> personMappingConfiguration.get().getConfiguration()).mappedBy(reversePartnerId));
			
			EntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext, personTable);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person johnDo = new Person(42);
			johnDo.setName("John Do");
			Person partner = new Person(666);
			partner.setName("Saca Do");
			johnDo.setPartner(partner);
			
			personPersister.insert(johnDo);
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedPerson).isEqualTo(johnDo);
			assertThat(loadedPerson.getPartner()).isEqualTo(johnDo.getPartner());
		}
		
		/**
		 * Person -> Partner
		 * Person -> House -> Gardener
		 */
		@Test
		void crud_2cycles_ownedBySource() {
			// we need a holder to skip final variable problem 
			Holder<FluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapOneToOne(Person::getPartner, () -> personMappingConfiguration.get().getConfiguration())
					.mapOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, Identifier.LONG_TYPE)
							.mapKey(House::getId, ALREADY_ASSIGNED)
							.mapOneToOne(House::getAddress, MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
									.mapKey(Address::getId, ALREADY_ASSIGNED)
									.map(Address::getLocation))
							.mapOneToOne(House::getGardener, () -> personMappingConfiguration.get().getConfiguration())
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
			);
			
			EntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext);
			
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
			assertThat(loadedPerson.getPartner()).isEqualTo(johnDo.getPartner());
			assertThat(loadedPerson.getHouse()).isEqualTo(johnDo.getHouse());
			assertThat(loadedPerson.getHouse().getGardener()).isEqualTo(johnDo.getHouse().getGardener());
			
			Person newGardener = new Person(999);
			newGardener.setName("Dandelion");
			johnDo.getHouse().setGardener(newGardener);
			personPersister.update(johnDo, loadedPerson, true);
			loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedPerson.getHouse()).isEqualTo(johnDo.getHouse());
			assertThat(loadedPerson.getHouse().getGardener().getId().getSurrogate()).isEqualTo(999);
			assertThat(loadedPerson.getHouse().getGardener().getName()).isEqualTo("Dandelion");
			
			personPersister.delete(johnDo);
			List<Long> allPersons = persistenceContext.newQuery("select id from Person", Long.class)
					.mapKey(Long::new, "id", long.class)
					.execute();
			// previous partner is the only Person remainig because we asked for orphan removal
			assertThat(allPersons).isEqualTo(Arrays.asList(666L));
		}
		
		/**
		 * Person -> Partner
		 * Person -> House -> Gardener
		 */
		@Test
		void crud_2cycles_ownedByTarget() {
			Table personTable = new Table("Person");
			Column<Table, Identifier<Long>> reversePartnerId = personTable.addColumn("reversePartnerId", Identifier.LONG_TYPE);
			
			// we need a holder to skip final variable problem 
			Holder<FluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapOneToOne(Person::getPartner, () -> personMappingConfiguration.get().getConfiguration()).mappedBy(reversePartnerId)
					.mapOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, Identifier.LONG_TYPE)
							.mapKey(House::getId, ALREADY_ASSIGNED)
							.mapOneToOne(House::getAddress, MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
									.mapKey(Address::getId, ALREADY_ASSIGNED)
									.map(Address::getLocation))
							.mapOneToOne(House::getGardener, () -> personMappingConfiguration.get().getConfiguration())
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
			);
			
			EntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext, personTable);
			
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
			assertThat(loadedPerson.getPartner()).isEqualTo(johnDo.getPartner());
			assertThat(loadedPerson.getHouse()).isEqualTo(johnDo.getHouse());
			assertThat(loadedPerson.getHouse().getGardener()).isEqualTo(johnDo.getHouse().getGardener());
			
			Person newGardener = new Person(999);
			newGardener.setName("Dandelion");
			johnDo.getHouse().setGardener(newGardener);
			personPersister.update(johnDo, loadedPerson, true);
			loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedPerson.getHouse()).isEqualTo(johnDo.getHouse());
			assertThat(loadedPerson.getHouse().getGardener().getId().getSurrogate()).isEqualTo(999);
			assertThat(loadedPerson.getHouse().getGardener().getName()).isEqualTo("Dandelion");
			
			personPersister.delete(johnDo);
			List<Long> allPersons = persistenceContext.newQuery("select id from Person", Long.class)
					.mapKey(Long::new, "id", long.class)
					.execute();
			// previous partner is the only Person remainig because we asked for orphan removal
			assertThat(allPersons).isEqualTo(Arrays.asList(666L));
		}
		
		/**
		 * Person -> Partner
		 * Person -> House -> Gardener
		 * with partner entity declared as gardener
		 */
		@Test
		void crud_2cycles_ownedBySource_entityCycle() {
			// we need a holder to skip final variable problem 
			Holder<FluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapOneToOne(Person::getPartner, () -> personMappingConfiguration.get().getConfiguration())
					.mapOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, Identifier.LONG_TYPE)
							.mapKey(House::getId, ALREADY_ASSIGNED)
							.mapOneToOne(House::getAddress, MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
									.mapKey(Address::getId, ALREADY_ASSIGNED)
									.map(Address::getLocation))
							.mapOneToOne(House::getGardener, () -> personMappingConfiguration.get().getConfiguration())
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
			);
			
			EntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext);
			
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
			assertThat(loadedPerson.getPartner()).isEqualTo(johnDo.getPartner());
			assertThat(loadedPerson.getHouse()).isEqualTo(johnDo.getHouse());
			assertThat(loadedPerson.getHouse().getGardener()).isEqualTo(johnDo.getHouse().getGardener());
			// partner and gardeneer must be exactly same instance since its a cycle
			assertThat(loadedPerson.getHouse().getGardener()).isSameAs(loadedPerson.getPartner());
		}
		
		/**
		 * Person -> Partner
		 * Person -> House -> Gardener
		 * with partner entity declared as gardener
		 */
		@Test
		void crud_2cycles_ownedByTarget_entityCycle() {
			Table personTable = new Table("Person");
			Column<Table, Identifier<Long>> reversePartnerId = personTable.addColumn("reversePartnerId", Identifier.LONG_TYPE);
			
			// we need a holder to skip final variable problem 
			Holder<FluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapOneToOne(Person::getPartner, () -> personMappingConfiguration.get().getConfiguration()).mappedBy(reversePartnerId)
					.mapOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, Identifier.LONG_TYPE)
							.mapKey(House::getId, ALREADY_ASSIGNED)
							.mapOneToOne(House::getAddress, MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
									.mapKey(Address::getId, ALREADY_ASSIGNED)
									.map(Address::getLocation))
							.mapOneToOne(House::getGardener, () -> personMappingConfiguration.get().getConfiguration())
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
			);
			
			EntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext, personTable);
			
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
			assertThat(loadedPerson.getPartner()).isEqualTo(johnDo.getPartner());
			assertThat(loadedPerson.getHouse()).isEqualTo(johnDo.getHouse());
			assertThat(loadedPerson.getHouse().getGardener()).isEqualTo(johnDo.getHouse().getGardener());
			// partner and gardeneer must be exactly same instance since its a cycle
			assertThat(loadedPerson.getHouse().getGardener()).isSameAs(loadedPerson.getPartner());
		}
		
		/**
		 * Person -> House -> Gardener
		 * Person -> House1 -> Gardener
		 */
		@Test
		void crud_2cycles_sibling() {
			new Query();
			Table personTable = new Table("Person");
			
			// we need a holder to skip final variable problem 
			Holder<FluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			FluentEntityMappingBuilder<House, Identifier<Long>> houseMapping = MappingEase.entityBuilder(House.class, Identifier.LONG_TYPE)
					.mapKey(House::getId, ALREADY_ASSIGNED)
					.map(House::getName);
			EntityMappingConfiguration<House, Identifier<Long>> houseMappingConfiguration = houseMapping.getConfiguration();
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapOneToOne(Person::getHouse, houseMapping).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapOneToOne(Person::getHouse1, houseMapping).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
			);
			
			EntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext, personTable);
			
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
			assertThat(loadedPerson.getHouse()).isEqualTo(johnDo.getHouse());
			assertThat(loadedPerson.getHouse1()).isEqualTo(johnDo.getHouse1());
			
			johnDo.getHouse().setName("new main house name");
			personPersister.update(johnDo, loadedPerson, true);
			loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedPerson.getHouse()).isEqualTo(johnDo.getHouse());
		}
		
		/**
		 * Person -> House -> Gardener
		 * Person -> House1 -> Gardener
		 */
		@Test
		void crud_2cycles_sibling_ownedByTarget() {
			Table personTable = new Table("Person");
			Column<Table, Identifier<Long>> reversePartnerId = personTable.addColumn("reversePartnerId", Identifier.LONG_TYPE);
			
			// we need a holder to skip final variable problem 
			Holder<FluentEntityMappingBuilder<Person, Identifier<Long>>> personMappingConfiguration = new Holder<>();
			FluentEntityMappingBuilder<House, Identifier<Long>> houseMapping = MappingEase.entityBuilder(House.class, Identifier.LONG_TYPE)
					.mapKey(House::getId, ALREADY_ASSIGNED)
					.mapOneToOne(House::getAddress, MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
							.mapKey(Address::getId, ALREADY_ASSIGNED)
							.map(Address::getLocation))
					.mapOneToOne(House::getGardener, () -> personMappingConfiguration.get().getConfiguration())
					.cascading(RelationMode.ALL_ORPHAN_REMOVAL);
			personMappingConfiguration.set(MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapOneToOne(Person::getPartner, () -> personMappingConfiguration.get().getConfiguration()).mappedBy(reversePartnerId)
					.mapOneToOne(Person::getHouse, houseMapping).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapOneToOne(Person::getHouse1, houseMapping).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
			);
			
			EntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.get().build(persistenceContext, personTable);
			
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
			assertThat(loadedPerson.getPartner()).isEqualTo(johnDo.getPartner());
			assertThat(loadedPerson.getHouse()).isEqualTo(johnDo.getHouse());
			assertThat(loadedPerson.getHouse1()).isEqualTo(johnDo.getHouse1());
			assertThat(loadedPerson.getHouse().getGardener()).isEqualTo(johnDo.getHouse().getGardener());
			
			Person newGardener = new Person(new PersistedIdentifier<>(999L));
			newGardener.setName("Dandelion");
			johnDo.getHouse1().setGardener(newGardener);
			personPersister.update(johnDo, loadedPerson, true);
			loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedPerson.getHouse()).isEqualTo(johnDo.getHouse());
			assertThat(loadedPerson.getHouse1().getGardener().getId().getSurrogate()).isEqualTo(999);
			assertThat(loadedPerson.getHouse1().getGardener().getName()).isEqualTo("Dandelion");
			
			personPersister.delete(johnDo);
			List<Long> allPersons = persistenceContext.newQuery("select id from Person", Long.class)
					.mapKey(Long::new, "id", long.class)
					.execute();
			// previous partner is the only Person remainig because we asked for orphan removal
			assertThat(allPersons).isEqualTo(Arrays.asList(666L));
		}
	}
	
	@Nested
	public class OneToMany {
		
		private PersistenceContext persistenceContext;
		
		private FluentEntityMappingBuilder<Person, Identifier<Long>> personMappingConfiguration;
		
		@BeforeEach
		public void initTest() {
			persistenceContext = new PersistenceContext(dataSource, DIALECT);
			
			personMappingConfiguration = MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapOneToManySet(Person::getChildren, () -> personMappingConfiguration.getConfiguration());
		}
		
		/**
		 * Person -> Children
		 */
		@Test
		void insertSelect_cycleIsDirect_withAssociationTable_1Parent_1Child() {
			EntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person johnDo = new Person(42);
			johnDo.setName("John Do");
			Person child1 = new Person(666);
			child1.setName("Saca Do");
			johnDo.addChild(child1);
			
			personPersister.insert(johnDo);
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedPerson.getChildren()).isEqualTo(johnDo.getChildren());
		}
		
		/**
		 * Person -> Children
		 */
		@Test
		void insertSelect_cycleIsDirect_withAssociationTable_1Parent_2Children() {
			EntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.build(persistenceContext);
			
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
			assertThat(loadedPerson.getChildren()).isEqualTo(johnDo.getChildren());
		}
		
		/**
		 * Person -> Children
		 */
		@Test
		void insertSelect_cycleIsDirect_ownedByReverseSide_1Parent_2Children() {
			personMappingConfiguration = MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapOneToManySet(Person::getChildren, () -> personMappingConfiguration.getConfiguration())
					.mappedBy(Person::getFather);
			
			EntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.build(persistenceContext);
			
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
			assertThat(johnDo.getChildren())
					.containsExactlyElementsOf(loadedPerson.getChildren());
		}
		
		/**
		 * Person -> Children
		 * Person -> Neighbours
		 */
		@Test
		void insertSelect_sibling_withAssociationTable() {
			// we need a holder to skip final variable problem 
			personMappingConfiguration = MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapOneToManySet(Person::getChildren, () -> personMappingConfiguration.getConfiguration())
						.reverselySetBy(Person::setFather)
					.mapOneToManySet(Person::getNeighbours, () -> personMappingConfiguration.getConfiguration())
						.reverselySetBy(Person::setDirectNeighboor);
			
			EntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.build(persistenceContext);
			
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
			
			Person neighbour1 = new Person(123);
			neighbour1.setName("Saca Do");
			johnDo.addNeighboor(neighbour1);
			neighbour1.setDirectNeighboor(johnDo);
			Person neighbour2 = new Person(456);
			neighbour2.setName("Ban Do");
			johnDo.addNeighboor(neighbour2);
			neighbour2.setDirectNeighboor(johnDo);
			
			personPersister.insert(johnDo);
			
			List<Map<Column<Table, Object>, ?>> capturedValues = new ArrayList<>();
			List<String> capturedSQL = new ArrayList<>();
			((SimpleRelationalEntityPersister) (((PersisterWrapper) personPersister).getDeepestSurrogate())).getSelectExecutor().setOperationListener(new SQLOperationListener<Column<Table, Object>>() {
				@Override
				public void onValuesSet(Map<Column<Table, Object>, ?> values) {
					capturedValues.add(values);
				}
				
				@Override
				public void onExecute(SQLStatement<Column<Table, Object>> sqlStatement) {
					capturedSQL.add(sqlStatement.getSQL());
				}
			});
			
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			
			assertThat(johnDo.getChildren())
					.containsExactlyElementsOf(loadedPerson.getChildren());
			
			// There must be :
			// - one SQL statement for very first select
			// - one SQL statement to select 2 children and 2 neighboors
			assertThat(capturedSQL)
					.containsExactly(
							"select" 
									+ " Person.name as Person_name," 
									+ " Person.id as Person_id," 
									+ " Person_children.children_id as Person_children_children_id," 
									+ " Person_neighbours.neighbour_id as Person_neighbours_neighbour_id" 
							+ " from Person" 
									+ " left outer join Person_children as Person_children on Person.id = Person_children.person_id" 
									+ " left outer join Person_neighbours as Person_neighbours on Person.id = Person_neighbours.person_id" 
							+ " where" 
									+ " Person.id in (?)",
							"select" 
									+ " Person.name as Person_name," 
									+ " Person.id as Person_id," 
									+ " Person_children.children_id as Person_children_children_id," 
									+ " Person_neighbours.neighbour_id as Person_neighbours_neighbour_id" 
							+ " from Person" 
									+ " left outer join Person_children as Person_children on Person.id = Person_children.person_id" 
									+ " left outer join Person_neighbours as Person_neighbours on Person.id = Person_neighbours.person_id" 
							+ " where" 
									+ " Person.id in (?, ?, ?, ?)"
					);
			assertThat(capturedValues)
					.extracting(map -> {
						Map<String, Object> result = new HashMap<>();
						map.forEach((column, value) -> {
							Object values;
							if (value instanceof List) {
								values = Iterables.collect((List<StatefulIdentifier>) value, StatefulIdentifier::getSurrogate, ArrayList::new);
							} else {
								values = ((StatefulIdentifier) value).getSurrogate();
							}
							result.put(column.getAbsoluteName(), values);
						});
						return result;
					})
					.containsExactly(
							Maps.forHashMap(String.class, Object.class)
									.add("Person.id", 42L),
							Maps.forHashMap(String.class, Object.class)
									.add("Person.id", Arrays.asList(888L, 456L, 666L, 123L))
					);
		}
		
		/**
		 * Person -> Children
		 * Person -> Neighbours
		 */
		@Test
		void insertSelect_sibling_ownedByTarget() {
			// we need a holder to skip final variable problem 
			personMappingConfiguration = MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapOneToManySet(Person::getChildren, () -> personMappingConfiguration.getConfiguration())
						.mappedBy(Person::setFather)
					.mapOneToManySet(Person::getNeighbours, () -> personMappingConfiguration.getConfiguration())
						.mappedBy(Person::setDirectNeighboor);
			
			EntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.build(persistenceContext);
			
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
			
			Person neighbour1 = new Person(123);
			neighbour1.setName("Saca Do");
			johnDo.addNeighboor(neighbour1);
			neighbour1.setDirectNeighboor(johnDo);
			Person neighbour2 = new Person(456);
			neighbour2.setName("Ban Do");
			johnDo.addNeighboor(neighbour2);
			neighbour2.setDirectNeighboor(johnDo);
			
			personPersister.insert(johnDo);
			
			List<Map<Column<Table, Object>, ?>> capturedValues = new ArrayList<>();
			List<String> capturedSQL = new ArrayList<>();
			((SimpleRelationalEntityPersister) (((PersisterWrapper) personPersister).getDeepestSurrogate())).getSelectExecutor().setOperationListener(new SQLOperationListener<Column<Table, Object>>() {
				@Override
				public void onValuesSet(Map<Column<Table, Object>, ?> values) {
					capturedValues.add(values);
				}
				
				@Override
				public void onExecute(SQLStatement<Column<Table, Object>> sqlStatement) {
					capturedSQL.add(sqlStatement.getSQL());
				}
			});
			
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			
			assertThat(johnDo.getChildren())
					.containsExactlyElementsOf(loadedPerson.getChildren());
			
			// There must be :
			// - one SQL statement for very first select
			// - one SQL statement to select 2 children and 2 neighboors
			assertThat(capturedSQL)
					.containsExactly(
							"select" 
									+ " Person.name as Person_name," 
									+ " Person.id as Person_id," 
									+ " Person_children.id as Person_children_id," 
									+ " Person_neighbours.id as Person_neighbours_id" 
								+ " from Person" 
									+ " left outer join Person as Person_children on Person.id = Person_children.fatherId" 
									+ " left outer join Person as Person_neighbours on Person.id = Person_neighbours.directNeighboorId" 
								+ " where" 
									+ " Person.id in (?)",
							"select"
									+ " Person.name as Person_name,"
									+ " Person.id as Person_id,"
									+ " Person_children.id as Person_children_id,"
									+ " Person_neighbours.id as Person_neighbours_id"
									+ " from Person"
									+ " left outer join Person as Person_children on Person.id = Person_children.fatherId"
									+ " left outer join Person as Person_neighbours on Person.id = Person_neighbours.directNeighboorId"
									+ " where"
									+ " Person.id in (?, ?, ?, ?)"
					);
			assertThat(capturedValues)
					.extracting(map -> {
						Map<String, Object> result = new HashMap<>();
						map.forEach((column, value) -> {
							Object values;
							if (value instanceof List) {
								values = Iterables.collect((List<StatefulIdentifier>) value, StatefulIdentifier::getSurrogate, ArrayList::new);
							} else {
								values = ((StatefulIdentifier) value).getSurrogate();
							}
							result.put(column.getAbsoluteName(), values);
						});
						return result;
					})
					.containsExactly(
							Maps.forHashMap(String.class, Object.class)
									.add("Person.id", 42L),
							Maps.forHashMap(String.class, Object.class)
									.add("Person.id", Arrays.asList(888L, 456L, 666L, 123L))
					);
		}
		
		/**
		 * Person -> Children
		 * Person -> Neighbours
		 */
		@Test
		void insertSelect_cycleIsDirect_withAssociationTable() {
			// we need a holder to skip final variable problem 
			personMappingConfiguration = MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapOneToManySet(Person::getChildren, () -> personMappingConfiguration.getConfiguration())
					.mappedBy(Person::setFather)
					.mapOneToManySet(Person::getNeighbours, () -> personMappingConfiguration.getConfiguration())
					.mappedBy(Person::setDirectNeighboor);
			
			EntityPersister<Person, Identifier<Long>> personPersister = personMappingConfiguration.build(persistenceContext);
			
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
			
			Person neighbour1 = new Person(123);
			neighbour1.setName("Saca Do");
			johnDo.addNeighboor(neighbour1);
			neighbour1.setDirectNeighboor(johnDo);
			Person neighbour2 = new Person(456);
			neighbour2.setName("Ban Do");
			johnDo.addNeighboor(neighbour2);
			neighbour2.setDirectNeighboor(johnDo);
			
			personPersister.insert(johnDo);
			
			List<Map<Column<Table, Object>, ?>> capturedValues = new ArrayList<>();
			List<String> capturedSQL = new ArrayList<>();
			((SimpleRelationalEntityPersister) (((PersisterWrapper) personPersister).getDeepestSurrogate())).getSelectExecutor().setOperationListener(new SQLOperationListener<Column<Table, Object>>() {
				@Override
				public void onValuesSet(Map<Column<Table, Object>, ?> values) {
					capturedValues.add(values);
				}
				
				@Override
				public void onExecute(SQLStatement<Column<Table, Object>> sqlStatement) {
					capturedSQL.add(sqlStatement.getSQL());
				}
			});
			
			Person loadedPerson = personPersister.select(new PersistedIdentifier<>(42L));
			
			assertThat(johnDo.getChildren())
					.containsExactlyElementsOf(loadedPerson.getChildren());
			
			// There must be :
			// - one SQL statement for very first select
			// - one SQL statement to select 2 children and 2 neighboors
			assertThat(capturedSQL)
					.containsExactly(
							"select"
									+ " Person.name as Person_name,"
									+ " Person.id as Person_id,"
									+ " Person_children.id as Person_children_id,"
									+ " Person_neighbours.id as Person_neighbours_id"
								+ " from Person"
									+ " left outer join Person as Person_children on Person.id = Person_children.fatherId"
									+ " left outer join Person as Person_neighbours on Person.id = Person_neighbours.directNeighboorId"
								+ " where"
									+ " Person.id in (?)",
							"select"
									+ " Person.name as Person_name,"
									+ " Person.id as Person_id,"
									+ " Person_children.id as Person_children_id,"
									+ " Person_neighbours.id as Person_neighbours_id"
								+ " from Person"
									+ " left outer join Person as Person_children on Person.id = Person_children.fatherId"
									+ " left outer join Person as Person_neighbours on Person.id = Person_neighbours.directNeighboorId"
								+ " where"
									+ " Person.id in (?, ?, ?, ?)"
					);
			assertThat(capturedValues)
					.extracting(map -> {
						Map<String, Object> result = new HashMap<>();
						map.forEach((column, value) -> {
							Object values;
							if (value instanceof List) {
								values = Iterables.collect((List<StatefulIdentifier>) value, StatefulIdentifier::getSurrogate, ArrayList::new);
							} else {
								values = ((StatefulIdentifier) value).getSurrogate();
							}
							result.put(column.getAbsoluteName(), values);
						});
						return result;
					})
					.containsExactly(
							Maps.forHashMap(String.class, Object.class)
									.add("Person.id", 42L),
							Maps.forHashMap(String.class, Object.class)
									.add("Person.id", Arrays.asList(888L, 456L, 666L, 123L))
					);
		}
	}
	
	public static class Person implements Identified<Long> {
		
		private Identifier<Long> id;
		
		private String name;
		
		private Person partner;
		
		private Person father;
		
		private final Set<Person> children = new HashSet<>();
		
		private Person directNeighboor;
		
		private final Set<Person> neighbours = new HashSet<>();
		
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
		
		public Person setDirectNeighboor(Person directNeighboor) {
			this.directNeighboor = directNeighboor;
			return this;
		}
		
		public Set<Person> getNeighbours() {
			return neighbours;
		}
		
		public void addNeighboor(Person neighbour) {
			this.neighbours.add(neighbour);
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
