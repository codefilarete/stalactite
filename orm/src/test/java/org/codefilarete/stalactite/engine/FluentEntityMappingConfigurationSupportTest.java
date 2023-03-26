package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.FluentEntityMappingBuilder.FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions;
import org.codefilarete.stalactite.engine.FluentEntityMappingBuilder.FluentMappingBuilderPropertyOptions;
import org.codefilarete.stalactite.engine.configurer.FluentEmbeddableMappingConfigurationSupport;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImplTest.ToStringBuilder;
import org.codefilarete.stalactite.engine.model.AbstractCountry;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Gender;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.PersonWithGender;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.codefilarete.stalactite.engine.runtime.EntityConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.PersisterWrapper;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.Dates;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.InvocationHandlerSupport;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.tool.function.Functions.chain;
import static org.codefilarete.tool.function.Functions.link;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class FluentEntityMappingConfigurationSupportTest {
	
	private static final Class<Identifier<UUID>> UUID_TYPE = (Class) Identifier.class;
	
	// NB: dialect is made non static because we register binder for the same column several times in these tests
	// and this is not supported : the very first one takes priority  
	private final HSQLDBDialect dialect = new HSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	void initTest() {
		dialect.getDmlGenerator().sortColumnsAlphabetically();	// for steady checks on SQL orders
		// binder creation for our identifier
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		
		persistenceContext = new PersistenceContext(dataSource, dialect);
	}
	
	@Test
	void build_identifierIsNotDefined_throwsException() {
		FluentMappingBuilderPropertyOptions<Toto, Identifier> mappingStrategy = MappingEase.entityBuilder(Toto.class, Identifier.class)
				.map(Toto::getId)
				.map(Toto::getName);
		
		// column should be correctly created
		assertThatThrownBy(() -> mappingStrategy.build(persistenceContext))
				.isInstanceOf(UnsupportedOperationException.class)
				.hasMessage("Identifier is not defined for o.c.s.e.FluentEntityMappingConfigurationSupportTest$Toto,"
						+ " please add one through o.c.s.e.FluentEntityMappingBuilder.mapKey(o.d.j.u.f.s.SerializableBiConsumer, o.c.s.e.ColumnOptions$IdentifierPolicy)");
	}
	
	@Nested
	class MapKey {
		
		@Test
		void calledTwice_throwsException() {
			assertThatThrownBy(() -> {
				MappingEase.entityBuilder(Toto.class, UUID_TYPE)
						.mapKey(Toto::getIdentifier, IdentifierPolicy.<Toto, Identifier<UUID>>alreadyAssigned(c -> c.getId().setPersisted(), c -> c.getId().isPersisted()))
						.mapKey(Toto::getId, IdentifierPolicy.<Toto, Identifier<UUID>>alreadyAssigned(c -> c.getId().setPersisted(), c -> c.getId().isPersisted()))
						.map(Toto::getName)
						.build(persistenceContext);
			}).hasMessage("Identifier is already defined by Toto::getIdentifier");
		}
		
		@Test
		void byDefault_defaultConstructorIsInvoked_setterIsCalled() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier<UUID>> idColumn = totoTable.addColumn("id", UUID_TYPE);
			
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, IdentifierPolicy.<Toto, Identifier<UUID>>alreadyAssigned(c -> c.getId().setPersisted(), c -> c.getId().isPersisted()))
					.map(Toto::getName)
					.build(persistenceContext, totoTable);	// necessary to set table since we override Identifier binding
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto entity = new Toto();
			entity.setName("Tutu");
			persister.insert(entity);
			Toto loadedInstance = persister.select(entity.getId());
			assertThat(loadedInstance.isSetIdWasCalled()).as("setId was called").isTrue();
			assertThat(loadedInstance.isConstructorWithIdWasCalled()).as("constructor with Id was called").isFalse();
		}
		
		@Test
		void usingConstructor_supplier_supplierIsCalled() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier<UUID>> idColumn = totoTable.addColumn("id", UUID_TYPE);
			
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			Supplier<Toto> constructor = Toto::newInstance;
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, IdentifierPolicy.<Toto, Identifier<UUID>>alreadyAssigned(c -> c.getId().setPersisted(), c -> c.getId().isPersisted()))
						.usingConstructor(constructor)
					.map(Toto::getName)
					.build(persistenceContext, totoTable);	// necessary to set table since we override Identifier binding
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto entity = new Toto();
			entity.setName("Tutu");
			persister.insert(entity);
			Toto loadedInstance = persister.select(entity.getId());
			assertThat(loadedInstance.isSetIdWasCalled()).as("setId was called").isTrue();
			assertThat(loadedInstance.isConstructorWithIdWasCalled()).as("constructor with Id was called").isFalse();
			assertThat(loadedInstance.getFirstName()).isEqualTo("set by static factory");
		}
		
		@Test
		void usingConstructor_constructorIsInvoked_setterIsNotCalled() {
			Table totoTable = new Table("Toto");

			dialect.getColumnBinderRegistry().register((Class) UUID_TYPE, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(UUID_TYPE, "VARCHAR(255)");
			
			Function<Identifier<UUID>, Toto> constructor = (Function<Identifier<UUID>, Toto>) (Function) (Function<PersistedIdentifier<UUID>, Toto>) Toto::new;
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, (Class<Identifier<UUID>>) (Class) UUID_TYPE)
					.mapKey(Toto::getId, IdentifierPolicy.<Toto, Identifier<UUID>>alreadyAssigned(c -> c.getId().setPersisted(), c -> c.getId().isPersisted()))
						.usingConstructor(constructor)
					.map(Toto::getName)
					.build(persistenceContext, totoTable);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto entity = new Toto();
			entity.setName("Tutu");
			persister.insert(entity);
			Toto loadedInstance = persister.select(entity.getId());
			assertThat(loadedInstance.isSetIdWasCalled()).as("setId was called").isFalse();
			assertThat(loadedInstance.isConstructorWithIdWasCalled()).as("constructor with Id was called").isTrue();
		}
		
		@Test
		<T extends Table<T>> void usingConstructor_1Arg_constructorIsInvoked_setterIsNotCalled() {
			T totoTable = (T) new Table("Toto");
			Column<T, Identifier<UUID>> idColumn = totoTable.addColumn("id", UUID_TYPE).primaryKey();

			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			Function<Identifier<UUID>, Toto> constructor = (Function<Identifier<UUID>, Toto>) (Function) (Function<PersistedIdentifier<UUID>, Toto>) Toto::new;
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, (Class<Identifier<UUID>>) (Class) UUID_TYPE)
					.mapKey(Toto::getId, IdentifierPolicy.<Toto, Identifier<UUID>>alreadyAssigned(c -> c.getId().setPersisted(), c -> c.getId().isPersisted()))
						.usingConstructor(constructor, idColumn)
					.map(Toto::getName)
					.build(persistenceContext, totoTable);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto entity = new Toto();
			entity.setName("Tutu");
			persister.insert(entity);
			Toto loadedInstance = persister.select(entity.getId());
			assertThat(loadedInstance.isSetIdWasCalled()).as("setId was called").isFalse();
			assertThat(loadedInstance.isConstructorWithIdWasCalled()).as("constructor with Id was called").isTrue();
		}
		
		@Test
		void usingConstructor_1ArgColumnName_constructorIsInvoked_setterIsNotCalled() {
			Table totoTable = new Table("Toto");
			
			dialect.getColumnBinderRegistry().register((Class) UUID_TYPE, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(UUID_TYPE, "VARCHAR(255)");
			
			Function<Identifier<UUID>, Toto> constructor = (Function<Identifier<UUID>, Toto>) (Function) (Function<PersistedIdentifier<UUID>, Toto>) Toto::new;
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, (Class<Identifier<UUID>>) (Class) UUID_TYPE)
					.mapKey(Toto::getId, IdentifierPolicy.<Toto, Identifier<UUID>>alreadyAssigned(c -> c.getId().setPersisted(), c -> c.getId().isPersisted()))
						.usingConstructor(constructor, "myId")
					.map(Toto::getName)
					.build(persistenceContext, totoTable);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto entity = new Toto();
			entity.setName("Tutu");
			persister.insert(entity);
			Toto loadedInstance = persister.select(entity.getId());
			assertThat(loadedInstance.isSetIdWasCalled()).as("setId was called").isFalse();
			assertThat(loadedInstance.isConstructorWithIdWasCalled()).as("constructor with Id was called").isTrue();
		}
		
		@Test
		<T extends Table<T>> void usingConstructor_2Args_constructorIsInvoked_setterIsNotCalled() {
			T totoTable = (T) new Table("Toto");
			Column<T, Identifier<UUID>> idColumn = totoTable.addColumn("id", UUID_TYPE).primaryKey();
			Column<T, String> nameColumn = totoTable.addColumn("name", String.class);
			
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			BiFunction<Identifier<UUID>, String, Toto> constructor = (BiFunction<Identifier<UUID>, String, Toto>) (BiFunction) (BiFunction<PersistedIdentifier<UUID>, String, Toto>) Toto::new;
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, (Class<Identifier<UUID>>) (Class) UUID_TYPE)
					.mapKey(Toto::getId, IdentifierPolicy.<Toto, Identifier<UUID>>alreadyAssigned(c -> c.getId().setPersisted(), c -> c.getId().isPersisted()))
						.usingConstructor(constructor, idColumn, nameColumn)
					.map(Toto::getName).setByConstructor()	// avoiding superfluous property setting
					.build(persistenceContext, totoTable);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto entity = new Toto();
			entity.setName("Tutu");
			persister.insert(entity);
			Toto loadedInstance = persister.select(entity.getId());
			assertThat(loadedInstance.isSetIdWasCalled()).as("setId was called").isFalse();
			assertThat(loadedInstance.isConstructorWith2ArgsWasCalled()).as("constructor with Id was called").isTrue();
			// Checking that property is not overridden by setter access (because we used setByConstructor)
			assertThat(loadedInstance.getName()).isEqualTo("Tutu by constructor");
		}
		
		@Test
		void usingFactory_constructorIsInvoked_setterIsNotCalled() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier<UUID>> idColumn = totoTable.addColumn("id", UUID_TYPE).primaryKey();
			Column<Table, String> nameColumn = totoTable.addColumn("name", String.class);
			
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, IdentifierPolicy.<Toto, Identifier<UUID>>alreadyAssigned(c -> c.getId().setPersisted(), c -> c.getId().isPersisted()))
					.usingFactory(row ->
						new Toto((PersistedIdentifier<UUID>) row.apply(idColumn), (String) row.apply(nameColumn))
					)
					.map(Toto::getName).setByConstructor()	// avoiding superfluous property setting
					.build(persistenceContext, totoTable);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto entity = new Toto();
			entity.setName("Tutu");
			persister.insert(entity);
			Toto loadedInstance = persister.select(entity.getId());
			assertThat(loadedInstance.isSetIdWasCalled()).as("setId was called").isFalse();
			assertThat(loadedInstance.isConstructorWith2ArgsWasCalled()).as("constructor with Id was called").isTrue();
			// Checking that property is not overridden by setter access (because we used setByConstructor)
			assertThat(loadedInstance.getName()).isEqualTo("Tutu by constructor");
		}
		
	}
	
	@Nested
	class UseConstructor {
		
		@Test
		void byDefault_defaultConstructorIsInvoked_setterIsCalled() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier<UUID>> idColumn = totoTable.addColumn("id", UUID_TYPE);
			
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.build(persistenceContext, totoTable);	// necessary to set table since we override Identifier binding
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto entity = new Toto();
			entity.setName("Tutu");
			persister.insert(entity);
			Toto loadedInstance = persister.select(entity.getId());
			assertThat(loadedInstance.isSetIdWasCalled()).as("setId was called").isTrue();
			assertThat(loadedInstance.isConstructorWithIdWasCalled()).as("constructor with Id was called").isFalse();
		}
		
		@Test
		<T extends Table<T>> void withConstructorSpecified_constructorIsInvoked_setterIsNotCalled() {
			T totoTable = (T) new Table("Toto");
			Column<T, Identifier<UUID>> idColumn = totoTable.addColumn("id", UUID_TYPE);
			
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			Function<Identifier<UUID>, Toto> constructor = (Function<Identifier<UUID>, Toto>) (Function) (Function<PersistedIdentifier<UUID>, Toto>) Toto::new;
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.usingConstructor(constructor, idColumn)
					.map(Toto::getName)
					.build(persistenceContext, totoTable);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto entity = new Toto();
			entity.setName("Tutu");
			persister.insert(entity);
			Toto loadedInstance = persister.select(entity.getId());
			assertThat(loadedInstance.isSetIdWasCalled()).as("setId was called").isFalse();
			assertThat(loadedInstance.isConstructorWithIdWasCalled()).as("constructor with Id was called").isTrue();
		}
		
		@Test
		void withConstructorSpecified_byColumnName_constructorIsInvoked() {
			Table totoTable = new Table("Toto");

			ParameterBinder<Identifier<UUID>> statefulIdentifierParameterBinder = (ParameterBinder) Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER);
			dialect.getColumnBinderRegistry().register(UUID_TYPE, statefulIdentifierParameterBinder);
			dialect.getSqlTypeRegistry().put(UUID_TYPE, "VARCHAR(255)");
			
			Function<Identifier<UUID>, Toto> constructor = (Function<Identifier<UUID>, Toto>) (Function) (Function<PersistedIdentifier<UUID>, Toto>) Toto::new;
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED, "identifier")
					.usingConstructor(constructor, "identifier")
					.map(Toto::getName, "label")
					.build(persistenceContext, totoTable);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			Toto entity = new Toto();
			persister.insert(entity);
			Toto loadedInstance = persister.select(entity.getId());
			assertThat(loadedInstance.isSetIdWasCalled()).as("setId was called").isFalse();
			assertThat(loadedInstance.isConstructorWithIdWasCalled()).as("constructor with Id was called").isTrue();
		}
		
		@Test
		<T extends Table<T>> void setByConstructor_constructorIsInvoked_setterIsCalled() {
			T totoTable = (T) new Table("Toto");
			Column<T, Identifier<UUID>> idColumn = totoTable.addColumn("id", UUID_TYPE);
			
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			Function<Identifier<UUID>, Toto> constructor = (Function<Identifier<UUID>, Toto>) (Function) (Function<PersistedIdentifier<UUID>, Toto>) Toto::new;
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.usingConstructor(constructor, idColumn)
					.map(Toto::getName)
					.build(persistenceContext, totoTable);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto entity = new Toto();
			entity.setName("Tutu");
			persister.insert(entity);
			Toto loadedInstance = persister.select(entity.getId());
			assertThat(loadedInstance.isSetIdWasCalled()).as("setId was called").isFalse();
			assertThat(loadedInstance.isConstructorWithIdWasCalled()).as("constructor with Id was called").isTrue();
		}
		
		@Test
		<T extends Table<T>> void setByConstructor_withSeveralArguments() {
			T totoTable = (T) new Table("Toto");
			Column<T, Identifier<UUID>> idColumn = totoTable.addColumn("id", UUID_TYPE);
			Column<T, String> nameColumn = totoTable.addColumn("name", String.class);
			
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			BiFunction<Identifier<UUID>, String, Toto> constructor = (BiFunction<Identifier<UUID>, String, Toto>) (BiFunction) (BiFunction<PersistedIdentifier<UUID>, String, Toto>) Toto::new;
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.usingConstructor(constructor, idColumn, nameColumn)
					.map(Toto::getName).setByConstructor()
					.build(persistenceContext, totoTable);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto entity = new Toto();
			entity.setName("Hello");
			persister.insert(entity);
			Toto loadedInstance = persister.select(entity.getId());
			assertThat(loadedInstance.isSetIdWasCalled()).as("setId was called").isFalse();
			assertThat(loadedInstance.getName()).isEqualTo("Hello by constructor");
		}
	}
	
	@Test
	void add_withoutName_targetedPropertyNameIsTaken() {
		EntityConfiguredPersister<Toto, Identifier<UUID>> persister = (EntityConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getName)
				.build(persistenceContext);
		
		// column should be correctly created
		assertThat(persister.getMapping().getTargetTable().getName()).isEqualTo("Toto");
		Column columnForProperty = (Column) persister.getMapping().getTargetTable().mapColumnsOnName().get("name");
		assertThat(columnForProperty).isNotNull();
		assertThat(columnForProperty.getJavaType()).isEqualTo(String.class);
	}
	
	@Test
	void add_mandatory_onMissingValue_throwsException() {
		Table totoTable = new Table("Toto");
		Column idColumn = totoTable.addColumn("id", UUID_TYPE);
		dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
		
		EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getName).mandatory()
				.map(Toto::getFirstName).mandatory()
				.build(persistenceContext);
		
		// column should be correctly created
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Toto toto = new Toto();
		assertThatThrownBy(() -> persister.insert(toto))
				.isInstanceOf(RuntimeException.class)
				.hasMessage("Error while inserting values for " + toto + " in statement \"insert into Toto(firstName, id, name) values (?, ?, ?)\"")
				.hasCause(new BindingException("Expected non null value for : Toto.firstName, Toto.name"));
	}
	
	@Test
	void add_mandatory_columnConstraintIsAdded() {
		EntityConfiguredJoinedTablesPersister<Toto, Identifier<UUID>> totoPersister = (EntityConfiguredJoinedTablesPersister<Toto, Identifier<UUID>>)
				MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getName).mandatory()
				.build(persistenceContext);
		
		assertThat(totoPersister.getMapping().getTargetTable().getColumn("name").isNullable()).isFalse();
	}
	
	@Test
	void add_withColumn_columnIsTaken() {
		Table toto = new Table("Toto");
		Column<Table, String> titleColumn = toto.addColumn("title", String.class);
		ConfiguredPersister<Toto, Identifier<UUID>> persister = (ConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getName, titleColumn)
				.build(persistenceContext);
		
		// column should not have been created
		Column columnForProperty = (Column) persister.getMapping().getTargetTable().mapColumnsOnName().get("name");
		assertThat(columnForProperty).isNull();
		
		// title column is expected to be added to the mapping and participate to DML actions 
		assertThat(persister.getMapping().getInsertableColumns().stream().map(Column::getName).collect(Collectors.toSet())).isEqualTo(Arrays.asSet("id", "title"));
		assertThat(persister.getMapping().getUpdatableColumns().stream().map(Column::getName).collect(Collectors.toSet())).isEqualTo(Arrays.asSet("title"));
	}
	
	@Test
	void add_definedAsIdentifier_identifierIsStoredAsString() {
		HSQLDBDialect dialect = new HSQLDBDialect();
		PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		
		Table totoTable = new Table("Toto");
		Column id = totoTable.addColumn("id", UUID_TYPE).primaryKey();
		Column name = totoTable.addColumn("name", String.class);
		// binder creation for our identifier
		dialect.getColumnBinderRegistry().register(id, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(id, "varchar(255)");
		
		EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.build(persistenceContext, totoTable);
		// column should be correctly created
		assertThat(totoTable.getColumn("id").isPrimaryKey()).isTrue();
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Toto toto = new Toto();
		toto.setName("toto");
		persister.persist(toto);
		
		List<Duo> select = persistenceContext.select(Duo::new, id, name);
		assertThat(select.size()).isEqualTo(1);
		assertThat(((Identifier) select.get(0).getLeft()).getSurrogate().toString()).isEqualTo(toto.getId().getSurrogate().toString());
	}
	
	@Test
	void add_mappingDefinedTwiceByMethod_throwsException() {
		assertThatThrownBy(() -> MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.map(Toto::setName)
					.build(persistenceContext))
				.isInstanceOf(MappingConfigurationException.class)
				.hasMessage("Mapping is already defined by method Toto::getName");
	}
	
	@Test
	void add_mappingDefinedTwiceByColumn_throwsException() {
		assertThatThrownBy(() -> MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName, "xyz")
					.map(Toto::getFirstName, "xyz")
					.build(persistenceContext))
				.isInstanceOf(MappingConfigurationException.class)
				.hasMessage("Column 'xyz' of mapping 'Toto::getName' is already targetted by 'Toto::getFirstName'");
	}
	
	@Test
	void add_methodHasNoMatchingField_configurationIsStillValid() {
		Table toto = new Table("Toto");
		MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getNoMatchingField)
				.build(persistenceContext, toto);
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("noMatchingField");
		assertThat(columnForProperty).isNotNull();
		assertThat(columnForProperty.getJavaType()).isEqualTo(Long.class);
	}
	
	@Test
	void add_methodIsASetter_configurationIsStillValid() {
		Table toto = new Table("Toto");
		MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::setId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.build(persistenceContext, toto);
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("id");
		assertThat(columnForProperty).isNotNull();
		assertThat(columnForProperty.getJavaType()).isEqualTo(UUID_TYPE);
	}
	
	@Test
	void embed_definedByGetter() {
		Table toto = new Table("Toto");
		MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
				.build(new PersistenceContext((ConnectionProvider) null, dialect), toto);
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("creationDate");
		assertThat(columnForProperty).isNotNull();
		assertThat(columnForProperty.getJavaType()).isEqualTo(Date.class);
	}
	
	@Test
	void embed_definedBySetter() {
		Table toto = new Table("Toto");
		MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.embed(Toto::setTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
				.build(new PersistenceContext((ConnectionProvider) null, dialect), toto);
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("creationDate");
		assertThat(columnForProperty).isNotNull();
		assertThat(columnForProperty.getJavaType()).isEqualTo(Date.class);
	}
	
	@Test
	void embed_withOverriddenColumnName() {
		Table toto = new Table("Toto");
		MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
					.overrideName(Timestamp::getCreationDate, "createdAt")
					.overrideName(Timestamp::getModificationDate, "modifiedAt")
				.build(new PersistenceContext((ConnectionProvider) null, dialect), toto);
		
		Map<String, Column> columnsByName = toto.mapColumnsOnName();
		
		// columns with getter name must be absent (hard to test: can be absent for many reasons !)
		assertThat(columnsByName.get("creationDate")).isNull();
		assertThat(columnsByName.get("modificationDate")).isNull();

		Column overriddenColumn;
		// Columns with good name must be present
		overriddenColumn = columnsByName.get("modifiedAt");
		assertThat(overriddenColumn).isNotNull();
		assertThat(overriddenColumn.getJavaType()).isEqualTo(Date.class);
		overriddenColumn = columnsByName.get("createdAt");
		assertThat(overriddenColumn).isNotNull();
		assertThat(overriddenColumn.getJavaType()).isEqualTo(Date.class);
	}
	
	@Test
	void embed_withOverriddenColumnName_nameAlreadyExists_throwsException() {
		Table totoTable = new Table("Toto");
		Column<Table, Identifier> idColumn = totoTable.addColumn("id", UUID_TYPE);
		dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
		
		assertThatThrownBy(() -> MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.embed(Toto::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.overrideName(Timestamp::getCreationDate, "modificationDate")
					.build(persistenceContext))
				.isInstanceOf(MappingConfigurationException.class)
				.hasMessage("Column 'modificationDate' of mapping 'Timestamp::getCreationDate' is already targetted by 'Timestamp::getModificationDate'");
	}
	
	@Test
	void embed_withOverriddenColumn() {
		Table targetTable = new Table("Toto");
		Column<Table, Date> createdAt = targetTable.addColumn("createdAt", Date.class);
		Column<Table, Date> modifiedAt = targetTable.addColumn("modifiedAt", Date.class);
		
		Connection connectionMock = InvocationHandlerSupport.mock(Connection.class);
		
		ConfiguredPersister<Toto, Identifier<UUID>> persister = (ConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
					.override(Timestamp::getCreationDate, createdAt)
					.override(Timestamp::getModificationDate, modifiedAt)
				.build(new PersistenceContext(new SimpleConnectionProvider(connectionMock), dialect), targetTable);
		
		Map<String, Column> columnsByName = targetTable.mapColumnsOnName();
		
		// columns with getter name must be absent (hard to test: can be absent for many reasons !)
		assertThat(columnsByName.get("creationDate")).isNull();
		assertThat(columnsByName.get("modificationDate")).isNull();
		
		// checking that overridden column are in DML statements
		assertThat(persister.getMapping().getInsertableColumns()).isEqualTo(targetTable.getColumns());
		assertThat(persister.getMapping().getUpdatableColumns()).isEqualTo(targetTable.getColumnsNoPrimaryKey());
		assertThat(persister.getMapping().getSelectableColumns()).isEqualTo(targetTable.getColumns());
	}
	
	@Test
	void embed_withSomeExcludedProperty() {
		Table targetTable = new Table("Toto");
		Column<Table, Date> modifiedAt = targetTable.addColumn("modifiedAt", Date.class);
		
		Connection connectionMock = InvocationHandlerSupport.mock(Connection.class);
		
		ConfiguredPersister<Toto, Identifier<UUID>> persister = (ConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
					.exclude(Timestamp::getCreationDate)
					.override(Timestamp::getModificationDate, modifiedAt)
				.build(new PersistenceContext(new SimpleConnectionProvider(connectionMock), dialect), targetTable);
		
		Map<String, Column> columnsByName = targetTable.mapColumnsOnName();
		
		// columns with getter name must be absent (hard to test: can be absent for many reasons !)
		assertThat(columnsByName.get("creationDate")).isNull();
		assertThat(columnsByName.get("modificationDate")).isNull();
		
		// checking that overridden column are in DML statements
		assertThat(persister.getMapping().getInsertableColumns()).isEqualTo(targetTable.getColumns());
		assertThat(persister.getMapping().getUpdatableColumns()).isEqualTo(targetTable.getColumnsNoPrimaryKey());
		assertThat(persister.getMapping().getSelectableColumns()).isEqualTo(targetTable.getColumns());
	}
	
	@Test
	void innerEmbed_withConflictingEmbeddable() throws SQLException {
		Table<?> countryTable = new Table<>("countryTable");
		
		FluentEntityMappingBuilder<Country, Identifier<Long>> mappingBuilder = MappingEase
				.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				.embed(Country::getPresident, MappingEase.embeddableBuilder(Person.class)
						.map(Person::getName)
						.map(Person::getVersion)
						.embed(Person::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
								.map(Timestamp::getCreationDate)
								.map(Timestamp::getModificationDate))
						.exclude(Timestamp::getCreationDate)
						.overrideName(Timestamp::getModificationDate, "presidentElectedAt"))
					.exclude(Person::getVersion)
					.overrideName(Person::getName, "presidentName")
					
				// this embed will conflict with Person one because its type is already mapped with no override
				.embed(Country::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
					.exclude(Timestamp::getModificationDate)
					.overrideName(Timestamp::getCreationDate, "countryCreatedAt");
		
		mappingBuilder.build(persistenceContext, countryTable);
		
		assertThat(countryTable.getColumns()).extracting(Column::getName).containsExactlyInAnyOrder(
				// from Country
				"id", "name",
				// from Person
				"presidentName", "presidentElectedAt",
				// from Country.timestamp
				"countryCreatedAt");
		
		Connection connectionMock = mock(Connection.class);
		
		// since we mock connection, we use a non-specific Dialect to avoid specific behavior on a generic connection
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getDmlGenerator().sortColumnsAlphabetically();	// for steady checks on SQL orders
		
		ConfiguredPersister<Country, Identifier<Long>> persister = (ConfiguredPersister<Country, Identifier<Long>>) mappingBuilder.build(
				new PersistenceContext(new SimpleConnectionProvider(connectionMock), dialect), countryTable);
		
		Map<String, ? extends Column> columnsByName = countryTable.mapColumnsOnName();
		
		// columns with getter name must be absent (hard to test: can be absent for many reasons !)
		assertThat(columnsByName.get("creationDate")).isNull();
		assertThat(columnsByName.get("modificationDate")).isNull();
		
		// checking that overridden column are in DML statements
		assertThat(persister.getMapping().getInsertableColumns()).isEqualTo(countryTable.getColumns());
		assertThat(persister.getMapping().getUpdatableColumns()).isEqualTo(countryTable.getColumnsNoPrimaryKey());
		assertThat(persister.getMapping().getSelectableColumns()).isEqualTo(countryTable.getColumns());
		
		Country country = new Country(new PersistableIdentifier<>(1L));
		country.setName("France");
		
		Timestamp countryTimestamp = new Timestamp();
		LocalDateTime localDateTime = LocalDate.of(2018, 01, 01).atStartOfDay();
		Date countryCreationDate = Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
		countryTimestamp.setCreationDate(countryCreationDate);
		country.setTimestamp(countryTimestamp);
		
		Person president = new Person();
		president.setName("François");
		
		Timestamp presidentTimestamp = new Timestamp();
		Date presidentElection = Date.from(LocalDate.of(2019, 01, 01).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
		presidentTimestamp.setModificationDate(presidentElection);
		president.setTimestamp(presidentTimestamp);
		
		country.setPresident(president);
		
		// preparing JDBC mocks and values capture
		PreparedStatement statementMock = mock(PreparedStatement.class);
		when(statementMock.executeLargeBatch()).thenReturn(new long[] { 1 });
		Map<Column<Table, Object>, Object> capturedValues = new HashMap<>();
		when(connectionMock.prepareStatement(anyString())).thenReturn(statementMock);
		
		StringBuilder capturedSQL = new StringBuilder();
		((SimpleRelationalEntityPersister) (((PersisterWrapper) persister).getDeepestSurrogate())).getInsertExecutor().setOperationListener(new SQLOperationListener<Column<Table, Object>>() {
			@Override
			public void onValuesSet(Map<Column<Table, Object>, ?> values) {
				capturedValues.putAll(values);
			}
			
			@Override
			public void onExecute(SQLStatement<Column<Table, Object>> sqlStatement) {
				capturedSQL.append(sqlStatement.getSQL());
			}
		});
		
		// Testing ...
		persister.insert(country);
		
		assertThat(capturedSQL.toString()).isEqualTo("insert into countryTable(countryCreatedAt, id, name, presidentElectedAt, presidentName) values" 
				+ " (?, ?, ?, ?, ?)");
		assertThat(capturedValues).isEqualTo(Maps.forHashMap(Column.class, Object.class)
				.add(columnsByName.get("presidentName"), country.getPresident().getName())
				.add(columnsByName.get("presidentElectedAt"), country.getPresident().getTimestamp().getModificationDate())
				.add(columnsByName.get("name"), country.getName())
				.add(columnsByName.get("countryCreatedAt"), country.getTimestamp().getCreationDate())
				.add(columnsByName.get("id"), country.getId()));
	}
	
	@Test
	void innerEmbed_withSomeExcludedProperties() throws SQLException {
		Table<?> countryTable = new Table<>("countryTable");
		
		FluentEntityMappingBuilder<Country, Identifier<Long>> mappingBuilder = MappingEase
				.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				.embed(Country::getPresident, MappingEase.embeddableBuilder(Person.class)
						.map(Person::getName)
						.map(Person::getVersion)
						.embed(Person::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
								.map(Timestamp::getCreationDate)
								.map(Timestamp::getModificationDate))
						.exclude(Timestamp::getCreationDate)
						.overrideName(Timestamp::getModificationDate, "presidentElectedAt"))
					.exclude(Person::getVersion)
					.overrideName(Person::getName, "presidentName");
		
		mappingBuilder.build(persistenceContext, countryTable);
		
		assertThat(countryTable.getColumns()).extracting(Column::getName).containsExactlyInAnyOrder(
				// from Country
				"id", "name",
				// from Person
				"presidentName", "presidentElectedAt");
		
		Connection connectionMock = mock(Connection.class);
		
		// since we mock connection, we use a non-specific Dialect to avoid specific behavior on a generic connection
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) UUID_TYPE, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getDmlGenerator().sortColumnsAlphabetically();	// for steady checks on SQL orders
		
		ConfiguredPersister<Country, Identifier<Long>> persister = (ConfiguredPersister<Country, Identifier<Long>>) mappingBuilder.build(
				new PersistenceContext(new SimpleConnectionProvider(connectionMock), dialect), countryTable);
		
		Map<String, ? extends Column> columnsByName = countryTable.mapColumnsOnName();
		
		// columns with getter name must be absent (hard to test: can be absent for many reasons !)
		assertThat(columnsByName.get("creationDate")).isNull();
		assertThat(columnsByName.get("modificationDate")).isNull();
		
		// checking that overridden column are in DML statements
		assertThat(persister.getMapping().getInsertableColumns()).isEqualTo(countryTable.getColumns());
		assertThat(persister.getMapping().getUpdatableColumns()).isEqualTo(countryTable.getColumnsNoPrimaryKey());
		assertThat(persister.getMapping().getSelectableColumns()).isEqualTo(countryTable.getColumns());
		
		Country country = new Country(new PersistableIdentifier<>(1L));
		country.setName("France");
		
		Person president = new Person();
		president.setName("François");
		
		Timestamp presidentTimestamp = new Timestamp();
		Date presidentElection = Date.from(LocalDate.of(2019, 01, 01).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
		presidentTimestamp.setModificationDate(presidentElection);
		president.setTimestamp(presidentTimestamp);
		
		country.setPresident(president);
		
		// preparing JDBC mocks and values capture
		PreparedStatement statementMock = mock(PreparedStatement.class);
		when(statementMock.executeLargeBatch()).thenReturn(new long[] { 1 });
		Map<Column<Table, Object>, Object> capturedValues = new HashMap<>();
		when(connectionMock.prepareStatement(anyString())).thenReturn(statementMock);
		
		StringBuilder capturedSQL = new StringBuilder();
		((SimpleRelationalEntityPersister) (((PersisterWrapper) persister).getDeepestSurrogate())).getInsertExecutor().setOperationListener(new SQLOperationListener<Column<Table, Object>>() {
			@Override
			public void onValuesSet(Map<Column<Table, Object>, ?> values) {
				capturedValues.putAll(values);
			}
			
			@Override
			public void onExecute(SQLStatement<Column<Table, Object>> sqlStatement) {
				capturedSQL.append(sqlStatement.getSQL());
			}
		});
		
		// Testing ...
		persister.insert(country);
		
		assertThat(capturedSQL.toString()).isEqualTo("insert into countryTable(id, name, presidentElectedAt, presidentName) values (?, ?, ?, ?)");
		assertThat(capturedValues).isEqualTo(Maps.forHashMap(Column.class, Object.class)
				.add(columnsByName.get("presidentName"), country.getPresident().getName())
				.add(columnsByName.get("presidentElectedAt"), country.getPresident().getTimestamp().getModificationDate())
				.add(columnsByName.get("name"), country.getName())
				.add(columnsByName.get("id"), country.getId()));
	}
	
	@Test
	void innerEmbed_withTwiceSameInnerEmbeddableName() {
		Table<?> countryTable = new Table<>("countryTable");
		FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<Country, Identifier<Long>, Timestamp> mappingBuilder = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				.embed(Country::getPresident, MappingEase.embeddableBuilder(Person.class)
						.map(Person::getId)
						.map(Person::getName)
						.map(Person::getVersion)
						.embed(Person::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
								.map(Timestamp::getCreationDate)
								.map(Timestamp::getModificationDate)))
					.overrideName(Person::getId, "presidentId")
					.overrideName(Person::getName, "presidentName")
				// this embed will conflict with Country one because its type is already mapped with no override
				.embed(Country::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate));
		assertThatThrownBy(() -> mappingBuilder.build(persistenceContext, countryTable))
				.isInstanceOf(MappingConfigurationException.class)
				.hasMessage("Person::getTimestamp conflicts with Country::getTimestamp while embedding a o.c.s.e.m.Timestamp" +
						", column names should be overridden : Timestamp::getModificationDate, Timestamp::getCreationDate");
		
		// we add an override, exception must still be thrown, with different message
		mappingBuilder.overrideName(Timestamp::getModificationDate, "modifiedAt");
		
		assertThatThrownBy(() -> mappingBuilder.build(persistenceContext))
				.isInstanceOf(MappingConfigurationException.class)
				.hasMessage("Person::getTimestamp conflicts with Country::getTimestamp while embedding a o.c.s.e.m.Timestamp" +
						", column names should be overridden : Timestamp::getCreationDate");
		
		// we override the last field, no exception is thrown
		mappingBuilder.overrideName(Timestamp::getCreationDate, "createdAt");
		mappingBuilder.build(persistenceContext, countryTable);
		
		assertThat(countryTable.getColumns()).extracting(Column::getName).containsExactlyInAnyOrder(
				// from Country
				"id", "name",
				// from Person
				"presidentId", "version", "presidentName", "creationDate", "modificationDate",
				// from Country.timestamp
				"createdAt", "modifiedAt");
	}
	
	@Nested
	class EmbedWithExternalEmbeddedBean {
		
		@Test
		void simpleCase() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier> idColumn = totoTable.addColumn("id", UUID_TYPE);
			Column<Table, Date> creationDate = totoTable.addColumn("creationDate", Date.class);
			Column<Table, Date> modificationDate = totoTable.addColumn("modificationDate", Date.class);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			// embeddeable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = MappingEase.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate)
					.map(Timestamp::getModificationDate);
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.embed(Toto::getTimestamp, timestampMapping)
					.build(persistenceContext, totoTable);	// necessary to set table since we override Identifier binding
			
			// column should be correctly created
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto toto = new Toto();
			// this partial instantiation of Timestamp let us test its partial load too
			toto.setTimestamp(new Timestamp(Dates.nowAsDate(), null));
			persister.insert(toto);
			
			// Is everything fine in database ?
			List<Timestamp> select = persistenceContext.select(Timestamp::new, creationDate, modificationDate);
			assertThat(select.get(0)).isEqualTo(toto.getTimestamp());
			
			// Is loading is fine too ?
			Toto loadedToto = persister.select(toto.getId());
			assertThat(loadedToto.getTimestamp()).isEqualTo(toto.getTimestamp());
		}
		
		@Test
		void embed_withMappedSuperClass() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier> idColumn = totoTable.addColumn("id", UUID_TYPE);
			Column<Table, Date> creationDate = totoTable.addColumn("creationDate", Date.class);
			Column<Table, Date> modificationDate = totoTable.addColumn("modificationDate", Date.class);
			Column<Table, Locale> localeColumn = totoTable.addColumn("locale", Locale.class);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			dialect.getColumnBinderRegistry().register(Locale.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(
					(resultSet, columnName) -> Locale.forLanguageTag(resultSet.getString(columnName)),
					(preparedStatement, valueIndex, value) -> preparedStatement.setString(valueIndex, value.toLanguageTag()))));
			dialect.getSqlTypeRegistry().put(Locale.class, "VARCHAR(20)");
			
			// embeddeable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = MappingEase.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate)
					.map(Timestamp::getModificationDate);
			
			EmbeddableMappingConfigurationProvider<TimestampWithLocale> timestampWithLocaleMapping = MappingEase.embeddableBuilder(TimestampWithLocale.class)
					.map(TimestampWithLocale::getLocale)
					.mapSuperClass(timestampMapping);
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.embed(Toto::getTimestamp, timestampWithLocaleMapping)
					.build(persistenceContext, totoTable);	// necessary to set table since we override Identifier binding
			
			// column should be correctly created
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto toto = new Toto();
			// this partial instantiation of Timestamp let us test its partial load too
			toto.setTimestamp(new TimestampWithLocale(Dates.nowAsDate(), null, Locale.US));
			persister.insert(toto);
			
			// Is everything fine in database ?
			List<TimestampWithLocale> select = persistenceContext.select(TimestampWithLocale::new, creationDate, modificationDate, localeColumn);
			assertThat(select.get(0)).isEqualTo(toto.getTimestamp());
			
			// Is loading is fine too ?
			Toto loadedToto = persister.select(toto.getId());
			assertThat(loadedToto.getTimestamp()).isEqualTo(toto.getTimestamp());
		}
		
		@Test
		void embed_withMappedSuperClassAndOverride() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier> idColumn = totoTable.addColumn("id", UUID_TYPE);
			Column<Table, Date> creationDate = totoTable.addColumn("creationDate", Date.class);
			Column<Table, Date> modificationDate = totoTable.addColumn("modificationTime", Date.class);
			Column<Table, Locale> localeColumn = totoTable.addColumn("locale", Locale.class);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			dialect.getColumnBinderRegistry().register(Locale.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(
					(resultSet, columnName) -> Locale.forLanguageTag(resultSet.getString(columnName)),
					(preparedStatement, valueIndex, value) -> preparedStatement.setString(valueIndex, value.toLanguageTag()))));
			dialect.getSqlTypeRegistry().put(Locale.class, "VARCHAR(20)");
			
			// embeddeable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = MappingEase.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate)
					.map(Timestamp::getModificationDate);
			
			EmbeddableMappingConfigurationProvider<TimestampWithLocale> timestampWithLocaleMapping = MappingEase.embeddableBuilder(TimestampWithLocale.class)
					.map(TimestampWithLocale::getLocale)
					.mapSuperClass(timestampMapping);
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.embed(Toto::getTimestamp, timestampWithLocaleMapping)
						.override(Timestamp::getModificationDate, modificationDate)
					.build(persistenceContext, totoTable);
			
			// column should be correctly created
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto toto = new Toto();
			// this partial instantiation of Timestamp let us test its partial load too
			toto.setTimestamp(new TimestampWithLocale(Dates.nowAsDate(), null, Locale.US));
			persister.insert(toto);
			
			// Is everything fine in database ?
			List<TimestampWithLocale> select = persistenceContext.select(TimestampWithLocale::new, creationDate, modificationDate, localeColumn);
			assertThat(select.get(0)).isEqualTo(toto.getTimestamp());
			
			// Is loading is fine too ?
			Toto loadedToto = persister.select(toto.getId());
			assertThat(loadedToto.getTimestamp()).isEqualTo(toto.getTimestamp());
		}
		
		@Test
		void overrideName() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier> idColumn = totoTable.addColumn("id", UUID_TYPE);
			Column<Table, Date> creationDate = totoTable.addColumn("createdAt", Date.class);
			Column<Table, Date> modificationDate = totoTable.addColumn("modificationDate", Date.class);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = MappingEase.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate)
					.map(Timestamp::getModificationDate);
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.embed(Toto::getTimestamp, timestampMapping)
						.overrideName(Timestamp::getCreationDate, "createdAt")
					.build(persistenceContext, totoTable);	// necessary to set table since we override Identifier binding
			
			Map<String, Column> columnsByName = totoTable.mapColumnsOnName();
			
			// columns with getter name must be absent (hard to test: can be absent for many reasons !)
			assertThat(columnsByName.get("creationDate")).isNull();
			
			// column should be correctly created
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto toto = new Toto();
			// this partial instantiation of Timestamp let us test its partial load too
			toto.setTimestamp(new Timestamp(Dates.nowAsDate(), null));
			persister.insert(toto);
			
			// Is everything fine in database ?
			List<Timestamp> select = persistenceContext.select(Timestamp::new, creationDate, modificationDate);
			assertThat(select.get(0)).isEqualTo(toto.getTimestamp());
			
			// Is loading is fine too ?
			Toto loadedToto = persister.select(toto.getId());
			assertThat(loadedToto.getTimestamp()).isEqualTo(toto.getTimestamp());
		}
		
		@Test
		void overrideName_nameAlreadyExists_throwsException() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier> idColumn = totoTable.addColumn("id", UUID_TYPE);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			// embeddeable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = MappingEase.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate)
					.map(Timestamp::getModificationDate);
			
			assertThatThrownBy(() -> MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.embed(Toto::getTimestamp, timestampMapping)
					.overrideName(Timestamp::getCreationDate, "modificationDate")
					.build(persistenceContext))
					.isInstanceOf(MappingConfigurationException.class)
					.hasMessage("Column 'modificationDate' of mapping 'Timestamp::getCreationDate' is already targetted by 'Timestamp::getModificationDate'");
		}
		
		@Test
		void overrideName_nameIsAlreadyOverridden_nameIsOverwritten() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier> idColumn = totoTable.addColumn("id", UUID_TYPE);
			Column<Table, Date> creationDate = totoTable.addColumn("createdAt", Date.class);
			Column<Table, Date> modificationDate = totoTable.addColumn("modificationDate", Date.class);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			// embeddeable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = MappingEase.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate, "creation")
					.map(Timestamp::getModificationDate);
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.embed(Toto::getTimestamp, timestampMapping)
						.overrideName(Timestamp::getCreationDate, "createdAt")
					.build(persistenceContext, totoTable);	// necessary to set table since we override Identifier binding
			
			// column should be correctly created
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto toto = new Toto();
			// this partial instantiation of Timestamp let us test its partial load too
			toto.setTimestamp(new Timestamp(Dates.nowAsDate(), null));
			persister.insert(toto);
			
			// Is everything fine in database ?
			List<Timestamp> select = persistenceContext.select(Timestamp::new, creationDate, modificationDate);
			assertThat(select.get(0)).isEqualTo(toto.getTimestamp());
			
			// Is loading is fine too ?
			Toto loadedToto = persister.select(toto.getId());
			assertThat(loadedToto.getTimestamp()).isEqualTo(toto.getTimestamp());
		}
		
		@Test
		void mappingDefinedTwice_throwsException() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier> idColumn = totoTable.addColumn("id", UUID_TYPE);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			// embeddeable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = MappingEase.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate)
					.map(Timestamp::getModificationDate)
					.map(Timestamp::setModificationDate);
			
			assertThatThrownBy(() -> MappingEase.entityBuilder(Toto.class, UUID_TYPE)
						.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
						.map(Toto::getName)
						.embed(Toto::getTimestamp, timestampMapping)
						.build(persistenceContext))
					.isInstanceOf(MappingConfigurationException.class)
					.hasMessage("Mapping is already defined by method Timestamp::getModificationDate");
		}
		
		@Test
		void exclude() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier> idColumn = totoTable.addColumn("id", UUID_TYPE);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			// embeddeable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = MappingEase.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate, "creation")
					.map(Timestamp::getModificationDate);
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.embed(Toto::getTimestamp, timestampMapping)
						.exclude(Timestamp::getCreationDate)
					.build(persistenceContext, totoTable);	// necessary to set table since we override Identifier binding
			
			Map map = totoTable.mapColumnsOnName();
			assertThat(map.get("creationDate")).isNull();
			
			// column should be correctly created
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto toto = new Toto();
			// this partial instantiation of Timestamp let us test its partial load too
			toto.setTimestamp(new Timestamp(Dates.nowAsDate(), null));
			persister.insert(toto);
			
			// Is loading is fine too ?
			Toto loadedToto = persister.select(toto.getId());
			// timestamp is expected to be null because all columns in database are null, which proves that creationDate is not taken into account
			assertThat(loadedToto.getTimestamp()).isNull();
		}
		
		@Test
		void overrideColumn() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier> idColumn = totoTable.addColumn("id", UUID_TYPE);
			Column<Table, Date> creationDate = totoTable.addColumn("createdAt", Date.class);
			Column<Table, Date> modificationDate = totoTable.addColumn("modificationDate", Date.class);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			// embeddeable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = MappingEase.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate, "creation")
					.map(Timestamp::getModificationDate);
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.embed(Toto::getTimestamp, timestampMapping)
						.override(Timestamp::getCreationDate, creationDate)
					.build(persistenceContext, totoTable);
			
			Map map = totoTable.mapColumnsOnName();
			assertThat(map.get("creationDate")).isNull();
			
			/// column should be correctly created
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto toto = new Toto();
			// this partial instantiation of Timestamp let us test its partial load too
			toto.setTimestamp(new Timestamp(Dates.nowAsDate(), null));
			persister.insert(toto);
			
			// Is everything fine in database ?
			List<Timestamp> select = persistenceContext.select(Timestamp::new, creationDate, modificationDate);
			assertThat(select.get(0)).isEqualTo(toto.getTimestamp());
			
			// Is loading is fine too ?
			Toto loadedToto = persister.select(toto.getId());
			assertThat(loadedToto.getTimestamp()).isEqualTo(toto.getTimestamp());
		}
	}
	
	@Test
	void withEnum_byDefault_ordinalIsUsed() {
		ConfiguredPersister<PersonWithGender, Identifier<Long>> personPersister = (ConfiguredPersister<PersonWithGender, Identifier<Long>>) MappingEase.entityBuilder(PersonWithGender.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				// no type of mapping given: neither ordinal nor name
				.mapEnum(PersonWithGender::getGender)
				.build(persistenceContext);
		
		Column gender = (Column) personPersister.getMapping().getTargetTable().mapColumnsOnName().get("gender");
		dialect.getSqlTypeRegistry().put(gender, "VARCHAR(255)");
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		PersonWithGender person = new PersonWithGender(new PersistableIdentifier<>(1L));
		person.setName("toto");
		person.setGender(Gender.FEMALE);
		personPersister.insert(person);
		
		// checking that name was used
		List<Integer> result = persistenceContext.newQuery("select * from PersonWithGender", Integer.class)
				.mapKey(Integer::new, "gender", Integer.class)
				.execute();
		assertThat(result).isEqualTo(Arrays.asList(1));
	}
	
	@Test
	void addEnum_mandatory_onMissingValue_throwsException() {
		ConfiguredPersister<PersonWithGender, Identifier<Long>> personPersister = (ConfiguredPersister<PersonWithGender, Identifier<Long>>) MappingEase.entityBuilder(PersonWithGender.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				// no type of mapping given: neither ordinal nor name
				.mapEnum(PersonWithGender::getGender).mandatory()
				.build(persistenceContext);
		
		Column gender = (Column) personPersister.getMapping().getTargetTable().mapColumnsOnName().get("gender");
		dialect.getSqlTypeRegistry().put(gender, "VARCHAR(255)");
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		PersonWithGender person = new PersonWithGender(new PersistableIdentifier<>(1L));
		person.setName("toto");
		person.setGender(null);
		
		assertThatThrownBy(() -> personPersister.insert(person))
				.isInstanceOf(RuntimeException.class)
				.hasMessage("Error while inserting values for " + person + " in statement \"insert into PersonWithGender(gender, id, name) values (?, ?, ?)\"")
				.hasCause(new BindingException("Expected non null value for : PersonWithGender.gender"));
	}
	
	@Test
	void addEnum_mandatory_columnConstraintIsAdded() {
		ConfiguredPersister<PersonWithGender, Identifier<Long>> personPersister = (ConfiguredPersister<PersonWithGender, Identifier<Long>>) MappingEase.entityBuilder(PersonWithGender.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				// no type of mapping given: neither ordinal nor name
				.mapEnum(PersonWithGender::getGender).mandatory()
				.build(persistenceContext);
		
		assertThat(personPersister.getMapping().getTargetTable().getColumn("gender").isNullable()).isFalse();
	}
	
	@Test
	void withEnum_mappedWithOrdinal() {
		ConfiguredPersister<PersonWithGender, Identifier<Long>> personPersister = (ConfiguredPersister<PersonWithGender, Identifier<Long>>) MappingEase.entityBuilder(PersonWithGender.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapEnum(PersonWithGender::getGender).byOrdinal()
				.build(persistenceContext);
		
		Column gender = (Column) personPersister.getMapping().getTargetTable().mapColumnsOnName().get("gender");
		dialect.getSqlTypeRegistry().put(gender, "INT");
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		PersonWithGender person = new PersonWithGender(new PersistableIdentifier<>(1L));
		person.setName("toto");
		person.setGender(Gender.FEMALE);
		personPersister.insert(person);
		
		// checking that ordinal was used
		List<Integer> result = persistenceContext.newQuery("select * from PersonWithGender", Integer.class)
				.mapKey(Integer::valueOf, "gender", String.class)
				.execute();
		assertThat(result).isEqualTo(Arrays.asList(person.getGender().ordinal()));
	}
	
	@Test
	void withEnum_columnMappedWithOrdinal() {
		Table personTable = new Table<>("PersonWithGender");
		Column<Table, Gender> genderColumn = personTable.addColumn("gender", Gender.class);
		
		EntityPersister<PersonWithGender, Identifier<Long>> personPersister = MappingEase.entityBuilder(PersonWithGender.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapEnum(PersonWithGender::getGender, genderColumn).byOrdinal()
				.build(persistenceContext);
		
		dialect.getSqlTypeRegistry().put(genderColumn, "INT");
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		PersonWithGender person = new PersonWithGender(new PersistableIdentifier<>(1L));
		person.setName("toto");
		person.setGender(Gender.FEMALE);
		personPersister.insert(person);
		
		// checking that ordinal was used
		List<Integer> result = persistenceContext.newQuery("select * from PersonWithGender", Integer.class)
				.mapKey(Integer::valueOf, "gender", String.class)
				.execute();
		assertThat(result).isEqualTo(Arrays.asList(person.getGender().ordinal()));
	}
	
	@Test
	void insert() {
		ConfiguredPersister<PersonWithGender, Identifier<Long>> personPersister = (ConfiguredPersister<PersonWithGender, Identifier<Long>>) MappingEase.entityBuilder(PersonWithGender.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				// no type of mapping given: neither ordinal nor name
				.mapEnum(PersonWithGender::getGender)
				.build(persistenceContext);
		
		Column gender = (Column) personPersister.getMapping().getTargetTable().mapColumnsOnName().get("gender");
		dialect.getSqlTypeRegistry().put(gender, "VARCHAR(255)");
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		PersonWithGender person = new PersonWithGender(new PersistableIdentifier<>(1L));
		person.setName(null);
		person.setGender(Gender.FEMALE);
		
		personPersister.insert(person);
		
		PersonWithGender loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getGender()).isEqualTo(Gender.FEMALE);
	}
	
	@Test
	void insert_nullValues() {
		ConfiguredPersister<PersonWithGender, Identifier<Long>> personPersister = (ConfiguredPersister<PersonWithGender, Identifier<Long>>) MappingEase.entityBuilder(PersonWithGender.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				// no type of mapping given: neither ordinal nor name
				.mapEnum(PersonWithGender::getGender)
				.build(persistenceContext);
		
		Column gender = (Column) personPersister.getMapping().getTargetTable().mapColumnsOnName().get("gender");
		dialect.getSqlTypeRegistry().put(gender, "VARCHAR(255)");
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		PersonWithGender person = new PersonWithGender(new PersistableIdentifier<>(1L));
		person.setName(null);
		person.setGender(null);
		
		personPersister.insert(person);
		
		PersonWithGender loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getGender()).isEqualTo(null);
	}
	
	@Test
	void update_nullValues() {
		ConfiguredPersister<PersonWithGender, Identifier<Long>> personPersister = (ConfiguredPersister<PersonWithGender, Identifier<Long>>) MappingEase.entityBuilder(PersonWithGender.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				// no type of mapping given: neither ordinal nor name
				.mapEnum(PersonWithGender::getGender)
				.build(persistenceContext);
		
		Column gender = (Column) personPersister.getMapping().getTargetTable().mapColumnsOnName().get("gender");
		dialect.getSqlTypeRegistry().put(gender, "VARCHAR(255)");
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		PersonWithGender person = new PersonWithGender(new PersistableIdentifier<>(1L));
		person.setName("toto");
		person.setGender(Gender.MALE);
		
		personPersister.insert(person);
		
		PersonWithGender updatedPerson = new PersonWithGender(person.getId());
		personPersister.update(updatedPerson, person, true);
		PersonWithGender loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getGender()).isEqualTo(null);
	}
	
	@Nested
	class CollectionOfElements {
		
		@Test
		void insert() {
			ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapCollection(Person::getNicknames, String.class)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person person = new Person(new PersistableIdentifier<>(1L));
			person.setName("toto");
			person.initNicknames();
			person.addNickname("tonton");
			
			personPersister.insert(person);
			
			Person loadedPerson = personPersister.select(person.getId());
			assertThat(loadedPerson.getNicknames()).isEqualTo(Arrays.asSet("tonton"));
		}
		
		@Test
		void update_withNewObject() {
			ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapCollection(Person::getNicknames, String.class)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person person = new Person(new PersistableIdentifier<>(1L));
			person.setName("toto");
			person.initNicknames();
			person.addNickname("tonton");
			person.addNickname("tintin");
			
			personPersister.insert(person);
			
			Person loadedPerson = personPersister.select(person.getId());
			assertThat(loadedPerson.getNicknames()).isEqualTo(Arrays.asSet("tintin", "tonton"));
			
			
			loadedPerson.addNickname("toutou");
			personPersister.update(loadedPerson, person, true);
			loadedPerson = personPersister.select(person.getId());
			assertThat(loadedPerson.getNicknames()).isEqualTo(Arrays.asSet("tintin", "tonton", "toutou"));
		}
		
		@Test
		void update_objectRemoval() {
			ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapCollection(Person::getNicknames, String.class)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person person = new Person(new PersistableIdentifier<>(1L));
			person.setName("toto");
			person.initNicknames();
			person.addNickname("tonton");
			person.addNickname("tintin");
			
			personPersister.insert(person);
			
			Person loadedPerson = personPersister.select(person.getId());
			
			person.getNicknames().remove("tintin");
			personPersister.update(person, loadedPerson, true);
			loadedPerson = personPersister.select(person.getId());
			assertThat(loadedPerson.getNicknames()).isEqualTo(Arrays.asSet("tonton"));
		}
		
		@Test
		void delete() {
			ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapCollection(Person::getNicknames, String.class)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person person = new Person(new PersistableIdentifier<>(1L));
			person.setName("toto");
			person.initNicknames();
			person.addNickname("tonton");
			person.addNickname("tintin");
			
			personPersister.insert(person);
			
			Person loadedPerson = personPersister.select(person.getId());
			personPersister.delete(loadedPerson);
			List<String> remainingNickNames = persistenceContext.newQuery("select nickNames from Person_nicknames", String.class)
					.mapKey("nickNames", String.class)
					.execute();
			assertThat(remainingNickNames).isEqualTo(Collections.emptyList());
		}
		
		@Test
		void withCollectionFactory() {
			ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapCollection(Person::getNicknames, String.class)
						.withCollectionFactory(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER.reversed()))
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person person = new Person(new PersistableIdentifier<>(1L));
			person.setName("toto");
			person.initNicknames();
			person.addNickname("d");
			person.addNickname("a");
			person.addNickname("c");
			person.addNickname("b");
			
			// because nickNames is initialized with HashSet we get this order
			assertThat(person.getNicknames()).isEqualTo(Arrays.asSet("a", "b", "c", "d"));
			
			personPersister.insert(person);
			
			Person loadedPerson = personPersister.select(person.getId());
			// because nickNames is initialized with TreeSet with reversed order we get this order
			assertThat(loadedPerson.getNicknames()).isEqualTo(Arrays.asSet("d", "c", "b", "a"));
		}
		
		@Test
		void foreignKeyIsPresent() {
			MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapCollection(Person::getNicknames, String.class)
					.build(persistenceContext);
			
			Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
			Map<String, Table> tablePerName = Iterables.map(tables, Table::getName);
			Table personTable = tablePerName.get("Person");
			Table nickNamesTable = tablePerName.get("Person_nicknames");
			
			Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
					Column::getAbsoluteName,
					chain(Column::getJavaType, Reflections::toString));
			Function<ForeignKey, String> fkPrinter = ToStringBuilder.of(", ",
					ForeignKey::getName,
					link(ForeignKey::getColumns, ToStringBuilder.asSeveral(columnPrinter)),
					link(ForeignKey::getTargetColumns, ToStringBuilder.asSeveral(columnPrinter)));
			
			assertThat(nickNamesTable.getForeignKeys())
					.usingElementComparator(Comparator.comparing(fkPrinter))
					.containsExactly(new ForeignKey("FK_Person_nicknames_id_Person_id", nickNamesTable.getColumn("id"), personTable.getColumn("id")));
		}
		
		
		@Test
		void mappedBy() {
			MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapCollection(Person::getNicknames, String.class)
						.mappedBy("identifier")
					.build(persistenceContext);
			
			Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
			Map<String, Table> tablePerName = Iterables.map(tables, Table::getName);
			Table personTable = tablePerName.get("Person");
			Table nickNamesTable = tablePerName.get("Person_nicknames");
			assertThat(nickNamesTable).isNotNull();
			
			Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
					Column::getAbsoluteName,
					chain(Column::getJavaType, Reflections::toString));
			Function<ForeignKey, String> fkPrinter = ToStringBuilder.of(", ",
					ForeignKey::getName,
					link(ForeignKey::getColumns, ToStringBuilder.asSeveral(columnPrinter)),
					link(ForeignKey::getTargetColumns, ToStringBuilder.asSeveral(columnPrinter)));
			
			assertThat(nickNamesTable.getForeignKeys())
					.usingElementComparator(Comparator.comparing(fkPrinter))
					.containsExactly(new ForeignKey("FK_Person_nicknames_identifier_Person_id", nickNamesTable.getColumn("identifier"), personTable.getColumn("id")));
		}
		
		@Test
		void withTableNaming() {
			MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Person::getName)
					.withElementCollectionTableNaming(accessorDefinition -> "Toto")
					.mapCollection(Person::getNicknames, String.class)
					.build(persistenceContext);
			
			Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
			Map<String, Table> tablePerName = Iterables.map(tables, Table::getName);
			Table personTable = tablePerName.get("Person");
			Table nickNamesTable = tablePerName.get("Toto");
			assertThat(nickNamesTable).isNotNull();
			
			Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
					Column::getAbsoluteName,
					chain(Column::getJavaType, Reflections::toString));
			Function<ForeignKey, String> fkPrinter = ToStringBuilder.of(", ",
					ForeignKey::getName,
					link(ForeignKey::getColumns, ToStringBuilder.asSeveral(columnPrinter)),
					link(ForeignKey::getTargetColumns, ToStringBuilder.asSeveral(columnPrinter)));
			
			assertThat(nickNamesTable.getForeignKeys())
					.usingElementComparator(Comparator.comparing(fkPrinter))
					.containsExactly(new ForeignKey("FK_Toto_id_Person_id", nickNamesTable.getColumn("id"), personTable.getColumn("id")));
		}
		
		@Test
		void withTable() {
			Table nickNamesTable = new Table("Toto");
			
			MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapCollection(Person::getNicknames, String.class)
						.withTable(nickNamesTable)
					.build(persistenceContext);
			
			Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
			Map<String, Table> tablePerName = Iterables.map(tables, Table::getName);
			Table personTable = tablePerName.get("Person");
			
			Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
					Column::getAbsoluteName,
					chain(Column::getJavaType, Reflections::toString));
			Function<ForeignKey, String> fkPrinter = ToStringBuilder.of(", ",
					ForeignKey::getName,
					link(ForeignKey::getColumns, ToStringBuilder.asSeveral(columnPrinter)),
					link(ForeignKey::getTargetColumns, ToStringBuilder.asSeveral(columnPrinter)));
			
			assertThat(nickNamesTable.getForeignKeys())
					.usingElementComparator(Comparator.comparing(fkPrinter))
					.containsExactly(new ForeignKey("FK_Toto_id_Person_id", nickNamesTable.getColumn("id"), personTable.getColumn("id")));
		}
		
		@Test
		void withTableName() {
			MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapCollection(Person::getNicknames, String.class)
						.withTable("Toto")
					.build(persistenceContext);
			
			Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
			Map<String, Table> tablePerName = Iterables.map(tables, Table::getName);
			Table personTable = tablePerName.get("Person");
			Table nickNamesTable = tablePerName.get("Toto");
			assertThat(nickNamesTable).isNotNull();
			
			Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
					Column::getAbsoluteName,
					chain(Column::getJavaType, Reflections::toString));
			Function<ForeignKey, String> fkPrinter = ToStringBuilder.of(", ",
					ForeignKey::getName,
					link(ForeignKey::getColumns, ToStringBuilder.asSeveral(columnPrinter)),
					link(ForeignKey::getTargetColumns, ToStringBuilder.asSeveral(columnPrinter)));
			
			assertThat(nickNamesTable.getForeignKeys())
					.usingElementComparator(Comparator.comparing(fkPrinter))
					.containsExactly(new ForeignKey("FK_Toto_id_Person_id", nickNamesTable.getColumn("id"), personTable.getColumn("id")));
		}
		
		@Test
		void overrideColumnName() {
			MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapCollection(Person::getNicknames, String.class)
						.override("toto")
					.build(persistenceContext);
			
			Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
			Map<String, Table> tablePerName = Iterables.map(tables, Table::getName);
			Table personTable = tablePerName.get("Person");
			Table nickNamesTable = tablePerName.get("Person_nicknames");
			
			assertThat(nickNamesTable.mapColumnsOnName().keySet()).isEqualTo(Arrays.asHashSet("id", "toto"));
		}
		
		
		@Test
		void crudEnum() {
			Table totoTable = new Table("Toto");
			Column idColumn = totoTable.addColumn("id", UUID_TYPE);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			ConfiguredPersister<Toto, Identifier<UUID>> personPersister = (ConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.mapCollection(Toto::getPossibleStates, State.class)
					.build(persistenceContext, totoTable);	// necessary to set table since we override Identifier binding
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto person = new Toto();
			person.setName("toto");
			person.getPossibleStates().add(State.DONE);
			person.getPossibleStates().add(State.IN_PROGRESS);
			
			personPersister.insert(person);
			
			Toto loadedPerson = personPersister.select(person.getId());
			assertThat(loadedPerson.getPossibleStates()).isEqualTo(Arrays.asSet(State.DONE, State.IN_PROGRESS));
		}
		
		@Test
		void crudComplexType() {
			Table totoTable = new Table("Toto");
			Column idColumn = totoTable.addColumn("id", UUID_TYPE);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			ConfiguredPersister<Toto, Identifier<UUID>> personPersister = (ConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.mapCollection(Toto::getTimes, Timestamp.class, MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
					.build(persistenceContext, totoTable);	// necessary to set table since we override Identifier binding
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto person = new Toto();
			person.setName("toto");
			Timestamp timestamp1 = new Timestamp(LocalDateTime.now().plusDays(1), LocalDateTime.now().plusDays(1));
			Timestamp timestamp2 = new Timestamp(LocalDateTime.now().plusDays(2), LocalDateTime.now().plusDays(2));
			person.getTimes().add(timestamp1);
			person.getTimes().add(timestamp2);
			
			personPersister.insert(person);
			
			Toto loadedPerson = personPersister.select(person.getId());
			assertThat(loadedPerson.getTimes()).isEqualTo(Arrays.asSet(timestamp1, timestamp2));
		}
		
		@Test
		void crudComplexType_overrideColumnName() {
			Table totoTable = new Table("toto");
			Column idColumn = totoTable.addColumn("id", UUID_TYPE);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			ConfiguredPersister<Toto, Identifier<UUID>> personPersister = (ConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.mapCollection(Toto::getTimes, Timestamp.class, MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
					.overrideName(Timestamp::getCreationDate, "createdAt")
					.build(persistenceContext, totoTable);	// necessary to set table since we override Identifier binding
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
			Map<String, Table> tablePerName = Iterables.map(tables, Table::getName);
			Table totoTimesTable = tablePerName.get("Toto_times");
			Map<String, Column> timesTableColumn = totoTimesTable.mapColumnsOnName();
			assertThat(timesTableColumn.get("createdAt")).isNotNull();
			
			Toto person = new Toto();
			person.setName("toto");
			Timestamp timestamp1 = new Timestamp(LocalDateTime.now().plusDays(1), LocalDateTime.now().plusDays(1));
			Timestamp timestamp2 = new Timestamp(LocalDateTime.now().plusDays(2), LocalDateTime.now().plusDays(2));
			person.getTimes().add(timestamp1);
			person.getTimes().add(timestamp2);
			
			personPersister.insert(person);
			
			Toto loadedPerson = personPersister.select(person.getId());
			assertThat(loadedPerson.getTimes()).isEqualTo(Arrays.asSet(timestamp1, timestamp2));
		}
	}
	
	/**
	 * Test to check that the API returns right Object which means:
	 * - interfaces are well written to return right types, so one can chain others methods
	 * - at runtime instance of the right type is also returned
	 * (avoid "java.lang.ClassCastException: com.sun.proxy.$Proxy10 cannot be cast to org.codefilarete.stalactite.engine.FluentEmbeddableMappingBuilder")
	 * <p>
	 * As many as possible combinations of method chaining should be done here, because all combination seems impossible, this test must be
	 * considered as best effort, and any regression found in user code should be added here
	 */
	@Test
	void apiUsage() {
		try {
			MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.embed(Country::getPresident, MappingEase.embeddableBuilder(Person.class)
							.map(Person::getName))
					.overrideName(Person::getId, "personId")
					.overrideName(Person::getName, "personName")
					.embed(Country::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.map(Country::getId)
					.map(Country::setDescription, "zxx")
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Country.class))
					.build(persistenceContext);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.embed(Country::getPresident, MappingEase.embeddableBuilder(Person.class)
							.map(Person::getName))
					.embed(Country::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.map(Country::getId, "zz")
					.mapOneToOne(Country::getPresident, MappingEase.entityBuilder(Person.class, long.class))
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Country.class))
					.map(Country::getDescription, "xx")
					.build(persistenceContext);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			MappingEase.entityBuilder(Country.class, long.class)
					.map(Country::getName)
					.map(Country::getId, "zz")
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Country.class))
					// embed with setter
					.embed(Country::setPresident, MappingEase.embeddableBuilder(Person.class)
							.map(Person::getName))
					// embed with setter
					.embed(Country::setTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.mapOneToManySet(Country::getCities, MappingEase.entityBuilder(City.class, long.class))
						// testing mappedBy with inheritance
						.mappedBy((SerializableFunction<City, AbstractCountry>) City::getAbstractCountry)
					.map(Country::getDescription, "xx")
					.map(Country::getDummyProperty, "dd")
					.build(persistenceContext);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			EmbeddableMappingConfigurationProvider<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
					.map(Person::getName);
			
			MappingEase.entityBuilder(Country.class, long.class)
					.map(Country::getName)
					.map(Country::getId, "zz")
					.mapOneToOne(Country::setPresident, MappingEase.entityBuilder(Person.class, long.class))
					.mapSuperClass((EmbeddableMappingConfigurationProvider<Country>) new FluentEmbeddableMappingConfigurationSupport<>(Country.class))
					// embed with setter
					.embed(Country::getPresident, personMappingBuilder)
					.build(persistenceContext);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			EmbeddableMappingConfigurationProvider<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
					.map(Person::getName);
			
			MappingEase.entityBuilder(Country.class, long.class)
					.map(Country::getName)
					.map(Country::getId, "zz")
					.embed(Country::getPresident, personMappingBuilder)
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Country.class))
					.mapOneToOne(Country::setPresident, MappingEase.entityBuilder(Person.class, long.class))
					// reusing embeddable ...
					.embed(Country::getPresident, personMappingBuilder)
					// with getter override
					.overrideName(Person::getName, "toto")
					// with setter override
					.overrideName(Person::setName, "tata")
					.build(persistenceContext);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		class PersonTable extends Table<PersonTable> {
			
			Column<PersonTable, Gender> gender = addColumn("gender", Gender.class);
			Column<PersonTable, String> name = addColumn("name", String.class);
			
			PersonTable() {
				super("Person");
			}
		}
		PersonTable personTable = new PersonTable();
		try {
			MappingEase.entityBuilder(PersonWithGender.class, long.class)
					.map(Person::getName)
					.map(Person::getName, personTable.name)
					.mapEnum(PersonWithGender::getGender).byOrdinal()
					.embed(Person::setTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.overrideName(Timestamp::getCreationDate, "myDate")
					.mapEnum(PersonWithGender::getGender, "MM").byOrdinal()
					.mapEnum(PersonWithGender::getGender, personTable.gender).byOrdinal()
					.map(PersonWithGender::getId, "zz")
					.mapEnum(PersonWithGender::setGender).byName()
					.embed(Person::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.mapEnum(PersonWithGender::setGender, "MM").byName()
					.build(persistenceContext, new Table<>("person"));
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
	}
	
	protected static class Toto implements Identified<UUID> {
		
		public static Toto newInstance() {
			Toto newInstance = new Toto();
			newInstance.firstName = "set by static factory";
			return newInstance;
		}
		
		private final Identifier<UUID> id;
		
		private Identifier<UUID> identifier;
		
		private String name;
		
		private String firstName;
		
		private Timestamp timestamp;
		
		private Set<State> possibleStates = new HashSet<>();
		
		private Set<Timestamp> times = new HashSet<>();
		
		private boolean setIdWasCalled;
		private boolean constructorWithIdWasCalled;
		private boolean constructorWith2ArgsWasCalled;
		
		public Toto() {
			id = new PersistableIdentifier<>(UUID.randomUUID());
		}
		
		public Toto(PersistedIdentifier<UUID> id) {
			this.id = id;
			this.constructorWithIdWasCalled = true;
		}
		
		public Toto(PersistedIdentifier<UUID> id, String name) {
			this.id = id;
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
		
		public Identifier<UUID> getIdentifier() {
			return identifier;
		}
		
		public void setIdentifier(Identifier<UUID> id) {
			this.identifier = id;
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
	
	private enum State {
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
