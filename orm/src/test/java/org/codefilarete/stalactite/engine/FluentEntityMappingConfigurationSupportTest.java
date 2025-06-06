package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.FluentEntityMappingBuilder.FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions;
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
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.PersisterWrapper;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter.LambdaPreparedStatementWriter;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader.LambdaResultSetReader;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.stalactite.test.DefaultDialect;
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
import static org.codefilarete.stalactite.query.model.QueryEase.where;
import static org.codefilarete.tool.collection.Iterables.first;
import static org.codefilarete.tool.collection.Iterables.map;
import static org.codefilarete.tool.function.Functions.chain;
import static org.codefilarete.tool.function.Functions.link;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class FluentEntityMappingConfigurationSupportTest {
	
	private static final Class<Identifier<UUID>> UUID_TYPE = (Class) Identifier.class;
	
	// NB: dialect is made non static because we register binder for the same column several times in these tests
	// and this is not supported : the very first one takes priority  
	private final Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
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
		FluentEntityMappingBuilder<Toto, Identifier> mappingStrategy = MappingEase.entityBuilder(Toto.class, Identifier.class)
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
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
					.mapKey(Toto::getId, IdentifierPolicy.<Toto, Identifier<UUID>>alreadyAssigned(c -> c.getId().setPersisted(), c -> c.getId().isPersisted()))
					.map(Toto::getName)
					.build(persistenceContext);	// necessary to set table since we override Identifier binding
			
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
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
					.mapKey(Toto::getId, IdentifierPolicy.<Toto, Identifier<UUID>>alreadyAssigned(c -> c.getId().setPersisted(), c -> c.getId().isPersisted()))
						.usingConstructor(constructor)
					.map(Toto::getName)
					.build(persistenceContext);	// necessary to set table since we override Identifier binding
			
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
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, (Class<Identifier<UUID>>) (Class) UUID_TYPE, totoTable)
					.mapKey(Toto::getId, IdentifierPolicy.<Toto, Identifier<UUID>>alreadyAssigned(c -> c.getId().setPersisted(), c -> c.getId().isPersisted()))
						.usingConstructor(constructor)
					.map(Toto::getName)
					.build(persistenceContext);
			
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
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, (Class<Identifier<UUID>>) (Class) UUID_TYPE, totoTable)
					.mapKey(Toto::getId, IdentifierPolicy.<Toto, Identifier<UUID>>alreadyAssigned(c -> c.getId().setPersisted(), c -> c.getId().isPersisted()))
						.usingConstructor(constructor, idColumn)
					.map(Toto::getName)
					.build(persistenceContext);
			
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
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, (Class<Identifier<UUID>>) (Class) UUID_TYPE, totoTable)
					.mapKey(Toto::getId, IdentifierPolicy.<Toto, Identifier<UUID>>alreadyAssigned(c -> c.getId().setPersisted(), c -> c.getId().isPersisted()))
						.usingConstructor(constructor, "myId")
					.map(Toto::getName)
					.build(persistenceContext);
			
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
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, (Class<Identifier<UUID>>) (Class) UUID_TYPE, totoTable)
					.mapKey(Toto::getId, IdentifierPolicy.<Toto, Identifier<UUID>>alreadyAssigned(c -> c.getId().setPersisted(), c -> c.getId().isPersisted()))
						.usingConstructor(constructor, idColumn, nameColumn)
					.map(Toto::getName).setByConstructor()	// avoiding superfluous property setting
					.build(persistenceContext);
			
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
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
					.mapKey(Toto::getId, IdentifierPolicy.<Toto, Identifier<UUID>>alreadyAssigned(c -> c.getId().setPersisted(), c -> c.getId().isPersisted()))
					.usingFactory(row ->
						new Toto((PersistedIdentifier<UUID>) row.apply(idColumn), (String) row.apply(nameColumn))
					)
					.map(Toto::getName).setByConstructor()	// avoiding superfluous property setting
					.build(persistenceContext);
			
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
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.build(persistenceContext);	// necessary to set table since we override Identifier binding
			
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
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.usingConstructor(constructor, idColumn)
					.map(Toto::getName)
					.build(persistenceContext);
			
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
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED, "identifier")
					.usingConstructor(constructor, "identifier")
					.map(Toto::getName).columnName("label")
					.build(persistenceContext);
			
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
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.usingConstructor(constructor, idColumn)
					.map(Toto::getName)
					.build(persistenceContext);
			
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
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.usingConstructor(constructor, idColumn, nameColumn)
					.map(Toto::getName).setByConstructor()
					.build(persistenceContext);
			
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
	void map_withoutName_targetedPropertyNameIsTaken() {
		ConfiguredRelationalPersister<Toto, Identifier<UUID>> persister = (ConfiguredRelationalPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getName)
				.build(persistenceContext);
		
		// column should be correctly created
		assertThat(persister.getMapping().getTargetTable().getName()).isEqualTo("Toto");
		Column columnForProperty = persister.getMapping().getTargetTable().mapColumnsOnName().get("name");
		assertThat(columnForProperty).isNotNull();
		assertThat(columnForProperty.getJavaType()).isEqualTo(String.class);
	}
	
	@Test
	void map_withColumnName_targetedPropertyNameIsTaken() {
		ConfiguredRelationalPersister<Toto, Identifier<UUID>> persister = (ConfiguredRelationalPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getName).columnName("dummyName")
				.build(persistenceContext);
		
		// column should be correctly created
		assertThat(persister.getMapping().getTargetTable().getName()).isEqualTo("Toto");
		Column columnForProperty = persister.getMapping().getTargetTable().mapColumnsOnName().get("dummyName");
		assertThat(columnForProperty).isNotNull();
		assertThat(columnForProperty.getJavaType()).isEqualTo(String.class);
	}
	
	@Test
	void map_withFieldName_targetedPropertyNameIsTaken() {
		ConfiguredRelationalPersister<Toto, Identifier<UUID>> persister = (ConfiguredRelationalPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getName).fieldName("firstName")
				.build(persistenceContext);
		
		// column should be correctly created
		assertThat(persister.getMapping().getTargetTable().getName()).isEqualTo("Toto");
		Column columnForProperty = persister.getMapping().getTargetTable().mapColumnsOnName().get("firstName");
		assertThat(columnForProperty).isNotNull();
		assertThat(columnForProperty.getJavaType()).isEqualTo(String.class);
	}
	
	@Test
	void map_mandatory_onMissingValue_throwsException() {
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
	void map_mandatory_columnConstraintIsAdded() {
		ConfiguredRelationalPersister<Toto, Identifier<UUID>> totoPersister = (ConfiguredRelationalPersister<Toto, Identifier<UUID>>)
				MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getName).mandatory()
				.build(persistenceContext);
		
		assertThat(totoPersister.getMapping().getTargetTable().getColumn("name").isNullable()).isFalse();
	}
	
	@Test
	void map_readonly_columnIsNotWrittenToDatabase() {
		Table totoTable = new Table("Toto");
		Column<Table, Identifier<UUID>> idColumn = totoTable.addColumn("id", Identifier.class);
		Column<Table, String> nameColumn = totoTable.addColumn("name", String.class);
		dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
		
		ConfiguredRelationalPersister<Toto, Identifier<UUID>> persister = (ConfiguredRelationalPersister<Toto, Identifier<UUID>>)
				MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getName).readonly()
				.map(Toto::getFirstName)
				.build(persistenceContext);
		
		// Checking that configuration is done right
		assertThat(persister.getMapping().getSelectableColumns().stream().map(Column::getName).collect(Collectors.toSet())).isEqualTo(Arrays.asSet("id", "name", "firstName"));
		assertThat(persister.getMapping().getInsertableColumns().stream().map(Column::getName).collect(Collectors.toSet())).isEqualTo(Arrays.asSet("id", "firstName"));
		assertThat(persister.getMapping().getUpdatableColumns().stream().map(Column::getName).collect(Collectors.toSet())).isEqualTo(Arrays.asSet("firstName"));
		
		// Testing in real conditions
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		UUID uuid = UUID.randomUUID();
		Toto toto = new Toto(new PersistedIdentifier<>(uuid));
		toto.setName("dummyName");
		persister.insert(toto);
		
		// insert should not write data
		List<Duo<Identifier<UUID>, String>> totoNames = persistenceContext.select(Duo::new, idColumn, nameColumn);
		assertThat(totoNames).containsExactly(new Duo<>(toto.getId(), null));
		
		// testing that select fill the property (the property is not totally ignored) 
		persistenceContext.update(totoTable, where(idColumn, Operators.eq(toto.getId()))).set(nameColumn, "dummyName").execute();
		totoNames = persistenceContext.select(Duo::new, idColumn, nameColumn);
		assertThat(totoNames).containsExactly(new Duo<>(toto.getId(), "dummyName"));
	}
	
	@Test
	void map_readonly_columnIsNotWrittenToDatabase_CRUD() {
		Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "varchar(36)");
		
		PersistenceContext persistenceContext = new PersistenceContext(dataSource, dialect);
		
		EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getName).readonly()
				.map(Toto::getFirstName)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Toto toto = new Toto(new PersistedIdentifier<>(UUID.randomUUID()));
		toto.setName("dummy value");
		
		persister.insert(toto);
		
		String firstName = persistenceContext.newQuery("select firstName from Toto", String.class)
				.mapKey("firstName", String.class)
				.execute(Accumulators.getFirst());
		assertThat(firstName).isNull();
		
		toto.setName("updated dummy value");
		persister.update(toto);
		
		firstName = persistenceContext.newQuery("select firstName from Toto", String.class)
				.mapKey("firstName", String.class)
				.execute(Accumulators.getFirst());
		assertThat(firstName).isNull();
	}
	
	@Test
	void map_withColumn_columnIsUsed() {
		Table toto = new Table("Toto");
		Column<Table, String> titleColumn = toto.addColumn("title", String.class);
		ConfiguredPersister<Toto, Identifier<UUID>> persister = (ConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getName).column(titleColumn)
				.build(persistenceContext);
		
		// column should not have been created
		Column columnForProperty = persister.getMapping().getTargetTable().mapColumnsOnName().get("name");
		assertThat(columnForProperty).isNull();
		
		// title column is expected to be added to the mapping and participate to DML actions 
		assertThat(persister.getMapping().getInsertableColumns().stream().map(Column::getName).collect(Collectors.toSet())).isEqualTo(Arrays.asSet("id", "title"));
		assertThat(persister.getMapping().getUpdatableColumns().stream().map(Column::getName).collect(Collectors.toSet())).isEqualTo(Arrays.asSet("title"));
	}
	
	@Test
	void map_withReadConverter_converterIsUsed() {
		Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		
		Table totoTable = new Table("Toto");
		Column<?, UUID> id = totoTable.addColumn("id", UUID_TYPE).primaryKey();
		Column<?, String> name = totoTable.addColumn("name", String.class);
		// binder creation for our identifier
		dialect.getColumnBinderRegistry().register(id, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(id, "varchar(255)");
		
		ConfiguredPersister<Toto, Identifier<UUID>> persister = (ConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getName).readConverter(String::toUpperCase)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Toto toto = new Toto();
		toto.setName("toto");
		persister.persist(toto);
		
		Toto select = persister.select(toto.getId());
		
		assertThat(select.getName()).isEqualTo("TOTO");
		// database value is still in lower case
		assertThat(persistenceContext.select(SerializableFunction.identity(), name)).containsExactly("toto");
	}
	
	@Test
	void map_withReadConverter_converterIsUsed_readOnlyProperty() {
		Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		
		Table totoTable = new Table("Toto");
		Column<Table, Identifier<UUID>> id = totoTable.addColumn("id", UUID_TYPE).primaryKey();
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		// binder creation for our identifier
		dialect.getColumnBinderRegistry().register(id, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(id, "varchar(255)");
		
		ConfiguredPersister<Toto, Identifier<UUID>> persister = (ConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getName).readonly().readConverter(String::toUpperCase)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		PersistedIdentifier<UUID> identifier = new PersistedIdentifier<>(UUID.randomUUID());
		persistenceContext.insert(totoTable).set(name, "toto").set(id, identifier).execute();
		
		Toto select = persister.select(identifier);
		
		assertThat(select.getName()).isEqualTo("TOTO");
		// database value is still in lower case
		assertThat(persistenceContext.select(SerializableFunction.identity(), name)).containsExactly("toto");
	}
	
	@Test
	void map_withWriteConverter_converterIsUsed() {
		Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		
		Table totoTable = new Table("Toto");
		Column id = totoTable.addColumn("id", UUID_TYPE).primaryKey();
		Column name = totoTable.addColumn("name", String.class);
		// binder creation for our identifier
		dialect.getColumnBinderRegistry().register(id, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(id, "varchar(255)");
		
		ConfiguredPersister<Toto, Identifier<UUID>> persister = (ConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.usingConstructor(uuidIdentifier -> new Toto((PersistedIdentifier<UUID>) uuidIdentifier))
				.map(Toto::getName).writeConverter(String::toUpperCase)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Toto toto = new Toto();
		toto.setName("toto");
		persister.persist(toto);
		
		Toto select = persister.select(toto.getId());
		
		assertThat(select.getName()).isEqualTo("TOTO");
		
		toto.setName("titi");
		persister.update(toto, select, true);
		
		select = persister.select(toto.getId());
		assertThat(select.getName()).isEqualTo("TITI");
		// database value is in upper case
		assertThat(persistenceContext.select(SerializableFunction.identity(), name)).containsExactly(new Object[] { "TITI" });
	}
	
	@Test
	void map_withSqlBinder_binderIsUsed_readCase() {
		Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		
		Table totoTable = new Table("Toto");
		Column id = totoTable.addColumn("id", UUID_TYPE).primaryKey();
		Column name = totoTable.addColumn("name", String.class);
		// binder creation for our identifier
		dialect.getColumnBinderRegistry().register(id, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(id, "varchar(255)");
		
		ConfiguredPersister<Toto, Identifier<UUID>> persister = (ConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				// upper casing data coming from database
				.map(Toto::getName).sqlBinder(new NullAwareParameterBinder<>(new LambdaResultSetReader<>(
						(resultSet, columnName) -> resultSet.getString(columnName).toUpperCase(), String.class), DefaultParameterBinders.STRING_BINDER))
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Toto toto = new Toto();
		toto.setName("toto");
		persister.persist(toto);
		
		Toto select = persister.select(toto.getId());
		
		assertThat(select.getName()).isEqualTo("TOTO");
	}
	
	@Test
	void map_withSqlBinder_binderIsUsed_writeCase() {
		Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		dialect.getSqlTypeRegistry().getJavaTypeToSqlTypeMapping().put(Set.class, "varchar(255)");
		
		Table totoTable = new Table("Toto");
		Column id = totoTable.addColumn("id", UUID_TYPE).primaryKey();
		// binder creation for our identifier
		dialect.getColumnBinderRegistry().register(id, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(id, "varchar(255)");
		
		ConfiguredPersister<Toto, Identifier<UUID>> persister = (ConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				// upper casing data going to database
				.map(Toto::getPossibleStates)
				// getPossibleStates is (de)serialized as a String
				.sqlBinder(new NullAwareParameterBinder<>(
						// converting String to a Set by parsing it
						DefaultParameterBinders.STRING_BINDER.thenApply(setAsString -> {
							String[] databaseValue = setAsString.substring(1, setAsString.length() - 1).split(", ");
							return EnumSet.copyOf(Stream.of(databaseValue).map(State::valueOf).collect(Collectors.toSet()));
						}),
						// writing Set as a String
						DefaultParameterBinders.STRING_BINDER.preApply(Set::toString)))
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Toto toto = new Toto();
		toto.setPossibleStates(EnumSet.of(State.DONE, State.IN_PROGRESS));
		persister.insert(toto);
		
		Toto select = persister.select(toto.getId());
		
		assertThat(select.getPossibleStates()).isEqualTo(Arrays.asSet(State.DONE, State.IN_PROGRESS));
	}
	
	@Test
	void map_withSqlBinderOnEmbeddedProperty_binderIsUsed_writeCase() {
		Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		dialect.getSqlTypeRegistry().getJavaTypeToSqlTypeMapping().put(Set.class, "varchar(255)");
		
		Table totoTable = new Table("Toto");
		Column id = totoTable.addColumn("id", UUID_TYPE).primaryKey();
		// this column may conflict by its name and type with mapping definition below, if the test is green, then
		// it means we handle it correctly down the code  
		Column creationDate = totoTable.addColumn("creationDate", String.class);
		// binder creation for our identifier
		dialect.getColumnBinderRegistry().register(id, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(id, "varchar(255)");
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		ConfiguredPersister<Toto, Identifier<UUID>> persister = (ConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				// we remove hours from the date while writing to database
				.embed(Toto::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate).sqlBinder(new NullAwareParameterBinder<>(
								// converting String to a Set by parsing it
								DefaultParameterBinders.STRING_BINDER.thenApply(sdf::parse),
								// writing Date as a String
								DefaultParameterBinders.STRING_BINDER.preApply(sdf::format)))
						.map(Timestamp::getModificationDate))
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Toto toto = new Toto();
		toto.setPossibleStates(EnumSet.of(State.DONE, State.IN_PROGRESS));
		Timestamp timestamp = new Timestamp();
		toto.setTimestamp(timestamp);
		persister.insert(toto);
		
		Toto select = persister.select(toto.getId());
		
		// Expected Timestamp is "today" on creation date, and current moment for modification date since the sql binder
		// removes hour + minute part only on creation date
		Date toDayAsDate = Date.from(Dates.today().atStartOfDay(ZoneId.systemDefault()).toInstant());
		Timestamp expected = new Timestamp(toDayAsDate, timestamp.getModificationDate());
		assertThat(select.getTimestamp()).isEqualTo(expected);
	}
	
	@Test
	void map_definedAsIdentifier_identifierIsStoredAsString() {
		Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		
		Table totoTable = new Table("Toto");
		Column id = totoTable.addColumn("id", UUID_TYPE).primaryKey();
		Column name = totoTable.addColumn("name", String.class);
		// binder creation for our identifier
		dialect.getColumnBinderRegistry().register(id, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(id, "varchar(255)");
		
		EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.build(persistenceContext);
		// column should be correctly created
		assertThat(totoTable.getColumn("id").isPrimaryKey()).isTrue();
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Toto toto = new Toto();
		toto.setName("toto");
		persister.persist(toto);
		
		List<Duo> select = persistenceContext.select(Duo::new, id, name);
		assertThat(select.size()).isEqualTo(1);
		assertThat(((Identifier) first(select).getLeft()).getDelegate().toString()).isEqualTo(toto.getId().getDelegate().toString());
	}
	
	@Test
	void map_mappingDefinedTwiceByMethod_throwsException() {
		assertThatThrownBy(() -> MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.map(Toto::setName)
					.build(persistenceContext))
				.isInstanceOf(MappingConfigurationException.class)
				.hasMessage("Mapping is already defined by method Toto::getName");
	}
	
	@Test
	void map_mappingDefinedTwiceByColumn_throwsException() {
		assertThatThrownBy(() -> MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName).columnName("xyz")
					.map(Toto::getFirstName).columnName("xyz")
					.build(persistenceContext))
				.isInstanceOf(MappingConfigurationException.class)
				.hasMessage("Column 'xyz' of mapping 'Toto::getName' is already targeted by 'Toto::getFirstName'");
	}
	
	@Test
	void map_methodHasNoMatchingField_configurationIsStillValid() {
		Table toto = new Table("Toto");
		MappingEase.entityBuilder(Toto.class, UUID_TYPE, toto)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getNoMatchingField)
				.build(persistenceContext);
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("noMatchingField");
		assertThat(columnForProperty).isNotNull();
		assertThat(columnForProperty.getJavaType()).isEqualTo(Long.class);
	}
	
	@Test
	void map_methodIsASetter_configurationIsStillValid() {
		Table toto = new Table("Toto");
		MappingEase.entityBuilder(Toto.class, UUID_TYPE, toto)
				.mapKey(Toto::setId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.build(persistenceContext);
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("id");
		assertThat(columnForProperty).isNotNull();
		assertThat(columnForProperty.getJavaType()).isEqualTo(UUID_TYPE);
	}
	
	@Nested
	class ExtraTable {
		
		@Test
		void tableStructure() {
			dialect.getColumnBinderRegistry().register(
					(Class<Identifier<UUID>>) (Class) Identifier.class,
					new NullAwareParameterBinder<>(new LambdaParameterBinder<>(DefaultParameterBinders.UUID_BINDER, PersistedIdentifier::new, StatefulIdentifier::getDelegate)));
			dialect.getSqlTypeRegistry().put(Identifier.class, "VARCHAR(255)");
			
			ConfiguredRelationalPersister<Toto, Identifier<UUID>> persister = (ConfiguredRelationalPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getIdentifier, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.extraTableName("Tata")
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// column should be correctly created
			Map<String, Table<?>> tablePerName = map(persister.getEntityJoinTree().giveTables(), Table::getName);
			assertThat(tablePerName.keySet()).containsExactlyInAnyOrder("Toto", "Tata");
			Table<?> totoTable = tablePerName.get("Toto");
			Table<?> tataTable = tablePerName.get("Tata");
			Column columnForProperty = tataTable.mapColumnsOnName().get("name");
			assertThat(columnForProperty).isNotNull();
			assertThat(columnForProperty.getJavaType()).isEqualTo(String.class);
			
			Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
					Column::getAbsoluteName,
					chain(Column::getJavaType, Reflections::toString));
			Function<ForeignKey, String> fkPrinter = ToStringBuilder.of(", ",
					ForeignKey::getName,
					link(ForeignKey::getColumns, ToStringBuilder.asSeveral(columnPrinter)),
					link(ForeignKey::getTargetColumns, ToStringBuilder.asSeveral(columnPrinter)));
			
			assertThat(tataTable.getForeignKeys())
					.usingElementComparator(Comparator.comparing(fkPrinter))
					.containsExactly(new ForeignKey("FK_Tata_identifier_Toto_identifier", tataTable.getColumn("identifier"), totoTable.getColumn("identifier")));
		}
		
		@Test
		void tableStructure_columnNameGiven_columnNameIsUsed() {
			dialect.getColumnBinderRegistry().register(
					(Class<Identifier<UUID>>) (Class) Identifier.class,
					new NullAwareParameterBinder<>(new LambdaParameterBinder<>(DefaultParameterBinders.UUID_BINDER, PersistedIdentifier::new, StatefulIdentifier::getDelegate)));
			dialect.getSqlTypeRegistry().put(Identifier.class, "VARCHAR(255)");
			
			ConfiguredRelationalPersister<Toto, Identifier<UUID>> persister = (ConfiguredRelationalPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getIdentifier, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName).columnName("dummyName")
					.extraTableName("Tata")
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// column should be correctly created
			assertThat(persister.getEntityJoinTree().giveTables()).extracting(Table::getName).containsExactlyInAnyOrder("Toto", "Tata");
			Table<?> tataTable = Iterables.find(persister.getEntityJoinTree().giveTables(), table -> table.getName().equals("Tata"));
			Column columnForProperty = tataTable.mapColumnsOnName().get("dummyName");
			assertThat(columnForProperty).isNotNull();
			assertThat(columnForProperty.getJavaType()).isEqualTo(String.class);
		}
		
		@Test
		void crud() {
			dialect.getColumnBinderRegistry().register(
					(Class<Identifier<UUID>>) (Class) Identifier.class,
					new NullAwareParameterBinder<>(new LambdaParameterBinder<>(DefaultParameterBinders.UUID_BINDER, PersistedIdentifier::new, StatefulIdentifier::getDelegate)));
			dialect.getSqlTypeRegistry().put(Identifier.class, "VARCHAR(255)");
			
			ConfiguredRelationalPersister<Toto, Identifier<UUID>> persister = (ConfiguredRelationalPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getIdentifier, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.extraTableName("Tata")
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto toto = new Toto();
			toto.setIdentifier(new PersistableIdentifier<>(UUID.randomUUID()));
			toto.setName("dummy value");
			persister.insert(toto);
			
			Toto selectedToto = persister.select(toto.getIdentifier());
			assertThat(selectedToto.getName()).isEqualTo("dummy value");
			
			toto.setName("another dummy value");
			persister.update(toto, selectedToto, true);
			selectedToto = persister.select(toto.getIdentifier());
			assertThat(selectedToto.getName()).isEqualTo("another dummy value");

			persister.delete(selectedToto);
			ExecutableQuery<String> stringExecutableQuery = persistenceContext.newQuery("select identifier from Toto union all select identifier from Tata", String.class)
					.mapKey("identifier", String.class);
			Set<String> identifiers = stringExecutableQuery.execute(Accumulators.toSet());
			assertThat(identifiers).isEmpty();
		}
		
		@Test
		void crud_severalTables() {
			dialect.getColumnBinderRegistry().register(
					(Class<Identifier<UUID>>) (Class) Identifier.class,
					new NullAwareParameterBinder<>(new LambdaParameterBinder<>(DefaultParameterBinders.UUID_BINDER, PersistedIdentifier::new, StatefulIdentifier::getDelegate)));
			dialect.getSqlTypeRegistry().put(Identifier.class, "VARCHAR(255)");
			
			ConfiguredRelationalPersister<Toto, Identifier<UUID>> persister = (ConfiguredRelationalPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.mapKey(Toto::getIdentifier, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName).extraTableName("Tata")
					.map(Toto::getFirstName).extraTableName("Tutu")
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto toto = new Toto();
			toto.setIdentifier(new PersistableIdentifier<>(UUID.randomUUID()));
			toto.setName("dummy name");
			toto.setFirstName("dummy firstName");
			persister.insert(toto);
			
			Toto selectedToto = persister.select(toto.getIdentifier());
			assertThat(selectedToto.getName()).isEqualTo("dummy name");
			assertThat(selectedToto.getFirstName()).isEqualTo("dummy firstName");
			
			toto.setName("another dummy name");
			toto.setFirstName("another dummy firstName");
			persister.update(toto, selectedToto, true);
			selectedToto = persister.select(toto.getIdentifier());
			assertThat(selectedToto.getName()).isEqualTo("another dummy name");
			assertThat(selectedToto.getFirstName()).isEqualTo("another dummy firstName");
			
			persister.delete(selectedToto);
			ExecutableQuery<String> stringExecutableQuery = persistenceContext.newQuery("select identifier from Toto union all select identifier from Tata", String.class)
					.mapKey("identifier", String.class);
			Set<String> identifiers = stringExecutableQuery.execute(Accumulators.toSet());
			assertThat(identifiers).isEmpty();
		}
		
		@Test
		void crud_withConfigurationFromInheritance() {
			dialect.getColumnBinderRegistry().register(
					(Class<Identifier<UUID>>) (Class) Identifier.class,
					new NullAwareParameterBinder<>(new LambdaParameterBinder<>(DefaultParameterBinders.UUID_BINDER, PersistedIdentifier::new, StatefulIdentifier::getDelegate)));
			dialect.getSqlTypeRegistry().put(Identifier.class, "VARCHAR(255)");
			
			ConfiguredRelationalPersister<Toto, Identifier<UUID>> persister = (ConfiguredRelationalPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE)
					.map(Toto::getName).extraTableName("Tata")
					.map(Toto::getFirstName).extraTableName("Tutu")
					.mapSuperClass(MappingEase.entityBuilder(AbstractToto.class, UUID_TYPE)
							.mapKey(AbstractToto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
							.map(AbstractToto::getProp1).extraTableName("Titi")
					)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto toto = new Toto();
			toto.setIdentifier(new PersistableIdentifier<>(UUID.randomUUID()));
			toto.setName("dummy name");
			toto.setProp1("dummy firstName");
			persister.insert(toto);
			
			Toto selectedToto = persister.select(toto.getId());
			assertThat(selectedToto.getName()).isEqualTo("dummy name");
			assertThat(selectedToto.getProp1()).isEqualTo("dummy firstName");
			
			toto.setName("another dummy name");
			toto.setProp1("another dummy firstName");
			persister.update(toto, selectedToto, true);
			selectedToto = persister.select(toto.getId());
			assertThat(selectedToto.getName()).isEqualTo("another dummy name");
			assertThat(selectedToto.getProp1()).isEqualTo("another dummy firstName");
			
			persister.delete(selectedToto);
			ExecutableQuery<String> stringExecutableQuery = persistenceContext.newQuery("select id from Toto union all select id from Tata", String.class)
					.mapKey("identifier", String.class);
			Set<String> identifiers = stringExecutableQuery.execute(Accumulators.toSet());
			assertThat(identifiers).isEmpty();
		}
		// TODO: check that no value on column does not insert row on secondary table
	}
	
	@Test
	void withTableNaming() {
		ConfiguredPersister<Toto, Identifier<UUID>> persister = (ConfiguredPersister) MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::setId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.withTableNaming(persistedClass -> "tata")
				.build(persistenceContext);
		
		assertThat(persister.giveImpliedTables().stream().map(Table::getName).collect(Collectors.toList())).containsExactlyInAnyOrder("tata");
	}
	
	@Test
	void embed_definedByGetter() {
		Table toto = new Table("Toto");
		MappingEase.entityBuilder(Toto.class, UUID_TYPE, toto)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
				.build(new PersistenceContext(mock(ConnectionProvider.class), dialect));
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("creationDate");
		assertThat(columnForProperty).isNotNull();
		assertThat(columnForProperty.getJavaType()).isEqualTo(Date.class);
	}
	
	@Test
	void embed_definedBySetter() {
		Table toto = new Table("Toto");
		MappingEase.entityBuilder(Toto.class, UUID_TYPE, toto)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.embed(Toto::setTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
				.build(new PersistenceContext(mock(ConnectionProvider.class), dialect));
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("creationDate");
		assertThat(columnForProperty).isNotNull();
		assertThat(columnForProperty.getJavaType()).isEqualTo(Date.class);
	}
	
	@Test
	void embed_withReadonlyProperty() {
		Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "varchar(36)");
		
		PersistenceContext persistenceContext = new PersistenceContext(dataSource, dialect);
		
		EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.embed(Toto::setTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate)
						.map(Timestamp::getReadonlyProperty).readonly())
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Toto toto = new Toto(new PersistedIdentifier<>(UUID.randomUUID()));
		Timestamp timestamp = new Timestamp();
		timestamp.setReadonlyProperty("dummy value");
		toto.setTimestamp(timestamp);
		
		persister.insert(toto);
		
		String readonlyProperty = persistenceContext.newQuery("select readonlyProperty from Toto", String.class)
				.mapKey("readonlyProperty", String.class)
				.execute(Accumulators.getFirst());
		assertThat(readonlyProperty).isNull();
	}
	
	@Test
	void embed_withOverriddenColumnName() {
		Table toto = new Table("Toto");
		MappingEase.entityBuilder(Toto.class, UUID_TYPE, toto)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
					.overrideName(Timestamp::getCreationDate, "createdAt")
					.overrideName(Timestamp::getModificationDate, "modifiedAt")
				.build(new PersistenceContext(mock(ConnectionProvider.class), dialect));
		
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
				.hasMessage("Column 'modificationDate' of mapping 'Timestamp::getCreationDate' is already targeted by 'Timestamp::getModificationDate'");
	}
	
	@Test
	void embed_withOverriddenColumn() {
		Table targetTable = new Table("Toto");
		Column<Table, Date> createdAt = targetTable.addColumn("createdAt", Date.class);
		Column<Table, Date> modifiedAt = targetTable.addColumn("modifiedAt", Date.class);
		
		Connection connectionMock = InvocationHandlerSupport.mock(Connection.class);
		
		ConfiguredPersister<Toto, Identifier<UUID>> persister = (ConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE, targetTable)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
					.override(Timestamp::getCreationDate, createdAt)
					.override(Timestamp::getModificationDate, modifiedAt)
				.build(new PersistenceContext(new SimpleConnectionProvider(connectionMock), dialect));
		
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
		
		ConfiguredPersister<Toto, Identifier<UUID>> persister = (ConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE, targetTable)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
					.exclude(Timestamp::getCreationDate)
					.override(Timestamp::getModificationDate, modifiedAt)
				.build(new PersistenceContext(new SimpleConnectionProvider(connectionMock), dialect));
		
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
				.entityBuilder(Country.class, Identifier.LONG_TYPE, countryTable)
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
		
		mappingBuilder.build(persistenceContext);
		
		assertThat(countryTable.getColumns()).extracting(Column::getName).containsExactlyInAnyOrder(
				// from Country
				"id", "name",
				// from Person
				"presidentName", "presidentElectedAt",
				// from Country.timestamp
				"countryCreatedAt");
		
		Connection connectionMock = mock(Connection.class);
		
		// since we mock connection, we use a non-specific Dialect to avoid specific behavior on a generic connection
		Dialect dialect = new DefaultDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getDmlGenerator().sortColumnsAlphabetically();	// for steady checks on SQL orders
		
		ConfiguredPersister<Country, Identifier<Long>> persister = (ConfiguredPersister<Country, Identifier<Long>>) mappingBuilder.build(
				new PersistenceContext(new SimpleConnectionProvider(connectionMock), dialect));
		
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
		when(connectionMock.prepareStatement(anyString(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(statementMock);
		
		StringBuilder capturedSQL = new StringBuilder();
		((SimpleRelationalEntityPersister) (((PersisterWrapper) persister).getDeepestDelegate())).getInsertExecutor().setOperationListener(new SQLOperationListener<Column<Table, Object>>() {
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
				.entityBuilder(Country.class, Identifier.LONG_TYPE, countryTable)
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
		
		mappingBuilder.build(persistenceContext);
		
		assertThat(countryTable.getColumns()).extracting(Column::getName).containsExactlyInAnyOrder(
				// from Country
				"id", "name",
				// from Person
				"presidentName", "presidentElectedAt");
		
		Connection connectionMock = mock(Connection.class);
		
		// since we mock connection, we use a non-specific Dialect to avoid specific behavior on a generic connection
		Dialect dialect = new DefaultDialect();
		dialect.getColumnBinderRegistry().register((Class) UUID_TYPE, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getDmlGenerator().sortColumnsAlphabetically();	// for steady checks on SQL orders
		
		ConfiguredPersister<Country, Identifier<Long>> persister = (ConfiguredPersister<Country, Identifier<Long>>) mappingBuilder.build(
				new PersistenceContext(new SimpleConnectionProvider(connectionMock), dialect));
		
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
		when(connectionMock.prepareStatement(anyString(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(statementMock);
		
		StringBuilder capturedSQL = new StringBuilder();
		((SimpleRelationalEntityPersister) (((PersisterWrapper) persister).getDeepestDelegate())).getInsertExecutor().setOperationListener(new SQLOperationListener<Column<Table, Object>>() {
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
		FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<Country, Identifier<Long>, Timestamp> mappingBuilder = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE, countryTable)
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
		assertThatThrownBy(() -> mappingBuilder.build(persistenceContext))
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
		mappingBuilder.build(persistenceContext);
		
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
			
			// embeddable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = MappingEase.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate)
					.map(Timestamp::getModificationDate);
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.embed(Toto::getTimestamp, timestampMapping)
					.build(persistenceContext);	// necessary to set table since we override Identifier binding
			
			// column should be correctly created
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto toto = new Toto();
			// this partial instantiation of Timestamp let us test its partial load too
			toto.setTimestamp(new Timestamp(Dates.nowAsDate(), null));
			persister.insert(toto);
			
			// Is everything fine in database ?
			List<Timestamp> select = persistenceContext.select(Timestamp::new, creationDate, modificationDate);
			assertThat(first(select)).isEqualTo(toto.getTimestamp());
			
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
					new LambdaResultSetReader<>((resultSet, columnName) -> Locale.forLanguageTag(resultSet.getString(columnName)), Locale.class),
					new LambdaPreparedStatementWriter<>((preparedStatement, valueIndex, value) -> preparedStatement.setString(valueIndex, value.toLanguageTag()), Locale.class))));
			dialect.getSqlTypeRegistry().put(Locale.class, "VARCHAR(20)");
			
			// embeddable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = MappingEase.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate)
					.map(Timestamp::getModificationDate);
			
			EmbeddableMappingConfigurationProvider<TimestampWithLocale> timestampWithLocaleMapping = MappingEase.embeddableBuilder(TimestampWithLocale.class)
					.map(TimestampWithLocale::getLocale)
					.mapSuperClass(timestampMapping);
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.embed(Toto::getTimestamp, timestampWithLocaleMapping)
					.build(persistenceContext);	// necessary to set table since we override Identifier binding
			
			// column should be correctly created
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto toto = new Toto();
			// this partial instantiation of Timestamp let us test its partial load too
			toto.setTimestamp(new TimestampWithLocale(Dates.nowAsDate(), null, Locale.US));
			persister.insert(toto);
			
			// Is everything fine in database ?
			List<TimestampWithLocale> select = persistenceContext.select(TimestampWithLocale::new, creationDate, modificationDate, localeColumn);
			assertThat(first(select)).isEqualTo(toto.getTimestamp());
			
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
					new LambdaResultSetReader<>((resultSet, columnName) -> Locale.forLanguageTag(resultSet.getString(columnName)), Locale.class),
					new LambdaPreparedStatementWriter<>((preparedStatement, valueIndex, value) -> preparedStatement.setString(valueIndex, value.toLanguageTag()), Locale.class))));
			dialect.getSqlTypeRegistry().put(Locale.class, "VARCHAR(20)");
			
			// embeddable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = MappingEase.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate)
					.map(Timestamp::getModificationDate);
			
			EmbeddableMappingConfigurationProvider<TimestampWithLocale> timestampWithLocaleMapping = MappingEase.embeddableBuilder(TimestampWithLocale.class)
					.map(TimestampWithLocale::getLocale)
					.mapSuperClass(timestampMapping);
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.embed(Toto::getTimestamp, timestampWithLocaleMapping)
						.override(Timestamp::getModificationDate, modificationDate)
					.build(persistenceContext);
			
			// column should be correctly created
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto toto = new Toto();
			// this partial instantiation of Timestamp let us test its partial load too
			toto.setTimestamp(new TimestampWithLocale(Dates.nowAsDate(), null, Locale.US));
			persister.insert(toto);
			
			// Is everything fine in database ?
			List<TimestampWithLocale> select = persistenceContext.select(TimestampWithLocale::new, creationDate, modificationDate, localeColumn);
			assertThat(first(select)).isEqualTo(toto.getTimestamp());
			
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
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.embed(Toto::getTimestamp, timestampMapping)
						.overrideName(Timestamp::getCreationDate, "createdAt")
					.build(persistenceContext);	// necessary to set table since we override Identifier binding
			
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
			assertThat(first(select)).isEqualTo(toto.getTimestamp());
			
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
			
			// embeddable mapping to be reused
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
					.hasMessage("Column 'modificationDate' of mapping 'Timestamp::getCreationDate' is already targeted by 'Timestamp::getModificationDate'");
		}
		
		@Test
		void overrideName_nameIsAlreadyOverridden_nameIsOverwritten() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier> idColumn = totoTable.addColumn("id", UUID_TYPE);
			Column<Table, Date> creationDate = totoTable.addColumn("createdAt", Date.class);
			Column<Table, Date> modificationDate = totoTable.addColumn("modificationDate", Date.class);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			// embeddable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = MappingEase.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate).columnName("creation")
					.map(Timestamp::getModificationDate);
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.embed(Toto::getTimestamp, timestampMapping)
						.overrideName(Timestamp::getCreationDate, "createdAt")
					.build(persistenceContext);	// necessary to set table since we override Identifier binding
			
			// column should be correctly created
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Toto toto = new Toto();
			// this partial instantiation of Timestamp let us test its partial load too
			toto.setTimestamp(new Timestamp(Dates.nowAsDate(), null));
			persister.insert(toto);
			
			// Is everything fine in database ?
			List<Timestamp> select = persistenceContext.select(Timestamp::new, creationDate, modificationDate);
			assertThat(first(select)).isEqualTo(toto.getTimestamp());
			
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
			
			// embeddable mapping to be reused
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
			
			// embeddable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = MappingEase.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate).columnName("creation")
					.map(Timestamp::getModificationDate);
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.embed(Toto::getTimestamp, timestampMapping)
						.exclude(Timestamp::getCreationDate)
					.build(persistenceContext);	// necessary to set table since we override Identifier binding
			
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
			
			// embeddable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = MappingEase.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate).columnName("creation")
					.map(Timestamp::getModificationDate);
			
			EntityPersister<Toto, Identifier<UUID>> persister = MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
					.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(Toto::getName)
					.embed(Toto::getTimestamp, timestampMapping)
						.override(Timestamp::getCreationDate, creationDate)
					.build(persistenceContext);
			
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
			assertThat(first(select)).isEqualTo(toto.getTimestamp());
			
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
		ExecutableQuery<Integer> integerExecutableQuery = persistenceContext.newQuery("select * from PersonWithGender", Integer.class)
				.mapKey(Integer::new, "gender", Integer.class);
		Set<Integer> result = integerExecutableQuery.execute(Accumulators.toSet());
		assertThat(result).containsExactly(1);
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
		ExecutableQuery<Integer> integerExecutableQuery = persistenceContext.newQuery("select * from PersonWithGender", Integer.class)
				.mapKey(Integer::valueOf, "gender", String.class);
		Set<Integer> result = integerExecutableQuery.execute(Accumulators.toSet());
		assertThat(result).containsExactly(person.getGender().ordinal());
	}
	
	@Test
	void withEnum_columnMappedWithOrdinal() {
		Table personTable = new Table<>("PersonWithGender");
		Column<Table, Gender> genderColumn = personTable.addColumn("gender", Gender.class);
		
		EntityPersister<PersonWithGender, Identifier<Long>> personPersister = MappingEase.entityBuilder(PersonWithGender.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapEnum(PersonWithGender::getGender).column(genderColumn).byOrdinal()
				.build(persistenceContext);
		
		dialect.getSqlTypeRegistry().put(genderColumn, "INT");
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		PersonWithGender person = new PersonWithGender(new PersistableIdentifier<>(1L));
		person.setName("toto");
		person.setGender(Gender.FEMALE);
		personPersister.insert(person);
		
		// checking that ordinal was used
		ExecutableQuery<Integer> integerExecutableQuery = persistenceContext.newQuery("select * from PersonWithGender", Integer.class)
				.mapKey(Integer::valueOf, "gender", String.class);
		Set<Integer> result = integerExecutableQuery.execute(Accumulators.toSet());
		assertThat(result).containsExactly(person.getGender().ordinal());
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
		
		Column gender = personPersister.getMapping().getTargetTable().mapColumnsOnName().get("gender");
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
		
		Column gender = personPersister.getMapping().getTargetTable().mapColumnsOnName().get("gender");
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
					.map(Country::setDescription).columnName("zxx").fieldName("tutu")
					.mapSuperClass(() -> new FluentEmbeddableMappingConfigurationSupport<>(Country.class))
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
					.map(Country::getId).columnName("zz")
					.mapOneToOne(Country::getPresident, MappingEase.entityBuilder(Person.class, long.class))
					.mapSuperClass(() -> new FluentEmbeddableMappingConfigurationSupport<>(Country.class))
					.map(Country::getDescription).columnName("xx")
					.build(persistenceContext);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			MappingEase.entityBuilder(Country.class, long.class)
					.map(Country::getName)
					.map(Country::getId).columnName("zz")
					.mapSuperClass(() -> new FluentEmbeddableMappingConfigurationSupport<>(Country.class))
					// embed with setter
					.embed(Country::setPresident, MappingEase.embeddableBuilder(Person.class)
							.map(Person::getName))
					// embed with setter
					.embed(Country::setTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.mapOneToMany(Country::getCities, MappingEase.entityBuilder(City.class, long.class))
						// testing mappedBy with inheritance
						.mappedBy((SerializableFunction<City, AbstractCountry>) City::getAbstractCountry)
					.map(Country::getDescription).columnName("xx")
					.map(Country::getDummyProperty).columnName("dd")
					.build(persistenceContext);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			EmbeddableMappingConfigurationProvider<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
					.map(Person::getName);
			
			MappingEase.entityBuilder(Country.class, long.class)
					.map(Country::getName)
					.map(Country::getId).columnName("zz")
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
					.map(Country::getId).columnName("zz")
					.embed(Country::getPresident, personMappingBuilder)
					.mapSuperClass(() -> new FluentEmbeddableMappingConfigurationSupport<>(Country.class))
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
			MappingEase.entityBuilder(PersonWithGender.class, long.class, new Table<>("person"))
					.map(Person::getName)
					.map(Person::getName).column(personTable.name)
					.mapEnum(PersonWithGender::getGender).byOrdinal()
					.embed(Person::setTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.overrideName(Timestamp::getCreationDate, "myDate")
					.mapEnum(PersonWithGender::getGender).columnName("MM").byOrdinal()
					.mapEnum(PersonWithGender::getGender).column(personTable.gender).byOrdinal()
					.map(PersonWithGender::getId).columnName("zz")
					.mapEnum(PersonWithGender::setGender).byName()
					.embed(Person::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.mapEnum(PersonWithGender::setGender).columnName("MM").byName()
					.build(persistenceContext);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
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
		
		private String name;
		
		private String firstName;
		
		private Timestamp timestamp;
		
		private Set<State> possibleStates = new HashSet<>();
		
		private Set<Timestamp> times = new HashSet<>();
		
		private boolean setIdWasCalled;
		private boolean constructorWithIdWasCalled;
		private boolean constructorWith2ArgsWasCalled;
		
		public Toto() {
			super();
		}
		
		public Toto(PersistedIdentifier<UUID> id) {
			super(id);
			this.constructorWithIdWasCalled = true;
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
