package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.dsl.FluentMappings;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.FluentEntityMappingConfigurationSupportTest.TimestampWithLocale;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.hsqldb.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.Dates;
import org.codefilarete.tool.collection.Iterables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.stalactite.sql.ddl.Size.length;
import static org.codefilarete.tool.collection.Iterables.first;
import static org.codefilarete.tool.collection.Iterables.map;

public class FluentEntityMappingConfigurationSupportEmbedTest {
	
	private static final Class<Identifier<UUID>> UUID_TYPE = (Class) Identifier.class;
	
	// NB: dialect is made non static because we register binder for the same column several times in these tests
	// and this is not supported : the very first one takes priority  
	private final Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	void initTest() {
		dialect.getDmlGenerator().sortColumnsAlphabetically();    // for steady checks on SQL orders
		// binder creation for our identifier
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		
		persistenceContext = new PersistenceContext(dataSource, dialect);
	}
	
	@Test
	void happyPath() {
		Table totoTable = new Table("Toto");
		Column<Table, Identifier> idColumn = totoTable.addColumn("id", UUID_TYPE);
		Column<Table, Date> creationDate = totoTable.addColumn("creationDate", Date.class);
		Column<Table, Date> modificationDate = totoTable.addColumn("modificationDate", Date.class);
		dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
		
		// embeddable mapping to be reused
		EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = FluentMappings.embeddableBuilder(Timestamp.class)
				.map(Timestamp::getCreationDate)
				.map(Timestamp::getModificationDate);
		
		EntityPersister<FluentEntityMappingConfigurationSupportTest.Toto, Identifier<UUID>> persister = FluentMappings.entityBuilder(FluentEntityMappingConfigurationSupportTest.Toto.class, UUID_TYPE)
				.onTable(totoTable)
				.mapKey(FluentEntityMappingConfigurationSupportTest.Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(FluentEntityMappingConfigurationSupportTest.Toto::getName)
				.embed(FluentEntityMappingConfigurationSupportTest.Toto::getTimestamp, timestampMapping)
				.build(persistenceContext);
		
		// column should be correctly created
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		FluentEntityMappingConfigurationSupportTest.Toto toto = new FluentEntityMappingConfigurationSupportTest.Toto();
		// this partial instantiation of Timestamp let us test its partial load too
		toto.setTimestamp(new Timestamp(Dates.nowAsDate(), null));
		persister.insert(toto);
		
		// Is everything fine in database ?
		List<Timestamp> select = persistenceContext.select(Timestamp::new, creationDate, modificationDate);
		assertThat(first(select)).isEqualTo(toto.getTimestamp());
		
		// Is loading is fine too ?
		FluentEntityMappingConfigurationSupportTest.Toto loadedToto = persister.select(toto.getId());
		assertThat(loadedToto.getTimestamp()).isEqualTo(toto.getTimestamp());
	}
	
	@Nested
	class WithMappedSuperClass {
		
		@Test
		void happyPath() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier> idColumn = totoTable.addColumn("id", UUID_TYPE);
			Column<Table, Date> creationDate = totoTable.addColumn("creationDate", Date.class);
			Column<Table, Date> modificationDate = totoTable.addColumn("modificationDate", Date.class);
			Column<Table, Locale> localeColumn = totoTable.addColumn("locale", Locale.class);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			dialect.getColumnBinderRegistry().register(Locale.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(
					new ResultSetReader.LambdaResultSetReader<>((resultSet, columnName) -> Locale.forLanguageTag(resultSet.getString(columnName)), Locale.class),
					new PreparedStatementWriter.LambdaPreparedStatementWriter<>((preparedStatement, valueIndex, value) -> preparedStatement.setString(valueIndex, value.toLanguageTag()), Locale.class))));
			dialect.getSqlTypeRegistry().put(Locale.class, "VARCHAR(20)");
			
			// embeddable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = FluentMappings.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate)
					.map(Timestamp::getModificationDate);
			
			EmbeddableMappingConfigurationProvider<TimestampWithLocale> timestampWithLocaleMapping = FluentMappings.embeddableBuilder(TimestampWithLocale.class)
					.map(TimestampWithLocale::getLocale)
					.mapSuperClass(timestampMapping);
			
			EntityPersister<FluentEntityMappingConfigurationSupportTest.Toto, Identifier<UUID>> persister = FluentMappings.entityBuilder(FluentEntityMappingConfigurationSupportTest.Toto.class, UUID_TYPE)
					.onTable(totoTable)
					.mapKey(FluentEntityMappingConfigurationSupportTest.Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(FluentEntityMappingConfigurationSupportTest.Toto::getName)
					.embed(FluentEntityMappingConfigurationSupportTest.Toto::getTimestamp, timestampWithLocaleMapping)
					.build(persistenceContext);
			
			// column should be correctly created
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			FluentEntityMappingConfigurationSupportTest.Toto toto = new FluentEntityMappingConfigurationSupportTest.Toto();
			// this partial instantiation of Timestamp let us test its partial load too
			toto.setTimestamp(new TimestampWithLocale(Dates.nowAsDate(), null, Locale.US));
			persister.insert(toto);
			
			// Is everything fine in database ?
			List<TimestampWithLocale> select = persistenceContext.select(TimestampWithLocale::new, creationDate, modificationDate, localeColumn);
			assertThat(first(select)).isEqualTo(toto.getTimestamp());
			
			// Is loading is fine too ?
			FluentEntityMappingConfigurationSupportTest.Toto loadedToto = persister.select(toto.getId());
			assertThat(loadedToto.getTimestamp()).isEqualTo(toto.getTimestamp());
		}
		
		@Test
		void withMappedSuperClassAndOverride_schemaGeneration() {
			Table<?> totoTable = new Table("Toto");
			Column<?, Identifier<UUID>> idColumn = totoTable.addColumn("id", UUID_TYPE);
			Column<?, Date> modificationDate = totoTable.addColumn("modificationTime", Date.class);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			dialect.getColumnBinderRegistry().register(Locale.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(
					new ResultSetReader.LambdaResultSetReader<>((resultSet, columnName) -> Locale.forLanguageTag(resultSet.getString(columnName)), Locale.class),
					new PreparedStatementWriter.LambdaPreparedStatementWriter<>((preparedStatement, valueIndex, value) -> preparedStatement.setString(valueIndex, value.toLanguageTag()), Locale.class))));
			dialect.getSqlTypeRegistry().put(Locale.class, "VARCHAR(20)");
			
			// embeddable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = FluentMappings.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate)
					.map(Timestamp::getModificationDate);
			
			EmbeddableMappingConfigurationProvider<TimestampWithLocale> timestampWithLocaleMapping = FluentMappings.embeddableBuilder(TimestampWithLocale.class)
					.map(TimestampWithLocale::getLocale)
					.mapSuperClass(timestampMapping);
			
			EntityPersister<FluentEntityMappingConfigurationSupportTest.Toto, Identifier<UUID>> persister = FluentMappings.entityBuilder(FluentEntityMappingConfigurationSupportTest.Toto.class, UUID_TYPE)
					.onTable(totoTable)
					.mapKey(FluentEntityMappingConfigurationSupportTest.Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(FluentEntityMappingConfigurationSupportTest.Toto::getName)
					.embed(FluentEntityMappingConfigurationSupportTest.Toto::getTimestamp, timestampWithLocaleMapping)
					.override(Timestamp::getModificationDate, modificationDate)
					.build(persistenceContext);
			
			// column should be correctly created
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			assertThat(totoTable.getColumns().stream().map(Column::getName)).containsExactlyInAnyOrder(
					"id", "name", "creationDate", "modificationTime", "locale"
			);
		}
		
		@Test
		void withMappedSuperClassAndOverrideName_schemaGeneration() {
			Table<?> totoTable = new Table("Toto");
			Column<?, Identifier<UUID>> idColumn = totoTable.addColumn("id", UUID_TYPE);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			dialect.getColumnBinderRegistry().register(Locale.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(
					new ResultSetReader.LambdaResultSetReader<>((resultSet, columnName) -> Locale.forLanguageTag(resultSet.getString(columnName)), Locale.class),
					new PreparedStatementWriter.LambdaPreparedStatementWriter<>((preparedStatement, valueIndex, value) -> preparedStatement.setString(valueIndex, value.toLanguageTag()), Locale.class))));
			dialect.getSqlTypeRegistry().put(Locale.class, "VARCHAR(20)");
			
			// embeddable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = FluentMappings.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate)
					.map(Timestamp::getModificationDate);
			
			EmbeddableMappingConfigurationProvider<TimestampWithLocale> timestampWithLocaleMapping = FluentMappings.embeddableBuilder(TimestampWithLocale.class)
					.map(TimestampWithLocale::getLocale)
					.mapSuperClass(timestampMapping);
			
			EntityPersister<FluentEntityMappingConfigurationSupportTest.Toto, Identifier<UUID>> persister = FluentMappings.entityBuilder(FluentEntityMappingConfigurationSupportTest.Toto.class, UUID_TYPE)
					.onTable(totoTable)
					.mapKey(FluentEntityMappingConfigurationSupportTest.Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(FluentEntityMappingConfigurationSupportTest.Toto::getName)
					.embed(FluentEntityMappingConfigurationSupportTest.Toto::getTimestamp, timestampWithLocaleMapping)
					.overrideName(Timestamp::getModificationDate, "modificationTime")
					.build(persistenceContext);
			
			// column should be correctly created
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			assertThat(totoTable.getColumns().stream().map(Column::getName)).containsExactlyInAnyOrder(
					"id", "name", "creationDate", "modificationTime", "locale"
			);
		}
		
		
		@Test
		void withMappedSuperClassAndOverrideSize_schemaGeneration() {
			Table<?> totoTable = new Table("Toto");
			Column<?, Identifier<UUID>> idColumn = totoTable.addColumn("id", UUID_TYPE);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			dialect.getColumnBinderRegistry().register(Locale.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(
					new ResultSetReader.LambdaResultSetReader<>((resultSet, columnName) -> Locale.forLanguageTag(resultSet.getString(columnName)), Locale.class),
					new PreparedStatementWriter.LambdaPreparedStatementWriter<>((preparedStatement, valueIndex, value) -> preparedStatement.setString(valueIndex, value.toLanguageTag()), Locale.class))));
			dialect.getSqlTypeRegistry().put(Locale.class, "VARCHAR(20)");
			
			// embeddable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = FluentMappings.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate)
					.map(Timestamp::getModificationDate)
					.map(Timestamp::getReadonlyProperty);
			
			EmbeddableMappingConfigurationProvider<TimestampWithLocale> timestampWithLocaleMapping = FluentMappings.embeddableBuilder(TimestampWithLocale.class)
					.map(TimestampWithLocale::getLocale)
					.mapSuperClass(timestampMapping);
			
			EntityPersister<FluentEntityMappingConfigurationSupportTest.Toto, Identifier<UUID>> persister = FluentMappings.entityBuilder(FluentEntityMappingConfigurationSupportTest.Toto.class, UUID_TYPE)
					.onTable(totoTable)
					.mapKey(FluentEntityMappingConfigurationSupportTest.Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(FluentEntityMappingConfigurationSupportTest.Toto::getName)
					.embed(FluentEntityMappingConfigurationSupportTest.Toto::getTimestamp, timestampWithLocaleMapping)
					.overrideSize(Timestamp::getReadonlyProperty, length(42))
					.build(persistenceContext);
			
			// column should be correctly created
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			assertThat(totoTable.getColumns().stream().map(Column::getName)).containsExactlyInAnyOrder(
					"id", "name", "creationDate", "modificationDate", "readonlyProperty", "locale"
			);
			assertThat(Iterables.map(totoTable.getColumns(), Column::getName).get("readonlyProperty").getSize())
					.usingRecursiveComparison()
					.isEqualTo(length(42));
		}
		
		@Test
		void withMappedSuperClassAndOverride_crud() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier> idColumn = totoTable.addColumn("id", UUID_TYPE);
			Column<Table, Date> creationDate = totoTable.addColumn("creationDate", Date.class);
			Column<Table, Date> modificationDate = totoTable.addColumn("modificationTime", Date.class);
			Column<Table, Locale> localeColumn = totoTable.addColumn("locale", Locale.class);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			dialect.getColumnBinderRegistry().register(Locale.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(
					new ResultSetReader.LambdaResultSetReader<>((resultSet, columnName) -> Locale.forLanguageTag(resultSet.getString(columnName)), Locale.class),
					new PreparedStatementWriter.LambdaPreparedStatementWriter<>((preparedStatement, valueIndex, value) -> preparedStatement.setString(valueIndex, value.toLanguageTag()), Locale.class))));
			dialect.getSqlTypeRegistry().put(Locale.class, "VARCHAR(20)");
			
			// embeddable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = FluentMappings.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate)
					.map(Timestamp::getModificationDate);
			
			EmbeddableMappingConfigurationProvider<TimestampWithLocale> timestampWithLocaleMapping = FluentMappings.embeddableBuilder(TimestampWithLocale.class)
					.map(TimestampWithLocale::getLocale)
					.mapSuperClass(timestampMapping);
			
			EntityPersister<FluentEntityMappingConfigurationSupportTest.Toto, Identifier<UUID>> persister = FluentMappings.entityBuilder(FluentEntityMappingConfigurationSupportTest.Toto.class, UUID_TYPE)
					.onTable(totoTable)
					.mapKey(FluentEntityMappingConfigurationSupportTest.Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(FluentEntityMappingConfigurationSupportTest.Toto::getName)
					.embed(FluentEntityMappingConfigurationSupportTest.Toto::getTimestamp, timestampWithLocaleMapping)
					.override(Timestamp::getModificationDate, modificationDate)
					.build(persistenceContext);
			
			// column should be correctly created
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			FluentEntityMappingConfigurationSupportTest.Toto toto = new FluentEntityMappingConfigurationSupportTest.Toto();
			// this partial instantiation of Timestamp let us test its partial load too
			toto.setTimestamp(new TimestampWithLocale(Dates.nowAsDate(), null, Locale.US));
			persister.insert(toto);
			
			// Is everything fine in database ?
			List<TimestampWithLocale> select = persistenceContext.select(TimestampWithLocale::new, creationDate, modificationDate, localeColumn);
			assertThat(first(select)).isEqualTo(toto.getTimestamp());
			
			// Is loading is fine too ?
			FluentEntityMappingConfigurationSupportTest.Toto loadedToto = persister.select(toto.getId());
			assertThat(loadedToto.getTimestamp()).isEqualTo(toto.getTimestamp());
		}
	}
	
	@Nested
	class OverrideName {
		
		@Test
		void happyPath() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier> idColumn = totoTable.addColumn("id", UUID_TYPE);
			Column<Table, Date> creationDate = totoTable.addColumn("createdAt", Date.class);
			Column<Table, Date> modificationDate = totoTable.addColumn("modificationDate", Date.class);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = FluentMappings.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate)
					.map(Timestamp::getModificationDate);
			
			EntityPersister<FluentEntityMappingConfigurationSupportTest.Toto, Identifier<UUID>> persister = FluentMappings.entityBuilder(FluentEntityMappingConfigurationSupportTest.Toto.class, UUID_TYPE)
					.onTable(totoTable)
					.mapKey(FluentEntityMappingConfigurationSupportTest.Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(FluentEntityMappingConfigurationSupportTest.Toto::getName)
					.embed(FluentEntityMappingConfigurationSupportTest.Toto::getTimestamp, timestampMapping)
					.overrideName(Timestamp::getCreationDate, "createdAt")
					.build(persistenceContext);
			
			Map<String, Column> columnsByName = totoTable.mapColumnsOnName();
			
			// columns with getter name must be absent (hard to test: can be absent for many reasons !)
			assertThat(columnsByName.get("creationDate")).isNull();
			
			// column should be correctly created
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			FluentEntityMappingConfigurationSupportTest.Toto toto = new FluentEntityMappingConfigurationSupportTest.Toto();
			// this partial instantiation of Timestamp let us test its partial load too
			toto.setTimestamp(new Timestamp(Dates.nowAsDate(), null));
			persister.insert(toto);
			
			// Is everything fine in database ?
			List<Timestamp> select = persistenceContext.select(Timestamp::new, creationDate, modificationDate);
			assertThat(first(select)).isEqualTo(toto.getTimestamp());
			
			// Is loading is fine too ?
			FluentEntityMappingConfigurationSupportTest.Toto loadedToto = persister.select(toto.getId());
			assertThat(loadedToto.getTimestamp()).isEqualTo(toto.getTimestamp());
		}
		
		@Test
		void nameAlreadyExists_throwsException() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier> idColumn = totoTable.addColumn("id", UUID_TYPE);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			// embeddable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = FluentMappings.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate)
					.map(Timestamp::getModificationDate);
			
			assertThatThrownBy(() -> FluentMappings.entityBuilder(FluentEntityMappingConfigurationSupportTest.Toto.class, UUID_TYPE)
					.mapKey(FluentEntityMappingConfigurationSupportTest.Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(FluentEntityMappingConfigurationSupportTest.Toto::getName)
					.embed(FluentEntityMappingConfigurationSupportTest.Toto::getTimestamp, timestampMapping)
					.overrideName(Timestamp::getCreationDate, "modificationDate")
					.build(persistenceContext))
					.isInstanceOf(MappingConfigurationException.class)
					.hasMessage("Column 'modificationDate' of mapping 'Timestamp::getCreationDate' is already targeted by 'Timestamp::getModificationDate'");
		}
		
		@Test
		void nameIsAlreadyOverridden_nameIsOverwritten() {
			Table totoTable = new Table("Toto");
			Column<Table, Identifier> idColumn = totoTable.addColumn("id", UUID_TYPE);
			Column<Table, Date> creationDate = totoTable.addColumn("createdAt", Date.class);
			Column<Table, Date> modificationDate = totoTable.addColumn("modificationDate", Date.class);
			dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
			dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
			
			// embeddable mapping to be reused
			EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = FluentMappings.embeddableBuilder(Timestamp.class)
					.map(Timestamp::getCreationDate).columnName("creation")
					.map(Timestamp::getModificationDate);
			
			EntityPersister<FluentEntityMappingConfigurationSupportTest.Toto, Identifier<UUID>> persister = FluentMappings.entityBuilder(FluentEntityMappingConfigurationSupportTest.Toto.class, UUID_TYPE)
					.onTable(totoTable)
					.mapKey(FluentEntityMappingConfigurationSupportTest.Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
					.map(FluentEntityMappingConfigurationSupportTest.Toto::getName)
					.embed(FluentEntityMappingConfigurationSupportTest.Toto::getTimestamp, timestampMapping)
					.overrideName(Timestamp::getCreationDate, "createdAt")
					.build(persistenceContext);
			
			// column should be correctly created
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			FluentEntityMappingConfigurationSupportTest.Toto toto = new FluentEntityMappingConfigurationSupportTest.Toto();
			// this partial instantiation of Timestamp let us test its partial load too
			toto.setTimestamp(new Timestamp(Dates.nowAsDate(), null));
			persister.insert(toto);
			
			// Is everything fine in database ?
			List<Timestamp> select = persistenceContext.select(Timestamp::new, creationDate, modificationDate);
			assertThat(first(select)).isEqualTo(toto.getTimestamp());
			
			// Is loading is fine too ?
			FluentEntityMappingConfigurationSupportTest.Toto loadedToto = persister.select(toto.getId());
			assertThat(loadedToto.getTimestamp()).isEqualTo(toto.getTimestamp());
		}
		
	}
	
	@Test
	void mappingDefinedTwice_throwsException() {
		Table totoTable = new Table("Toto");
		Column<Table, Identifier> idColumn = totoTable.addColumn("id", UUID_TYPE);
		dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
		
		// embeddable mapping to be reused
		EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = FluentMappings.embeddableBuilder(Timestamp.class)
				.map(Timestamp::getCreationDate)
				.map(Timestamp::getModificationDate)
				.map(Timestamp::setModificationDate);
		
		assertThatThrownBy(() -> FluentMappings.entityBuilder(FluentEntityMappingConfigurationSupportTest.Toto.class, UUID_TYPE)
				.mapKey(FluentEntityMappingConfigurationSupportTest.Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(FluentEntityMappingConfigurationSupportTest.Toto::getName)
				.embed(FluentEntityMappingConfigurationSupportTest.Toto::getTimestamp, timestampMapping)
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
		EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = FluentMappings.embeddableBuilder(Timestamp.class)
				.map(Timestamp::getCreationDate).columnName("creation")
				.map(Timestamp::getModificationDate);
		
		EntityPersister<FluentEntityMappingConfigurationSupportTest.Toto, Identifier<UUID>> persister = FluentMappings.entityBuilder(FluentEntityMappingConfigurationSupportTest.Toto.class, UUID_TYPE)
				.onTable(totoTable)
				.mapKey(FluentEntityMappingConfigurationSupportTest.Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(FluentEntityMappingConfigurationSupportTest.Toto::getName)
				.embed(FluentEntityMappingConfigurationSupportTest.Toto::getTimestamp, timestampMapping)
				.exclude(Timestamp::getCreationDate)
				.build(persistenceContext);
		
		Map map = totoTable.mapColumnsOnName();
		assertThat(map.get("creationDate")).isNull();
		
		// column should be correctly created
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		FluentEntityMappingConfigurationSupportTest.Toto toto = new FluentEntityMappingConfigurationSupportTest.Toto();
		// this partial instantiation of Timestamp let us test its partial load too
		toto.setTimestamp(new Timestamp(Dates.nowAsDate(), null));
		persister.insert(toto);
		
		// Is loading is fine too ?
		FluentEntityMappingConfigurationSupportTest.Toto loadedToto = persister.select(toto.getId());
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
		EmbeddableMappingConfigurationProvider<Timestamp> timestampMapping = FluentMappings.embeddableBuilder(Timestamp.class)
				.map(Timestamp::getCreationDate).columnName("creation")
				.map(Timestamp::getModificationDate);
		
		EntityPersister<FluentEntityMappingConfigurationSupportTest.Toto, Identifier<UUID>> persister = FluentMappings.entityBuilder(FluentEntityMappingConfigurationSupportTest.Toto.class, UUID_TYPE)
				.onTable(totoTable)
				.mapKey(FluentEntityMappingConfigurationSupportTest.Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(FluentEntityMappingConfigurationSupportTest.Toto::getName)
				.embed(FluentEntityMappingConfigurationSupportTest.Toto::getTimestamp, timestampMapping)
				.override(Timestamp::getCreationDate, creationDate)
				.build(persistenceContext);
		
		Map map = totoTable.mapColumnsOnName();
		assertThat(map.get("creationDate")).isNull();
		
		/// column should be correctly created
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		FluentEntityMappingConfigurationSupportTest.Toto toto = new FluentEntityMappingConfigurationSupportTest.Toto();
		// this partial instantiation of Timestamp let us test its partial load too
		toto.setTimestamp(new Timestamp(Dates.nowAsDate(), null));
		persister.insert(toto);
		
		// Is everything fine in database ?
		List<Timestamp> select = persistenceContext.select(Timestamp::new, creationDate, modificationDate);
		assertThat(first(select)).isEqualTo(toto.getTimestamp());
		
		// Is loading is fine too ?
		FluentEntityMappingConfigurationSupportTest.Toto loadedToto = persister.select(toto.getId());
		assertThat(loadedToto.getTimestamp()).isEqualTo(toto.getTimestamp());
	}
}
