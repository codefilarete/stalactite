package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.engine.PersistenceContextConfigurationBuilder.PersistenceContextConfiguration;
import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelector;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.sql.DatabaseSequenceSelectorFactory;
import org.codefilarete.stalactite.sql.DialectOptions;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.DDLSequenceGenerator;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.DMLGenerator.NoopSorter;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Guillaume Mary
 */
class PersistenceContextConfigurationBuilderTest {
	
	private final DatabaseSequenceSelectorFactory SEQUENCE_SELECT_BUILDER = (databaseSequence, connectionProvider)
			-> new DatabaseSequenceSelector(databaseSequence, "SELECT NEXT VALUE FOR " + databaseSequence.getAbsoluteName(), new ReadOperationFactory(), connectionProvider);
	
	@Test
	void inOperatorMaxSize() {
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry();
		DefaultTypeMapping javaTypeToSqlTypes = new DefaultTypeMapping();
		ParameterBinderRegistry parameterBinderRegistry = new ParameterBinderRegistry();
		
		PersistenceContextConfigurationBuilder testInstance = new PersistenceContextConfigurationBuilder(
				new DatabaseVendorSettings(
						new DatabaseSignet("my_vendor", 1, 0),
						Arrays.asSet("aKeyWord"),
						'`',
						javaTypeToSqlTypes,
						parameterBinderRegistry,
						(parameterBinders, dmlNameProviderFactory, sqlTypeRegistry) -> {
							DMLGenerator dmlGenerator = new DMLGenerator(parameterBinders, NoopSorter.INSTANCE, dmlNameProviderFactory);
							DDLTableGenerator ddlTableGenerator = new DDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
							DDLSequenceGenerator ddlSequenceGenerator = new DDLSequenceGenerator(dmlNameProviderFactory);
							return new SQLOperationsFactories(new WriteOperationFactory(), new ReadOperationFactory(), dmlGenerator, ddlTableGenerator, ddlSequenceGenerator, SEQUENCE_SELECT_BUILDER);
						},
						new GeneratedKeysReaderFactory() {
							@Override
							public <I> GeneratedKeysReader<I> build(String keyName, Class<I> columnType) {
								return new GeneratedKeysReader<>(keyName, columnBinderRegistry.getBinder(columnType));
							}
						},
						1000,
						false
				),
				new ConnectionSettings(),
				mock(DataSource.class)
		);
		
		PersistenceContextConfiguration builtConfiguration = testInstance.build(DialectOptions.noOptions().setInOperatorMaxSize(1500));
		// "In" operator size is taken on DialectOptions to make it more easily changed by user. Database vendor's one is considered a default value.
		assertThat(builtConfiguration.getDialect().getInOperatorMaxSize()).isEqualTo(1500);
	}
	
	@Test
	void fetchSize() {
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry();
		DefaultTypeMapping javaTypeToSqlTypes = new DefaultTypeMapping();
		ParameterBinderRegistry parameterBinderRegistry = new ParameterBinderRegistry();

		PersistenceContextConfigurationBuilder testInstance = new PersistenceContextConfigurationBuilder(
				new DatabaseVendorSettings(
						new DatabaseSignet("my_vendor", 1, 0),
						Arrays.asSet("aKeyWord"),
						'`',
						javaTypeToSqlTypes,
						parameterBinderRegistry,
						(parameterBinders, dmlNameProviderFactory, sqlTypeRegistry) -> {
							DMLGenerator dmlGenerator = new DMLGenerator(parameterBinders, NoopSorter.INSTANCE, dmlNameProviderFactory);
							DDLTableGenerator ddlTableGenerator = new DDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
							DDLSequenceGenerator ddlSequenceGenerator = new DDLSequenceGenerator(dmlNameProviderFactory);
							return new SQLOperationsFactories(new WriteOperationFactory(), new ReadOperationFactory(), dmlGenerator, ddlTableGenerator, ddlSequenceGenerator, SEQUENCE_SELECT_BUILDER);
						},
						new GeneratedKeysReaderFactory() {
							@Override
							public <I> GeneratedKeysReader<I> build(String keyName, Class<I> columnType) {
								return new GeneratedKeysReader<>(keyName, columnBinderRegistry.getBinder(columnType));
							}
						},
						1000,
						false
				),
				new ConnectionSettings(10, 200),
				mock(DataSource.class)
		);

		PersistenceContextConfiguration builtConfiguration = testInstance.build(DialectOptions.noOptions());
		assertThat(builtConfiguration.getConnectionConfiguration().getFetchSize()).isEqualTo(200);
	}
	
	@Test
	<T extends Table<T>> void quoteKeyWords() {
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry();
		DefaultTypeMapping javaTypeToSqlTypes = new DefaultTypeMapping();
		ParameterBinderRegistry parameterBinderRegistry = new ParameterBinderRegistry();
		
		PersistenceContextConfigurationBuilder testInstance = new PersistenceContextConfigurationBuilder(
				new DatabaseVendorSettings(
						new DatabaseSignet("my_vendor", 1, 0),
						Arrays.asSet("aKeyWord"),
						'`',
						javaTypeToSqlTypes,
						parameterBinderRegistry,
						(parameterBinders, dmlNameProviderFactory, sqlTypeRegistry) -> {
							DMLGenerator dmlGenerator = new DMLGenerator(parameterBinders, NoopSorter.INSTANCE, dmlNameProviderFactory);
							DDLTableGenerator ddlTableGenerator = new DDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
							DDLSequenceGenerator ddlSequenceGenerator = new DDLSequenceGenerator(dmlNameProviderFactory);
							return new SQLOperationsFactories(new WriteOperationFactory(), new ReadOperationFactory(), dmlGenerator, ddlTableGenerator, ddlSequenceGenerator, SEQUENCE_SELECT_BUILDER);
						},
						new GeneratedKeysReaderFactory() {
							@Override
							public <I> GeneratedKeysReader<I> build(String keyName, Class<I> columnType) {
								return new GeneratedKeysReader<>(keyName, columnBinderRegistry.getBinder(columnType));
							}
						},
						100,
						false
				),
				new ConnectionSettings(),
				mock(DataSource.class)
		);
		
		PersistenceContextConfiguration builtConfiguration = testInstance.build();
		T totoTable = (T) new Table("aKeyWord");
		Column<T, Long> idColumn = totoTable.addColumn("id", long.class);
		assertThat(builtConfiguration.getDialect().getDdlTableGenerator().generateCreateTable(totoTable)).isEqualTo("create table `aKeyWord`(id bigint)");
		assertThat(builtConfiguration.getDialect().getDmlGenerator().buildInsert(Arrays.asSet(idColumn)).getSQL()).isEqualTo("insert into `aKeyWord`(id) values (?)");
		assertThat(builtConfiguration.getDialect().getDmlGenerator().buildUpdate(Arrays.asSet(idColumn), Arrays.asSet(idColumn)).getSQL()).isEqualTo("update `aKeyWord` set id = ? where id = ?");
		assertThat(builtConfiguration.getDialect().getDmlGenerator().buildDelete(totoTable, Arrays.asSet(idColumn)).getSQL()).isEqualTo("delete from `aKeyWord` where id = ?");
		assertThat(builtConfiguration.getDialect().getDmlGenerator().buildSelect(totoTable, Arrays.asSet(idColumn), Arrays.asSet(idColumn)).getSQL()).isEqualTo("select id from `aKeyWord` where id = ?");
		
		Query query = new Query(totoTable);
		query.select(idColumn);
		query.where(idColumn, Operators.eq(idColumn));
		query.groupBy(idColumn);
		query.having(idColumn, Operators.eq(idColumn));
		assertThat(builtConfiguration.getDialect().getQuerySQLBuilderFactory().queryBuilder(query).toSQL())
				.isEqualTo("select `aKeyWord`.id from `aKeyWord` where `aKeyWord`.id = `aKeyWord`.id group by `aKeyWord`.id having `aKeyWord`.id= `aKeyWord`.id");
	}
}
