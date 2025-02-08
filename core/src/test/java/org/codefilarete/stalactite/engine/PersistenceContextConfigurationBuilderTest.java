package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.engine.PersistenceContextConfigurationBuilder.PersistenceContextConfiguration;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory;
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
	
	@Test
	void inOperatorMaxSize() {
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry();
		DefaultTypeMapping javaTypeToSqlTypes = new DefaultTypeMapping();
		ParameterBinderRegistry parameterBinderRegistry = new ParameterBinderRegistry();
		
		PersistenceContextConfigurationBuilder testInstance = new PersistenceContextConfigurationBuilder(
				new DatabaseVendorSettings(
						Arrays.asSet("aKeyWord"),
						'`',
						javaTypeToSqlTypes,
						parameterBinderRegistry,
						(parameterBinders, dmlNameProviderFactory, sqlTypeRegistry) -> {
							DMLGenerator dmlGenerator = new DMLGenerator(parameterBinders, NoopSorter.INSTANCE, dmlNameProviderFactory);
							DDLTableGenerator ddlTableGenerator = new DDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
							return new SQLOperationsFactories(new WriteOperationFactory(), new ReadOperationFactory(), dmlGenerator, ddlTableGenerator);
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
				new ConnectionSettings(mock(DataSource.class), 10, 150, 3)
		);
		
		PersistenceContextConfiguration builtConfiguration = testInstance.build();
		// "In" operator size is taken on ConnectionSettings to make it more easily changed by user. Database vendor's one is considered a default value.
		assertThat(builtConfiguration.getDialect().getInOperatorMaxSize()).isEqualTo(150);
	}
	
	@Test
	<T extends Table<T>> void quoteKeyWords() {
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry();
		DefaultTypeMapping javaTypeToSqlTypes = new DefaultTypeMapping();
		ParameterBinderRegistry parameterBinderRegistry = new ParameterBinderRegistry();
		
		PersistenceContextConfigurationBuilder testInstance = new PersistenceContextConfigurationBuilder(
				new DatabaseVendorSettings(
						Arrays.asSet("aKeyWord"),
						'`',
						javaTypeToSqlTypes,
						parameterBinderRegistry,
						(parameterBinders, dmlNameProviderFactory, sqlTypeRegistry) -> {
							DMLGenerator dmlGenerator = new DMLGenerator(parameterBinders, NoopSorter.INSTANCE, dmlNameProviderFactory);
							DDLTableGenerator ddlTableGenerator = new DDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
							return new SQLOperationsFactories(new WriteOperationFactory(), new ReadOperationFactory(), dmlGenerator, ddlTableGenerator);
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
				new ConnectionSettings(mock(DataSource.class), 10, 150, 3)
		);
		
		PersistenceContextConfiguration builtConfiguration = testInstance.build();
		T totoTable = (T) new Table("aKeyWord");
		Column<T, Long> idColumn = totoTable.addColumn("id", long.class);
		assertThat(builtConfiguration.getDialect().getDdlTableGenerator().generateCreateTable(totoTable)).isEqualTo("create table `aKeyWord`(id bigint not null)");
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
	
	@Test
	<T extends Table<T>> void quoteAllSQLIdentifiers() {
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry();
		DefaultTypeMapping javaTypeToSqlTypes = new DefaultTypeMapping();
		ParameterBinderRegistry parameterBinderRegistry = new ParameterBinderRegistry();
		
		PersistenceContextConfigurationBuilder testInstance = new PersistenceContextConfigurationBuilder(
				new DatabaseVendorSettings(
						Arrays.asSet("aKeyWord"),
						'`',
						javaTypeToSqlTypes,
						parameterBinderRegistry,
						(parameterBinders, dmlNameProviderFactory, sqlTypeRegistry) -> {
							DMLGenerator dmlGenerator = new DMLGenerator(parameterBinders, NoopSorter.INSTANCE, dmlNameProviderFactory);
							DDLTableGenerator ddlTableGenerator = new DDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
							return new SQLOperationsFactories(new WriteOperationFactory(), new ReadOperationFactory(), dmlGenerator, ddlTableGenerator);
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
				new ConnectionSettings(mock(DataSource.class), 10, 150, 3)
		);
		
		testInstance.quoteAllSQLIdentifiers();
		
		PersistenceContextConfiguration builtConfiguration = testInstance.build();
		T totoTable = (T) new Table("Toto");
		Column<T, Long> idColumn = totoTable.addColumn("id", long.class);
		assertThat(builtConfiguration.getDialect().getDdlTableGenerator().generateCreateTable(totoTable)).isEqualTo("create table `Toto`(`id` bigint not null)");
		assertThat(builtConfiguration.getDialect().getDmlGenerator().buildInsert(Arrays.asSet(idColumn)).getSQL()).isEqualTo("insert into `Toto`(`id`) values (?)");
		assertThat(builtConfiguration.getDialect().getDmlGenerator().buildUpdate(Arrays.asSet(idColumn), Arrays.asSet(idColumn)).getSQL()).isEqualTo("update `Toto` set `id` = ? where `id` = ?");
		assertThat(builtConfiguration.getDialect().getDmlGenerator().buildDelete(totoTable, Arrays.asSet(idColumn)).getSQL()).isEqualTo("delete from `Toto` where `id` = ?");
		assertThat(builtConfiguration.getDialect().getDmlGenerator().buildSelect(totoTable, Arrays.asSet(idColumn), Arrays.asSet(idColumn)).getSQL()).isEqualTo("select `id` from `Toto` where `id` = ?");
		
		Query query = new Query(totoTable);
		query.select(idColumn);
		query.where(idColumn, Operators.eq(idColumn));
		query.groupBy(idColumn);
		query.having(idColumn, Operators.eq(idColumn));
		assertThat(builtConfiguration.getDialect().getQuerySQLBuilderFactory().queryBuilder(query).toSQL())
				.isEqualTo("select `Toto`.`id` from `Toto` where `Toto`.`id` = `Toto`.`id` group by `Toto`.`id` having `Toto`.`id`= `Toto`.`id`");
	}
	
}