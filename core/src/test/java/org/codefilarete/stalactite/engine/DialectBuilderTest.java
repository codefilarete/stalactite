package org.codefilarete.stalactite.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;

import org.codefilarete.stalactite.query.builder.FunctionSQLBuilderFactory.FunctionSQLBuilder;
import org.codefilarete.stalactite.query.builder.OperatorSQLBuilderFactory;
import org.codefilarete.stalactite.query.builder.SQLAppender;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryEase;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.UnitaryOperator;
import org.codefilarete.stalactite.query.model.operator.Like;
import org.codefilarete.stalactite.sql.DMLNameProviderFactory;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.DialectOptions;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory.DefaultGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.QuerySQLBuilderFactoryBuilder;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.DDLSequenceGenerator;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.InMemoryResultSet;
import org.codefilarete.stalactite.sql.statement.ColumnParameterizedSQL;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.DMLGenerator.NoopSorter;
import org.codefilarete.stalactite.sql.statement.PreparedUpdate;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.trace.MutableLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.query.model.Operators.like;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DialectBuilderTest {
	
	private DatabaseVendorSettings defaultDatabaseVendorSettings;
	
	@BeforeEach
	void initialize() {
		ParameterBinderRegistry parameterBinderRegistry = new ParameterBinderRegistry();
		DefaultTypeMapping defaultTypeMapping = new DefaultTypeMapping();
		defaultDatabaseVendorSettings = new DatabaseVendorSettings(
				new DatabaseSignet("my_vendor", 1, 0), 
				Arrays.asSet("a_keyword"),
				'\'',
				defaultTypeMapping,
				parameterBinderRegistry,
				(parameterBinders, dmlNameProviderFactory, sqlTypeRegistry) -> {
					DMLGenerator dmlGenerator = new DMLGenerator(parameterBinders, NoopSorter.INSTANCE, dmlNameProviderFactory);
					DDLTableGenerator ddlTableGenerator = new DDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
					DDLSequenceGenerator ddlSequenceGenerator = new DDLSequenceGenerator(dmlNameProviderFactory);
					return new SQLOperationsFactories(new WriteOperationFactory(), new ReadOperationFactory(), dmlGenerator, ddlTableGenerator, ddlSequenceGenerator, (databaseSequence, connectionProvider) -> {
						MutableLong counter = new MutableLong();
						return counter::increment;
					});
				},
				new DefaultGeneratedKeysReaderFactory(parameterBinderRegistry),
				100,
				false
		);
	}
	
	
	@Nested
	class Keywords {
		
		@Test
		<T extends Table<T>> void keywordsAreEscaped() {
			DialectBuilder testInstance = new DialectBuilder(defaultDatabaseVendorSettings);
			
			Dialect dialect = testInstance.build();
			
			T keywordNamedTable = (T) new Table("a_KeYworD");
			Column<T, Long> idColumn = keywordNamedTable.addColumn("id", long.class);
			Column<T, String> nameColumn = keywordNamedTable.addColumn("name", String.class);
			
			// Note that since that there's a lot of propagation of quote character in the code, it is simpler to check
			// the result of some SQL code formating instead of checking that all generators have the right quote character
			String totoTableCreateScript = dialect.getDdlTableGenerator().generateCreateTable(keywordNamedTable);
			assertThat(totoTableCreateScript).isEqualTo("create table 'a_KeYworD'(id bigint not null, name varchar)");
			
			String sequenceScript = dialect.getDdlSequenceGenerator().generateCreateSequence(new org.codefilarete.stalactite.sql.ddl.structure.Sequence(null, "A_KeywOrD"));
			assertThat(sequenceScript).isEqualTo("create sequence 'A_KeywOrD'");
			
			ColumnParameterizedSQL<T> insert = dialect.getDmlGenerator().buildInsert(Arrays.asList(idColumn, nameColumn));
			assertThat(insert.getSQL()).isEqualTo("insert into 'a_KeYworD'(id, name) values (?, ?)");
			
			PreparedUpdate<T> update = dialect.getDmlGenerator().buildUpdate(Arrays.asList(idColumn, nameColumn), Arrays.asList(idColumn));
			assertThat(update.getSQL()).isEqualTo("update 'a_KeYworD' set id = ?, name = ? where id = ?");
			
			ColumnParameterizedSQL<T> delete = dialect.getDmlGenerator().buildDelete(keywordNamedTable, Arrays.asList(idColumn));
			assertThat(delete.getSQL()).isEqualTo("delete from 'a_KeYworD' where id = ?");
			
			ColumnParameterizedSQL<T> select = dialect.getDmlGenerator().buildSelect(keywordNamedTable, Arrays.asList(idColumn), Arrays.asList(idColumn));
			assertThat(select.getSQL()).isEqualTo("select id from 'a_KeYworD' where id = ?");
		}
		
		@Test
		<T extends Table<T>> void keywordsCanBeChanged() {
			DialectBuilder testInstance = new DialectBuilder(defaultDatabaseVendorSettings, new DialectOptions()
					.addSqlKeywords("another_keyword")
					.removeSqlKeywords("a_keyWOrd"));
			
			Dialect dialect = testInstance.build();
			
			T keywordNamedTable = (T) new Table("a_KeYworD");
			Column<T, Long> idColumn = keywordNamedTable.addColumn("another_keyword", long.class);
			Column<T, String> nameColumn = keywordNamedTable.addColumn("name", String.class);
			
			// Note that since that there's a lot of propagation of quote character in the code, it is simpler to check
			// the result of some SQL code formating instead of checking that all generators have the right quote character
			String totoTableCreateScript = dialect.getDdlTableGenerator().generateCreateTable(keywordNamedTable);
			assertThat(totoTableCreateScript).isEqualTo("create table a_KeYworD('another_keyword' bigint not null, name varchar)");
			
			String sequenceScript = dialect.getDdlSequenceGenerator().generateCreateSequence(new org.codefilarete.stalactite.sql.ddl.structure.Sequence(null, "A_KeywOrD"));
			assertThat(sequenceScript).isEqualTo("create sequence A_KeywOrD");
			
			ColumnParameterizedSQL<T> insert = dialect.getDmlGenerator().buildInsert(Arrays.asList(idColumn, nameColumn));
			assertThat(insert.getSQL()).isEqualTo("insert into a_KeYworD('another_keyword', name) values (?, ?)");
			
			PreparedUpdate<T> update = dialect.getDmlGenerator().buildUpdate(Arrays.asList(idColumn, nameColumn), Arrays.asList(idColumn));
			assertThat(update.getSQL()).isEqualTo("update a_KeYworD set 'another_keyword' = ?, name = ? where 'another_keyword' = ?");
			
			ColumnParameterizedSQL<T> delete = dialect.getDmlGenerator().buildDelete(keywordNamedTable, Arrays.asList(idColumn));
			assertThat(delete.getSQL()).isEqualTo("delete from a_KeYworD where 'another_keyword' = ?");
			
			ColumnParameterizedSQL<T> select = dialect.getDmlGenerator().buildSelect(keywordNamedTable, Arrays.asList(idColumn), Arrays.asList(idColumn));
			assertThat(select.getSQL()).isEqualTo("select 'another_keyword' from a_KeYworD where 'another_keyword' = ?");
		}
	}
	
	@Nested
	class BuildWithOptions {
		
		@Test
		<T extends Table<T>> void quoteSQLIdentifiers_sqlIdentifiersAreQuoted() {
			DialectOptions dialectOptions = new DialectOptions();
			dialectOptions.quoteSQLIdentifiers();
			dialectOptions.setQuoteCharacter('`');
			DialectBuilder testInstance = new DialectBuilder(defaultDatabaseVendorSettings, dialectOptions);
			
			Dialect dialect = testInstance.build();
			
			T totoTable = (T) new Table("Toto");
			Column<T, Long> idColumn = totoTable.addColumn("id", long.class);
			Column<T, String> nameColumn = totoTable.addColumn("name", String.class);
			
			// Note that since that there's a lot of propagation of quote character in the code, it is simpler to check
			// the result of some SQL code formating instead of checking that all generators have the right quote character
			String totoTableCreateScript = dialect.getDdlTableGenerator().generateCreateTable(totoTable);
			assertThat(totoTableCreateScript).isEqualTo("create table `Toto`(`id` bigint not null, `name` varchar)");
			
			String sequenceScript = dialect.getDdlSequenceGenerator().generateCreateSequence(new org.codefilarete.stalactite.sql.ddl.structure.Sequence(null, "tOtO"));
			assertThat(sequenceScript).isEqualTo("create sequence `tOtO`");
			
			ColumnParameterizedSQL<T> insert = dialect.getDmlGenerator().buildInsert(Arrays.asList(idColumn, nameColumn));
			assertThat(insert.getSQL()).isEqualTo("insert into `Toto`(`id`, `name`) values (?, ?)");
			
			PreparedUpdate<T> update = dialect.getDmlGenerator().buildUpdate(Arrays.asList(idColumn, nameColumn), Arrays.asList(idColumn));
			assertThat(update.getSQL()).isEqualTo("update `Toto` set `id` = ?, `name` = ? where `id` = ?");
			
			ColumnParameterizedSQL<T> delete = dialect.getDmlGenerator().buildDelete(totoTable, Arrays.asList(idColumn));
			assertThat(delete.getSQL()).isEqualTo("delete from `Toto` where `id` = ?");
			
			ColumnParameterizedSQL<T> select = dialect.getDmlGenerator().buildSelect(totoTable, Arrays.asList(idColumn), Arrays.asList(idColumn));
			assertThat(select.getSQL()).isEqualTo("select `id` from `Toto` where `id` = ?");
			
			
			Query query = new Query(totoTable);
			query.select(idColumn);
			query.where(idColumn, Operators.eq(idColumn));
			query.groupBy(idColumn);
			query.having(idColumn, Operators.eq(idColumn));
			assertThat(dialect.getQuerySQLBuilderFactory().queryBuilder(query).toSQL())
					.isEqualTo("select `Toto`.`id` from `Toto` where `Toto`.`id` = `Toto`.`id` group by `Toto`.`id` having `Toto`.`id`= `Toto`.`id`");
		}
		
		@Test
		<T extends Table<T>> void withNewTypeBinding_bindingIsApplied() {
			class DummyType {
				
			}
			
			ParameterBinder dummyParameterBinder = mock(ParameterBinder.class);
			
			DialectOptions dialectOptions = new DialectOptions();
			dialectOptions.addTypeBinding(DummyType.class, "a SQL type", dummyParameterBinder);
			DialectBuilder testInstance = new DialectBuilder(defaultDatabaseVendorSettings, dialectOptions);
			
			Dialect dialect = testInstance.build();
			
			assertThat(dialect.getColumnBinderRegistry().getBinder(DummyType.class)).isEqualTo(dummyParameterBinder);
			
			T dummyTable = (T) new Table("Dummy");
			Column<T, DummyType> dummyColumn = dummyTable.addColumn("id", DummyType.class);
			assertThat(dialect.getSqlTypeRegistry().getTypeName(dummyColumn)).isEqualTo("a SQL type");
		}
		
		@Test
		void operatorPrintIsOverridden_itIsTakenIntoAccountInDeleteAndQuery() throws SQLException {
			DialectBuilder testInstance = new DialectBuilder(defaultDatabaseVendorSettings) {
				@Override
				protected QuerySQLBuilderFactoryBuilder createQuerySQLBuilderFactoryBuilder(DMLNameProviderFactory dmlNameProviderFactory, ColumnBinderRegistry columnBinderRegistry) {
					QuerySQLBuilderFactoryBuilder querySQLBuilderFactoryBuilder = super.createQuerySQLBuilderFactoryBuilder(dmlNameProviderFactory, columnBinderRegistry);
					querySQLBuilderFactoryBuilder.withOperatorSQLBuilderFactory(
							new OperatorSQLBuilderFactory() {
								@Override
								public OperatorSQLBuilder operatorSQLBuilder(FunctionSQLBuilder functionSQLBuilder) {
									return new OperatorSQLBuilder(functionSQLBuilder) {
										
										/** Overridden to write "like" in upper case, just to check and demonstrate how to branch some behavior on operator print */
										@Override
										public <V> void cat(Selectable<V> column, ConditionalOperator<?, V> operator, SQLAppender sql) {
											if (operator instanceof Like) {
												sql.cat("LIKE ").catValue(((Like) operator).getValue());
											} else {
												super.cat(column, operator, sql);
											}
										}
									};
								}
							});
					return querySQLBuilderFactoryBuilder;
				}
			};
			
			Dialect dialect = testInstance.build();
			
			Connection connectionMock = Mockito.mock(Connection.class);
			ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
			PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
			when(preparedStatementMock.executeQuery()).thenReturn(new InMemoryResultSet(Collections.emptySet()));
			when(connectionMock.prepareStatement(sqlCaptor.capture())).thenReturn(preparedStatementMock);
			PersistenceContext persistenceContext = new PersistenceContext(() -> connectionMock, dialect);
			Table dummyTable = new Table("dummyTable");
			Column dummyColumn = dummyTable.addColumn("dummyColumn", String.class);
			
			// Checking that operator override is taken into Query rendering
			persistenceContext.newQuery(QueryEase.select(dummyColumn).from(dummyTable).where(dummyColumn, like("x")), String.class)
					.execute(Accumulators.getFirst());
			assertThat(sqlCaptor.getValue()).isEqualTo("select dummyTable.dummyColumn from dummyTable where dummyTable.dummyColumn LIKE 'x'");
			
			// Checking that operator override is taken into Delete rendering
			persistenceContext.delete(dummyTable).where(dummyColumn, like("x")).execute();
			assertThat(sqlCaptor.getValue()).isEqualTo("delete from dummyTable where dummyColumn LIKE ?");
		}
		
		@Test
		void userDefinedOperatorCanBeTakenIntoAccountByOperatorSQLBuilderOverride() throws SQLException {
			class MyOperator extends UnitaryOperator<String> {
				
				public MyOperator(String value) {
					super(value);
				}
			}
			
			DialectBuilder testInstance = new DialectBuilder(defaultDatabaseVendorSettings) {
				@Override
				protected QuerySQLBuilderFactoryBuilder createQuerySQLBuilderFactoryBuilder(DMLNameProviderFactory dmlNameProviderFactory, ColumnBinderRegistry columnBinderRegistry) {
					QuerySQLBuilderFactoryBuilder querySQLBuilderFactoryBuilder = super.createQuerySQLBuilderFactoryBuilder(dmlNameProviderFactory, columnBinderRegistry);
					querySQLBuilderFactoryBuilder.withOperatorSQLBuilderFactory(
							new OperatorSQLBuilderFactory() {
								@Override
								public OperatorSQLBuilder operatorSQLBuilder(FunctionSQLBuilder functionSQLBuilder) {
									return new OperatorSQLBuilder(functionSQLBuilder) {
										
										/** Overridden to write "like" in upper case, just to check and demonstrate how to branch some behavior on operator print */
										@Override
										public <V> void cat(Selectable<V> column, ConditionalOperator<?, V> operator, SQLAppender sql) {
											if (operator instanceof MyOperator) {
												sql.cat("myOperator ").catValue(((MyOperator) operator).getValue());
											} else {
												super.cat(column, operator, sql);
											}
										}
									};
								}
							});
					return querySQLBuilderFactoryBuilder;
				}
			};
			
			Dialect dialect = testInstance.build();
			
			Connection connectionMock = Mockito.mock(Connection.class);
			ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
			PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
			when(preparedStatementMock.executeQuery()).thenReturn(new InMemoryResultSet(Collections.emptySet()));
			when(connectionMock.prepareStatement(sqlCaptor.capture())).thenReturn(preparedStatementMock);
			PersistenceContext persistenceContext = new PersistenceContext(() -> connectionMock, dialect);
			Table dummyTable = new Table("dummyTable");
			Column dummyColumn = dummyTable.addColumn("dummyColumn", String.class);
			
			// Checking that operator override is taken into Query rendering
			persistenceContext.newQuery(QueryEase.select(dummyColumn).from(dummyTable).where(dummyColumn, new MyOperator("42")), String.class)
					.execute(Accumulators.getFirst());
			assertThat(sqlCaptor.getValue()).isEqualTo("select dummyTable.dummyColumn from dummyTable where dummyTable.dummyColumn myOperator '42'");
			
			// Checking that operator override is taken into Delete rendering
			persistenceContext.delete(dummyTable).where(dummyColumn, new MyOperator("42")).execute();
			assertThat(sqlCaptor.getValue()).isEqualTo("delete from dummyTable where dummyColumn myOperator ?");
		}
	}
}