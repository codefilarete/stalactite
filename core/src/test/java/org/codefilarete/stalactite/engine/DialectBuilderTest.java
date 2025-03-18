package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.DialectOptions;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory.DefaultGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.DDLSequenceGenerator;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.ColumnParameterizedSQL;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.DMLGenerator.NoopSorter;
import org.codefilarete.stalactite.sql.statement.PreparedUpdate;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.trace.MutableLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

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
	
	@Test
	<T extends Table<T>> void build_keywordsAreEscaped() {
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
		<T extends Table<T>> void withNewTypeBinding_bindingISApplied() {
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
	}
}