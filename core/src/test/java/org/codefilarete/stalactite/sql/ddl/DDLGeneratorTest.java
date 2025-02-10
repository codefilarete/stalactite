package org.codefilarete.stalactite.sql.ddl;

import java.util.List;

import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class DDLGeneratorTest {
	
	@Test
	void addedTablesAreInFinalScript() {
		JavaTypeToSqlTypeMapping typeMapping = new JavaTypeToSqlTypeMapping();
		typeMapping.put(String.class, "VARCHAR");
		DDLGenerator testInstance = new DDLGenerator(new SqlTypeRegistry(typeMapping), DMLNameProvider::new);
		Table totoTable = new Table("toto");
		totoTable.addColumn("id", String.class);
		testInstance.addTables(totoTable);
		
		assertThat(testInstance.getCreationScripts()).isEqualTo(Arrays.asList("create table toto(id VARCHAR)"));
		assertThat(testInstance.getDropScripts()).isEqualTo(Arrays.asList("drop table toto"));
		
		totoTable.addColumn("name", String.class);
		assertThat(testInstance.getCreationScripts()).isEqualTo(Arrays.asList("create table toto(id VARCHAR, name VARCHAR)"));
		assertThat(testInstance.getDropScripts()).isEqualTo(Arrays.asList("drop table toto"));
	}
	
	@Test
	void overriddenColumnTypeIsTakenIntoAccount() {
		JavaTypeToSqlTypeMapping typeMapping = new JavaTypeToSqlTypeMapping();
		typeMapping.put(String.class, "VARCHAR");
		SqlTypeRegistry sqlTypeRegistry = new SqlTypeRegistry(typeMapping);
		DDLGenerator testInstance = new DDLGenerator(sqlTypeRegistry, DMLNameProvider::new);
		Table totoTable = new Table("toto");
		Column idColumn = totoTable.addColumn("id", String.class);
		testInstance.addTables(totoTable);
		sqlTypeRegistry.put(idColumn, "BLOB");
		
		assertThat(testInstance.getCreationScripts()).isEqualTo(Arrays.asList("create table toto(id BLOB)"));
	}
	
	@Test
	void indexesAreInFinalScript() {
		JavaTypeToSqlTypeMapping typeMapping = new JavaTypeToSqlTypeMapping();
		typeMapping.put(String.class, "VARCHAR");
		DDLGenerator testInstance = new DDLGenerator(new SqlTypeRegistry(typeMapping), DMLNameProvider::new);
		Table totoTable = new Table<>("toto");
		Column<Table, String> idColumn = totoTable.addColumn("id", String.class);
		totoTable.addIndex("totoIDX", idColumn);
		testInstance.addTables(totoTable);
		
		assertThat(testInstance.getCreationScripts()).isEqualTo(Arrays.asList(
				"create table toto(id VARCHAR)",
				"create index totoIDX on toto(id)"
		));
		// by default indexes are not in final script because they are expected to be dropped with table
		assertThat(testInstance.getDropScripts()).isEqualTo(Arrays.asList(
				"drop table toto"
		));
	}
	
	@Test
	void uniqueConstraintsAreInFinalScript() {
		JavaTypeToSqlTypeMapping typeMapping = new JavaTypeToSqlTypeMapping();
		typeMapping.put(String.class, "VARCHAR");
		DDLGenerator testInstance = new DDLGenerator(new SqlTypeRegistry(typeMapping), DMLNameProvider::new);
		Table totoTable = new Table<>("toto");
		Column<Table, String> idColumn = totoTable.addColumn("id", String.class);
		Column<Table, String> lastNameColumn = totoTable.addColumn("lastName", String.class);
		totoTable.addUniqueConstraint("UK_toto", idColumn, lastNameColumn);
		testInstance.addTables(totoTable);
		
		assertThat(testInstance.getCreationScripts()).isEqualTo(Arrays.asList(
				"create table toto(id VARCHAR, lastName VARCHAR)",
				"alter table toto add constraint UK_toto unique (id, lastName)"
		));
		// by default indexes are not in final script because they are expected to be dropped with table
		assertThat(testInstance.getDropScripts()).isEqualTo(Arrays.asList(
				"drop table toto"
		));
	}
	
	@Test
	void foreignKeysAreInFinalScript() {
		JavaTypeToSqlTypeMapping typeMapping = new JavaTypeToSqlTypeMapping();
		typeMapping.put(String.class, "VARCHAR");
		DDLGenerator testInstance = new DDLGenerator(new SqlTypeRegistry(typeMapping), DMLNameProvider::new);
		Table totoTable = new Table("toto");
		Column totoPKColumn = totoTable.addColumn("id", String.class);
		Table tataTable = new Table("tata");
		Column tataPKColumn = tataTable.addColumn("id", String.class);
		totoTable.addForeignKey("totoFK", totoPKColumn, tataPKColumn);
		testInstance.addTables(totoTable, tataTable);
		
		assertThat(testInstance.getCreationScripts()).isEqualTo(Arrays.asList(
				"create table toto(id VARCHAR)",
				"create table tata(id VARCHAR)",
				"alter table toto add constraint totoFK foreign key(id) references tata(id)"
		));
		// by default foreignKeys are not in final script because they are expected to be dropped with table
		assertThat(testInstance.getDropScripts()).isEqualTo(Arrays.asList(
				"drop table toto",
				"drop table tata"
		));
	}
	
	@Test
	void sequencesAreInFinalScript() {
		JavaTypeToSqlTypeMapping typeMapping = new JavaTypeToSqlTypeMapping();
		typeMapping.put(String.class, "VARCHAR");
		DDLGenerator testInstance = new DDLGenerator(new SqlTypeRegistry(typeMapping), DMLNameProvider::new);
		Sequence totoSequence = new Sequence("toto");
		Sequence titiSequence = new Sequence("titi")
				.withInitialValue(4)
				.withBatchSize(50);
		testInstance.addSequences(totoSequence, titiSequence);

		assertThat(testInstance.getCreationScripts()).isEqualTo(Arrays.asList(
				"create sequence toto",
				"create sequence titi start with 4 increment by 50"
		));
		// by default foreignKeys are not in final script because they are expected to be dropped with table
		assertThat(testInstance.getDropScripts()).isEqualTo(Arrays.asList(
				"drop sequence toto",
				"drop sequence titi"
		));
	}
	
	@Test
	void addedDDLProvidersAreInFinalScript() {
		JavaTypeToSqlTypeMapping typeMapping = new JavaTypeToSqlTypeMapping();
		typeMapping.put(String.class, "VARCHAR");
		DDLGenerator testInstance = new DDLGenerator(new SqlTypeRegistry(typeMapping), DMLNameProvider::new);
		Table totoTable = new Table("toto");
		totoTable.addColumn("id", String.class);
		testInstance.addTables(totoTable);
		
		testInstance.addDDLProviders(new DDLProvider() {
			@Override
			public List<String> getCreationScripts() {
				return Arrays.asList("my wonderful first SQL creation script", "my wonderful second SQL creation script");
			}
			
			@Override
			public List<String> getDropScripts() {
				return Arrays.asList("my wonderful first SQL drop script", "my wonderful second SQL drop script");
			}
		});
		
		assertThat(testInstance.getCreationScripts()).isEqualTo(Arrays.asList(
				"create table toto(id VARCHAR)",
				"my wonderful first SQL creation script",
				"my wonderful second SQL creation script"
		));
		assertThat(testInstance.getDropScripts()).isEqualTo(Arrays.asList(
				"drop table toto",
				"my wonderful first SQL drop script",
				"my wonderful second SQL drop script"
		));
	}
	
}