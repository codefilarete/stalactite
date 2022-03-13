package org.codefilarete.stalactite.persistence.sql.ddl;

import java.util.List;

import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
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
		DDLGenerator testInstance = new DDLGenerator(new SqlTypeRegistry(typeMapping));
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
	void overridenColumnTypeIsTakenIntoAccount() {
		JavaTypeToSqlTypeMapping typeMapping = new JavaTypeToSqlTypeMapping();
		typeMapping.put(String.class, "VARCHAR");
		SqlTypeRegistry sqlTypeRegistry = new SqlTypeRegistry(typeMapping);
		DDLGenerator testInstance = new DDLGenerator(sqlTypeRegistry);
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
		DDLGenerator testInstance = new DDLGenerator(new SqlTypeRegistry(typeMapping));
		Table totoTable = new Table("toto");
		Column idColumn = totoTable.addColumn("id", String.class);
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
	void foreignKeysAreInFinalScript() {
		JavaTypeToSqlTypeMapping typeMapping = new JavaTypeToSqlTypeMapping();
		typeMapping.put(String.class, "VARCHAR");
		DDLGenerator testInstance = new DDLGenerator(new SqlTypeRegistry(typeMapping));
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
	void addedDDLGeneratorsAreInFinalScript() {
		JavaTypeToSqlTypeMapping typeMapping = new JavaTypeToSqlTypeMapping();
		typeMapping.put(String.class, "VARCHAR");
		DDLGenerator testInstance = new DDLGenerator(new SqlTypeRegistry(typeMapping));
		Table totoTable = new Table("toto");
		totoTable.addColumn("id", String.class);
		testInstance.addTables(totoTable);
		
		testInstance.addDDLGenerators(new DDLProvider() {
			@Override
			public List<String> getCreationScripts() {
				return Arrays.asList("my wonderfull first SQL creation script", "my wonderfull second SQL creation script");
			}
			
			@Override
			public List<String> getDropScripts() {
				return Arrays.asList("my wonderfull first SQL drop script", "my wonderfull second SQL drop script");
			}
		});
		
		assertThat(testInstance.getCreationScripts()).isEqualTo(Arrays.asList(
				"create table toto(id VARCHAR)",
				"my wonderfull first SQL creation script",
				"my wonderfull second SQL creation script"
		));
		assertThat(testInstance.getDropScripts()).isEqualTo(Arrays.asList(
				"drop table toto",
				"my wonderfull first SQL drop script",
				"my wonderfull second SQL drop script"
		));
	}
	
}