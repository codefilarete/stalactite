package org.gama.stalactite.persistence.sql.ddl;

import java.util.List;

import org.gama.lang.collection.Arrays;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Guillaume Mary
 */
class DDLGeneratorTest {
	
	@Test
	void addedTablesAreInFinalScript() {
		JavaTypeToSqlTypeMapping typeMapping = new JavaTypeToSqlTypeMapping();
		typeMapping.put(String.class, "VARCHAR");
		DDLGenerator testInstance = new DDLGenerator(typeMapping);
		Table totoTable = new Table("toto");
		totoTable.addColumn("id", String.class);
		testInstance.addTables(totoTable);
		
		assertEquals(Arrays.asList("create table toto(id VARCHAR)"), testInstance.getCreationScripts());
		assertEquals(Arrays.asList("drop table toto"), testInstance.getDropScripts());
		
		totoTable.addColumn("name", String.class);
		assertEquals(Arrays.asList("create table toto(id VARCHAR, name VARCHAR)"), testInstance.getCreationScripts());
		assertEquals(Arrays.asList("drop table toto"), testInstance.getDropScripts());
	}
	
	@Test
	void indexesAreInFinalScript() {
		JavaTypeToSqlTypeMapping typeMapping = new JavaTypeToSqlTypeMapping();
		typeMapping.put(String.class, "VARCHAR");
		DDLGenerator testInstance = new DDLGenerator(typeMapping);
		Table totoTable = new Table("toto");
		Column idColumn = totoTable.addColumn("id", String.class);
		totoTable.addIndex("totoIDX", idColumn);
		testInstance.addTables(totoTable);
		
		assertEquals(Arrays.asList(
				"create table toto(id VARCHAR)",
				"create index totoIDX on toto(id)"
				), testInstance.getCreationScripts());
		// by default indexes are not in final script because they are expected to be dropped with table
		assertEquals(Arrays.asList(
				"drop table toto"
				), testInstance.getDropScripts());
	}
	
	@Test
	void foreignKeysAreInFinalScript() {
		JavaTypeToSqlTypeMapping typeMapping = new JavaTypeToSqlTypeMapping();
		typeMapping.put(String.class, "VARCHAR");
		DDLGenerator testInstance = new DDLGenerator(typeMapping);
		Table totoTable = new Table("toto");
		Column totoPKColumn = totoTable.addColumn("id", String.class);
		Table tataTable = new Table("tata");
		Column tataPKColumn = tataTable.addColumn("id", String.class);
		totoTable.addForeignKey("totoFK", totoPKColumn, tataPKColumn);
		testInstance.addTables(totoTable, tataTable);
		
		assertEquals(Arrays.asList(
				"create table toto(id VARCHAR)",
				"create table tata(id VARCHAR)",
				"alter table toto add constraint totoFK foreign key(id) references tata(id)"
				), testInstance.getCreationScripts());
		// by default foreignKeys are not in final script because they are expected to be dropped with table
		assertEquals(Arrays.asList(
				"drop table toto",
				"drop table tata"
		), testInstance.getDropScripts());
	}
	
	@Test
	void addedDDLGeneratorsAreInFinalScript() {
		JavaTypeToSqlTypeMapping typeMapping = new JavaTypeToSqlTypeMapping();
		typeMapping.put(String.class, "VARCHAR");
		DDLGenerator testInstance = new DDLGenerator(typeMapping);
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
		
		assertEquals(Arrays.asList(
				"create table toto(id VARCHAR)",
				"my wonderfull first SQL creation script",
				"my wonderfull second SQL creation script"
				), testInstance.getCreationScripts());
		assertEquals(Arrays.asList(
				"drop table toto",
				"my wonderfull first SQL drop script",
				"my wonderfull second SQL drop script"
				), testInstance.getDropScripts());
	}
	
}