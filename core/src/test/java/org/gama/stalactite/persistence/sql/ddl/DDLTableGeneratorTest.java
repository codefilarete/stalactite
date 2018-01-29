package org.gama.stalactite.persistence.sql.ddl;

import javax.annotation.Nonnull;
import java.util.Collections;

import org.gama.lang.collection.Arrays;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.ForeignKey;
import org.gama.stalactite.persistence.structure.Index;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.DMLNameProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DDLTableGeneratorTest {
	
	@Test
	public void testGenerateCreateTable() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null) {
			@Override
			protected String getSqlType(Column column) {
				return "type";
			}
		};
		
		Table t = new Table(null, "Toto");
		
		t.addColumn("A", String.class);
		String generatedCreateTable = testInstance.generateCreateTable(t);
		assertEquals("create table Toto(A type)", generatedCreateTable);
		
		t.addColumn("B", String.class);
		generatedCreateTable = testInstance.generateCreateTable(t);
		assertEquals("create table Toto(A type, B type)", generatedCreateTable);
		
		Column<String> primaryKey = t.addColumn("C", String.class);
		primaryKey.setPrimaryKey(true);
		generatedCreateTable = testInstance.generateCreateTable(t);
		assertEquals("create table Toto(A type, B type, C type, primary key (C))", generatedCreateTable);
		
		t.addColumn("D", Integer.TYPE);	// test isNullable
		generatedCreateTable = testInstance.generateCreateTable(t);
		assertEquals("create table Toto(A type, B type, C type, D type not null, primary key (C))", generatedCreateTable);
		
		// test with a non default DMLNameProvider
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == primaryKey) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		t.addColumn("D", Integer.TYPE);	// test isNullable
		testInstance = new DDLTableGenerator(null, dmlNameProvider) {
			@Override
			protected String getSqlType(Column column) {
				return "type";
			}
		};
		
		generatedCreateTable = testInstance.generateCreateTable(t);
		assertEquals("create table Toto(A type, B type, 'key' type, D type not null, primary key ('key'))", generatedCreateTable);
	}
	
	@Test
	public void testGenerateDropTable() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null);
		
		Table toto = new Table(null, "Toto");
		
		String generateDropTable = testInstance.generateDropTable(toto);
		assertEquals("drop table Toto", generateDropTable);
		
		// test with a non default DMLNameProvider
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Table table) {
				if (table == toto) {
					return "'user'";
				}
				return super.getSimpleName(toto);
			}
		};
		testInstance = new DDLTableGenerator(null, dmlNameProvider);
		
		generateDropTable = testInstance.generateDropTable(toto);
		assertEquals("drop table 'user'", generateDropTable);
	}
	
	@Test
	public void testGenerateDropTableIfExists() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null);
		
		Table toto = new Table(null, "Toto");
		
		String generateDropTable = testInstance.generateDropTableIfExists(toto);
		assertEquals("drop table if exists Toto", generateDropTable);
		
		// test with a non default DMLNameProvider
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Table table) {
				if (table == toto) {
					return "'user'";
				}
				return super.getSimpleName(toto);
			}
		};
		testInstance = new DDLTableGenerator(null, dmlNameProvider);
		
		generateDropTable = testInstance.generateDropTableIfExists(toto);
		assertEquals("drop table if exists 'user'", generateDropTable);
	}
	
	@Test
	public void testGenerateAddColumn() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null) {
			@Override
			protected String getSqlType(Column column) {
				return "type";
			}
		};
		
		Table t = new Table(null, "Toto");
		
		Column<String> newColumn = t.addColumn("A", String.class);
		String generateAddColumn = testInstance.generateAddColumn(newColumn);
		assertEquals("alter table Toto add column A type", generateAddColumn);
		
		// test with a non default DMLNameProvider
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == newColumn) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		testInstance = new DDLTableGenerator(null, dmlNameProvider) {
			@Override
			protected String getSqlType(Column column) {
				return "type";
			}
		};
		
		generateAddColumn = testInstance.generateAddColumn(newColumn);
		assertEquals("alter table Toto add column 'key' type", generateAddColumn);
	}
	
	@Test
	public void testGenerateDropColumn() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null) {
			@Override
			protected String getSqlType(Column column) {
				return "type";
			}
		};
		
		Table t = new Table(null, "Toto");
		
		Column<String> newColumn = t.addColumn("A", String.class);
		String generateDropColumn = testInstance.generateDropColumn(newColumn);
		assertEquals("alter table Toto drop column A", generateDropColumn);
		
		// test with a non default DMLNameProvider
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == newColumn) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		testInstance = new DDLTableGenerator(null, dmlNameProvider) {
			@Override
			protected String getSqlType(Column column) {
				return "type";
			}
		};
		
		generateDropColumn = testInstance.generateDropColumn(newColumn);
		assertEquals("alter table Toto drop column 'key'", generateDropColumn);
	}
	
	@Test
	public void testGenerateCreateIndex() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null);
		
		Table t = new Table(null, "Toto");
		Column colA = t.addColumn("A", String.class);
		Column colB = t.addColumn("B", String.class);
		
		Index idx1 = t.addIndex("Idx1", colA);
		String generatedCreateIndex = testInstance.generateCreateIndex(idx1);
		assertEquals("create index Idx1 on Toto(A)", generatedCreateIndex);
		
		Index idx2 = t.addIndex("Idx2", colA, colB);
		generatedCreateIndex = testInstance.generateCreateIndex(idx2);
		assertEquals("create index Idx2 on Toto(A, B)", generatedCreateIndex);

		// test with a non default DMLNameProvider
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == colA) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		t.addColumn("D", Integer.TYPE);	// test isNullable
		testInstance = new DDLTableGenerator(null, dmlNameProvider);
		generatedCreateIndex = testInstance.generateCreateIndex(idx2);
		assertEquals("create index Idx2 on Toto('key', B)", generatedCreateIndex);
	}
	
	@Test
	public void testGenerateDropIndex() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null);
		
		Table t = new Table(null, "Toto");
		Column<String> colA = t.addColumn("A", String.class);
		
		Index idx = t.addIndex("idx1", colA);
		
		String generateDropIndex = testInstance.generateDropIndex(idx);
		assertEquals("drop index idx1", generateDropIndex);
		
		// test with a non default DMLNameProvider
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == colA) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		testInstance = new DDLTableGenerator(null, dmlNameProvider) {
			@Override
			protected String getSqlType(Column column) {
				return "type";
			}
		};
		
		generateDropIndex = testInstance.generateDropIndex(idx);
		assertEquals("drop index idx1", generateDropIndex);
	}
	
	@Test
	public void testGenerateCreateForeignKey() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null);
		
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		Column colB = toto.addColumn("B", String.class);
		
		Table titi = new Table(null, "Titi");
		Column colA2 = titi.addColumn("A", String.class);
		Column colB2 = titi.addColumn("B", String.class);
		
		ForeignKey foreignKey = toto.addForeignKey("FK1", colA, colA2);
		String generatedCreateIndex = testInstance.generateCreateForeignKey(foreignKey);
		assertEquals("alter table Toto add constraint FK1 foreign key(A) references Titi(A)", generatedCreateIndex);
		
		foreignKey = toto.addForeignKey("FK1", Arrays.asList(colA, colB), Arrays.asList(colA2, colB2));
		generatedCreateIndex = testInstance.generateCreateForeignKey(foreignKey);
		assertEquals("alter table Toto add constraint FK1 foreign key(A, B) references Titi(A, B)", generatedCreateIndex);

		// test with a non default DMLNameProvider
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == colA || column == colA2) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		testInstance = new DDLTableGenerator(null, dmlNameProvider);
		generatedCreateIndex = testInstance.generateCreateForeignKey(foreignKey);
		assertEquals("alter table Toto add constraint FK1 foreign key('key', B) references Titi('key', B)", generatedCreateIndex);
	}
	
	@Test
	public void testGenerateDropForeignKey() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null);
		
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		
		Table titi = new Table(null, "Titi");
		Column colA2 = titi.addColumn("A", String.class);
		
		ForeignKey foreignKey = toto.addForeignKey("FK1", colA, colA2);
		
		String generateDropForeignKey = testInstance.generateDropForeignKey(foreignKey);
		assertEquals("alter table Toto drop constraint FK1", generateDropForeignKey);
		
		// test with a non default DMLNameProvider
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == colA) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		testInstance = new DDLTableGenerator(null, dmlNameProvider) {
			@Override
			protected String getSqlType(Column column) {
				return "type";
			}
		};
		
		generateDropForeignKey = testInstance.generateDropForeignKey(foreignKey);
		assertEquals("alter table Toto drop constraint FK1", generateDropForeignKey);
	}
}