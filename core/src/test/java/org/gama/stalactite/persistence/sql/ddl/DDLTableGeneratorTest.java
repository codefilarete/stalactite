package org.gama.stalactite.persistence.sql.ddl;

import org.gama.lang.collection.Arrays;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.ForeignKey;
import org.gama.stalactite.persistence.structure.Index;
import org.gama.stalactite.persistence.structure.Table;
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
		
		t.addColumn("C", String.class).setPrimaryKey(true);
		generatedCreateTable = testInstance.generateCreateTable(t);
		assertEquals("create table Toto(A type, B type, C type, primary key (C))", generatedCreateTable);
		
		t.addColumn("D", Integer.TYPE);	// test isNullable
		generatedCreateTable = testInstance.generateCreateTable(t);
		assertEquals("create table Toto(A type, B type, C type, D type not null, primary key (C))", generatedCreateTable);
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
	}
}