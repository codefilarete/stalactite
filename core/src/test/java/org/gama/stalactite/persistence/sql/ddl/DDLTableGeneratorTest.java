package org.gama.stalactite.persistence.sql.ddl;

import org.gama.lang.collection.KeepOrderSet;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.persistence.structure.Table.ForeignKey;
import org.gama.stalactite.persistence.structure.Table.Index;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DDLTableGeneratorTest {
	
	@Test
	public void testGenerateCreateTable() throws Exception {
		DDLTableGenerator testInstance = new DDLTableGenerator(null) {
			@Override
			protected String getSqlType(Column column) {
				return "type";
			}
		};
		
		Table t = new Table(null, "Toto");
		
		t.new Column("A", String.class);
		String generatedCreateTable = testInstance.generateCreateTable(t);
		assertEquals("create table Toto(A type)", generatedCreateTable);
		
		t.new Column("B", String.class);
		generatedCreateTable = testInstance.generateCreateTable(t);
		assertEquals("create table Toto(A type, B type)", generatedCreateTable);
		
		t.new Column("C", String.class).setPrimaryKey(true);
		generatedCreateTable = testInstance.generateCreateTable(t);
		assertEquals("create table Toto(A type, B type, C type, primary key (C))", generatedCreateTable);
		
		t.new Column("D", Integer.TYPE);	// test isNullable
		generatedCreateTable = testInstance.generateCreateTable(t);
		assertEquals("create table Toto(A type, B type, C type, D type not null, primary key (C))", generatedCreateTable);
	}
	
	@Test
	public void testGenerateCreateIndex() throws Exception {
		DDLTableGenerator testInstance = new DDLTableGenerator(null);
		
		Table t = new Table(null, "Toto");
		Column colA = t.new Column("A", String.class);
		Column colB = t.new Column("B", String.class);
		
		Index idx1 = t.new Index(colA, "Idx1");
		String generatedCreateIndex = testInstance.generateCreateIndex(idx1);
		assertEquals("create index Idx1 on Toto(A)", generatedCreateIndex);
		
		Index idx2 = t.new Index(new KeepOrderSet<>(colA, colB), "Idx2");
		generatedCreateIndex = testInstance.generateCreateIndex(idx2);
		assertEquals("create index Idx2 on Toto(A, B)", generatedCreateIndex);
	}
	
	@Test
	public void testGenerateCreateForeignKey() throws Exception {
		DDLTableGenerator testInstance = new DDLTableGenerator(null);
		
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		Column colB = toto.new Column("B", String.class);
		
		Table titi = new Table(null, "Titi");
		Column colA2 = titi.new Column("A", String.class);
		Column colB2 = titi.new Column("B", String.class);
		
		ForeignKey foreignKey = toto.new ForeignKey(colA, "FK1", colA2);
		String generatedCreateIndex = testInstance.generateCreateForeignKey(foreignKey);
		assertEquals("alter table Toto add constraint FK1 foreign key(A) references Titi(A)", generatedCreateIndex);
		
		foreignKey = toto.new ForeignKey(new KeepOrderSet<>(colA, colB), "FK1", new KeepOrderSet<>(colA2, colB2));
		generatedCreateIndex = testInstance.generateCreateForeignKey(foreignKey);
		assertEquals("alter table Toto add constraint FK1 foreign key(A, B) references Titi(A, B)", generatedCreateIndex);
	}
}