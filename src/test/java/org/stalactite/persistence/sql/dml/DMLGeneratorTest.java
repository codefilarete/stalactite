package org.stalactite.persistence.sql.dml;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;

import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.testng.annotations.Test;

public class DMLGeneratorTest {
	
	@Test
	public void testBuildInsert() throws Exception {
		Table toto = new Table(null, "Toto");
		toto.new Column("A", String.class);
		toto.new Column("B", String.class);
		
		DMLGenerator testInstance = new DMLGenerator();
		CRUDStatement buildedInsert = testInstance.buildInsert(toto.getColumns());
		assertEquals(buildedInsert.getSql(), "insert into Toto(A, B) values (?, ?)");
	}
	
	@Test
	public void testBuildUpdate() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		toto.new Column("B", String.class);
		
		DMLGenerator testInstance = new DMLGenerator();
		CRUDStatement buildedInsert = testInstance.buildUpdate(toto.getColumns(), Arrays.asList(colA));
		assertEquals(buildedInsert.getSql(), "update Toto set A = ?, B = ? where A = ?");
	}
	
	@Test
	public void testBuildDelete() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		toto.new Column("B", String.class);
		
		DMLGenerator testInstance = new DMLGenerator();
		CRUDStatement buildedDelete = testInstance.buildDelete(toto, Arrays.asList(colA));
		assertEquals(buildedDelete.getSql(), "delete Toto where A = ?");
	}
	
	@Test
	public void testBuildSelect() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		Column colB = toto.new Column("B", String.class);
		
		DMLGenerator testInstance = new DMLGenerator();
		CRUDStatement buildedSelect = testInstance.buildSelect(toto, Arrays.asList(colA, colB), Arrays.asList(colA));
		assertEquals(buildedSelect.getSql(), "select A, B from Toto where A = ?");
	}
}