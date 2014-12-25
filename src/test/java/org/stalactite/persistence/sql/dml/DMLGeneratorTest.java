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
		String buildedInsert = testInstance.buildInsert(toto.getColumns());
		assertEquals(buildedInsert, "insert into Toto(A, B) values (?, ?)");
	}
	
	@Test
	public void testBuildUpdate() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		toto.new Column("B", String.class);
		
		DMLGenerator testInstance = new DMLGenerator();
		String buildedInsert = testInstance.buildUpdate(toto.getColumns(), Arrays.asList(colA));
		assertEquals(buildedInsert, "update Toto set A = ?, B = ? where A = ?");
	}
	
	@Test
	public void testBuildDelete() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		toto.new Column("B", String.class);
		
		DMLGenerator testInstance = new DMLGenerator();
		String buildedDelete = testInstance.buildDelete(toto, Arrays.asList(colA));
		assertEquals(buildedDelete, "delete Toto where A = ?");
	}
}