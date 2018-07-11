package org.gama.stalactite.persistence.structure;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Guillaume Mary
 */
public class TableTest {
	
	@Test
	public void testFindColumn() {
		Table testInstance = new Table("toto");
		// empty columns should throw any exception nor found anything
		assertNull(testInstance.findColumn("xx"));
		
		// basic case
		Column nameColumn = testInstance.addColumn("name", String.class);
		assertSame(nameColumn, testInstance.findColumn("name"));
		// xx still doesn't exist
		assertNull(testInstance.findColumn("xx"));
	}
	
	@Test
	public void testGetAbsoluteName() {
		Table testInstance = new Table("toto");
		Column nameColumn = testInstance.addColumn("name", String.class);
		assertEquals("toto.name", nameColumn.getAbsoluteName());
	}
	
	@Test
	public void testGetAlias() {
		Table testInstance = new Table("toto");
		Column nameColumn = testInstance.addColumn("name", String.class);
		assertEquals("toto_name", nameColumn.getAlias());
	}
}