package org.gama.stalactite.persistence.structure;

import org.gama.lang.collection.Arrays;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

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
	
	@Test
	public void testGetPrimaryKey() {
		Table testInstance = new Table("toto");
		assertNull(testInstance.getPrimaryKey());
		
		Column dummyColumn = testInstance.addColumn("dummyColumn", String.class);
		assertNull(testInstance.getPrimaryKey());
		
		Column id = testInstance.addColumn("id", long.class);
		id.primaryKey();
		Column subId = testInstance.addColumn("subId", long.class);
		subId.primaryKey();
		assertNotNull(testInstance.getPrimaryKey());
		assertIterableEquals(Arrays.asList(id, subId), testInstance.getPrimaryKey().getColumns());
	}
}