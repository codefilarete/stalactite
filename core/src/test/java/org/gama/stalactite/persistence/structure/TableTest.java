package org.gama.stalactite.persistence.structure;

import org.gama.lang.collection.Arrays;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Guillaume Mary
 */
public class TableTest {
	
	@Test
	public void addColumn_alreadyExists_returnsExistingOne() {
		Table testInstance = new Table("toto");
		// empty table shouldn't throw any exception nor found anything
		Column xxColumn = testInstance.addColumn("xx", String.class);
		
		// same column with same type doesn't has any consequence
		Column newColumn = testInstance.addColumn("xx", String.class);
		assertSame(xxColumn, newColumn);
	}
	
	@Test
	public void addColumn_alreadyExistsWithDifferentType_throwsException() {
		Table testInstance = new Table("toto");
		// empty table shouldn't throw any exception nor found anything
		testInstance.addColumn("xx", String.class);
		
		// same column with other type throws exception
		IllegalArgumentException thrownException = assertThrows(IllegalArgumentException.class, () -> testInstance.addColumn("xx", Integer.class));
		assertEquals("Trying to add a column that already exists with a different type : toto.xx j.l.String vs j.l.Integer", thrownException.getMessage());
		// same column with other type throws exception
		thrownException = assertThrows(IllegalArgumentException.class, () -> testInstance.addColumn("xx", String.class, 12));
		assertEquals("Trying to add a column that already exists with a different type : toto.xx j.l.String vs j.l.String(12)", thrownException.getMessage());
	}
	
	@Test
	public void addForeignKey_alreadyExists_returnsExistingOne() {
		Table testInstance = new Table("toto");
		Column xColumn = testInstance.addColumn("x", String.class);
		Table referencedInstance = new Table("tata");
		Column yColumn = referencedInstance.addColumn("y", String.class);
		// empty table shouldn't throw any exception nor found anything
		ForeignKey fk = testInstance.addForeignKey("dummy FK name", xColumn, yColumn);
		
		// same column with same type doesn't has any consequence
		ForeignKey newFK = testInstance.addForeignKey("dummy FK name", xColumn, yColumn);
		assertSame(fk, newFK);
	}
	
	@Test
	public void addForeignKey_alreadyExistsWithDifferentType_throwsException() {
		Table testInstance = new Table("toto");
		Column xColumn = testInstance.addColumn("x", String.class);
		Column xxColumn = testInstance.addColumn("xx", String.class);
		Table referencedInstance = new Table("tata");
		Column yColumn = referencedInstance.addColumn("y", String.class);
		// empty table shouldn't throw any exception nor found anything
		testInstance.addForeignKey("dummy FK name", xColumn, yColumn);
		
		// same column with other type throws exception
		IllegalArgumentException thrownException = assertThrows(IllegalArgumentException.class, () -> testInstance.addForeignKey("dummy FK name", xxColumn, yColumn));
		assertEquals("Trying to add a foreign key that already exists with different columns : dummy FK name toto.x -> tata.y vs toto.xx -> tata.y", thrownException.getMessage());
	}
	
	@Test
	public void findColumn() {
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
	public void getAbsoluteName() {
		Table testInstance = new Table("toto");
		Column nameColumn = testInstance.addColumn("name", String.class);
		assertEquals("toto.name", nameColumn.getAbsoluteName());
	}
	
	@Test
	public void getAlias() {
		Table testInstance = new Table("toto");
		Column nameColumn = testInstance.addColumn("name", String.class);
		assertEquals("toto_name", nameColumn.getAlias());
	}
	
	@Test
	public void getPrimaryKey() {
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
	
	@Test
	public void getColumnsPrimaryKey() {
		Table testInstance = new Table("toto");
		assertNull(testInstance.getPrimaryKey());
		
		Column dummyColumn = testInstance.addColumn("dummyColumn", String.class);
		Column id = testInstance.addColumn("id", long.class).primaryKey();
		assertEquals(Arrays.asSet(id), testInstance.getPrimaryKey().getColumns());
		assertIterableEquals(Arrays.asList(dummyColumn), testInstance.getColumnsNoPrimaryKey());
	}
	
	@Test
	public void addForeignKey() {
		Table testInstance1 = new Table("toto");
		Column tataId = testInstance1.addColumn("tataId", Integer.class);
		Table testInstance2 = new Table("tata");
		Column id = testInstance2.addColumn("id", Integer.class);
		
		ForeignKey createdFK = testInstance1.addForeignKey("XX", tataId, id);
		
		assertEquals("XX", createdFK.getName());
		assertEquals(Arrays.asSet(tataId), createdFK.getColumns());
		assertEquals(testInstance1, createdFK.getTable());
		assertEquals(Arrays.asSet(id), createdFK.getTargetColumns());
		assertEquals(testInstance2, createdFK.getTargetTable());
	}
	
	@Test
	public void addForeignKey_withNamingFunction() {
		Table<?> testInstance1 = new Table("toto");
		Column tataId = testInstance1.addColumn("tataId", Integer.class);
		Table testInstance2 = new Table("tata");
		Column id = testInstance2.addColumn("id", Integer.class);
		
		ForeignKey createdFK = testInstance1.addForeignKey((c1, c2) -> c1.getName() + "_" + c2.getName(), tataId, id);
		
		assertEquals("tataId_id", createdFK.getName());
		assertEquals(Arrays.asSet(tataId), createdFK.getColumns());
		assertEquals(testInstance1, createdFK.getTable());
		assertEquals(Arrays.asSet(id), createdFK.getTargetColumns());
		assertEquals(testInstance2, createdFK.getTargetTable());
	}
}