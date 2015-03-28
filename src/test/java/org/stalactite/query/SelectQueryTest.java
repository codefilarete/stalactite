package org.stalactite.query;

import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.stalactite.query.model.CriteriaSuite;
import org.stalactite.query.model.SelectQuery;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class SelectQueryTest {

	private Table tableA, tableB;
	private Column columnX, columnY;

	@BeforeMethod
	public void setUp() {
		tableA = new Table(null, "A");
		columnX = tableA.new Column("x", String.class);
		tableB = new Table(null, "B");
		columnY = tableB.new Column("y", String.class);
	}
	
	@Test
	public void testSelect_rawJoin() {
		SelectQuery testInstance = new SelectQuery();
		testInstance.select("a").add("b");
		testInstance.select("a").from(tableA, tableB, "x = y");
		testInstance.select("a", "b").from(tableA, tableB, "x = y");
		testInstance.select("a").add("b").from(tableA, tableB, "x = y");
		testInstance.select(columnX).from(tableA, tableB, "x = y");
		testInstance.select(columnX, columnY).from(tableA, tableB, "x = y");
		testInstance.select(columnX).add(columnY).from(tableA, tableB, "x = y");
		testInstance.select(columnX).add("b").from(tableA, tableB, "x = y");
		
		testInstance.select("fds").from(tableA, "a", tableB, "b", "a.x = b.y");
	}
	
	@Test
	public void testSelect_columnJoin() {
		SelectQuery testInstance = new SelectQuery();
		testInstance.select("a").from(columnX, columnY);
		testInstance.from(columnX, columnY);
	}

	@Test
	public void testSelect_where() {
		SelectQuery testInstance = new SelectQuery();
		testInstance.select("fds").from(columnX, columnY).where(columnX, "= ?").and(columnY, " = ?")
				.or(columnY, " = ?").or(new CriteriaSuite(columnY, "= 12").and(columnX, "= 4"))
				.groupBy(columnX);
		testInstance.groupBy(columnX, columnY).add(columnY).having(columnX, "> 2");
	}

}