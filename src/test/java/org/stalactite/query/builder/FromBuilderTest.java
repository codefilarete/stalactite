package org.stalactite.query.builder;

import static org.junit.Assert.assertEquals;

import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.stalactite.query.model.From;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class FromBuilderTest {
	
	private static final String TEST_TO_SQL_DATA = "testToSQLData";

	@DataProvider(name = TEST_TO_SQL_DATA)
	public Object[][] testToSQL_data() {
		Table tableToto = new Table(null, "Toto");
		Column colTotoA = tableToto.new Column("a", String.class);
		Column colTotoB = tableToto.new Column("b", String.class);
		Table tableTata = new Table(null, "Tata");
		Column colTataA = tableTata.new Column("a", String.class);
		Column colTataB = tableTata.new Column("b", String.class);
		Table tableTutu = new Table(null, "Tutu");
		Column colTutuA = tableTutu.new Column("a", String.class);
		Column colTutuB = tableTutu.new Column("b", String.class);
		
		return new Object[][] {
				// test écriture From avec des Tables
				// 1 jointure, avec ou sans alias
				{ new From().innerJoin(tableToto, tableTata, "Toto.id = Tata.id"), "Toto inner join Tata on Toto.id = Tata.id" },
				{ new From().innerJoin(tableToto, "to", tableTata, "ta", "to.id = ta.id"), "Toto as to inner join Tata as ta on to.id = ta.id" },
				{ new From().leftOuterJoin(tableToto, tableTata, "Toto.id = Tata.id"), "Toto left outer join Tata on Toto.id = Tata.id" },
				{ new From().leftOuterJoin(tableToto, "to", tableTata, "ta", "to.id = ta.id"), "Toto as to left outer join Tata as ta on to.id = ta.id" },
				{ new From().rightOuterJoin(tableToto, tableTata, "Toto.id = Tata.id"), "Toto right outer join Tata on Toto.id = Tata.id" },
				{ new From().rightOuterJoin(tableToto, "to", tableTata, "ta", "to.id = ta.id"), "Toto as to right outer join Tata as ta on to.id = ta.id" },
				// 2 jointures, avec ou sans alias
				{ new From().innerJoin(tableToto, tableTata, "Toto.a = Tata.a").innerJoin(tableToto, tableTutu, "Toto.b = Tutu.b"),
						"Toto inner join Tata on Toto.a = Tata.a inner join Tutu on Toto.b = Tutu.b" },
				{ new From().innerJoin(tableToto, "to", tableTata, "ta", "to.a = ta.a").innerJoin(tableToto, tableTutu, "to.b = Tutu.b"),
						"Toto as to inner join Tata as ta on to.a = ta.a inner join Tutu on to.b = Tutu.b" },
				{ new From().innerJoin(tableToto, tableTata, "to.a = Tata.a").innerJoin(tableToto, "to", tableTutu, "tu", "to.b = tu.b"),
						"Toto as to inner join Tata on to.a = Tata.a inner join Tutu as tu on to.b = tu.b" },
				
				// test écriture From avec des Columns
				// 1 jointure, avec ou sans alias
				{ new From().innerJoin(colTotoA, colTataA), "Toto inner join Tata on Toto.a = Tata.a" },
				{ new From().innerJoin(colTotoA, "to", colTataA, "ta"), "Toto as to inner join Tata as ta on to.a = ta.a" },
				{ new From().leftOuterJoin(colTotoA, colTataA), "Toto left outer join Tata on Toto.a = Tata.a" },
				{ new From().leftOuterJoin(colTotoA, "to", colTataA, "ta"), "Toto as to left outer join Tata as ta on to.a = ta.a" },
				{ new From().rightOuterJoin(colTotoA, colTataA), "Toto right outer join Tata on Toto.a = Tata.a" },
				{ new From().rightOuterJoin(colTotoA, "to", colTataA, "ta"), "Toto as to right outer join Tata as ta on to.a = ta.a" },
				
				// 2 jointures, avec ou sans alias
				{ new From().innerJoin(colTotoA, colTataA).innerJoin(colTotoB, colTutuB),
						"Toto inner join Tata on Toto.a = Tata.a inner join Tutu on Toto.b = Tutu.b" },
				{ new From().innerJoin(colTotoA, "to", colTataA, "ta").innerJoin(colTotoB, colTutuB),
						"Toto as to inner join Tata as ta on to.a = ta.a inner join Tutu on to.b = Tutu.b" },
				{ new From().innerJoin(colTotoA, colTataA).innerJoin(colTotoB, "to", colTutuB, "tu"),
						"Toto as to inner join Tata on to.a = Tata.a inner join Tutu as tu on to.b = tu.b" },
				
				{ new From().innerJoin(colTotoA, "to", colTataA, "ta").innerJoin(colTotoB, null, colTutuB, "tu").innerJoin(colTutuB, colTataA),
						"Toto as to inner join Tata as ta on to.a = ta.a inner join Tutu as tu on to.b = tu.b inner join Tata as ta on tu.b = ta.a" },
				
				// mix par Table et par Column
				{ new From().innerJoin(tableToto, tableTata, "Toto.a = Tata.a").innerJoin(colTotoB, colTutuB),
						"Toto inner join Tata on Toto.a = Tata.a inner join Tutu on Toto.b = Tutu.b" },

		};
		
	}
	
	@Test(dataProvider = TEST_TO_SQL_DATA)
	public void testToSQL(From from, String expected) {
		FromBuilder testInstance = new FromBuilder(from);
		assertEquals(expected, testInstance.toSQL());
	}
}