package org.stalactite.query.builder;

import static org.junit.Assert.assertEquals;

import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.stalactite.query.model.SelectQuery;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class SelectQueryBuilderTest {
	
	private static final String TEST_TO_SQL_DATA = "testToSQLData";
	
	@DataProvider(name = TEST_TO_SQL_DATA)
	public Object[][] testToSQL_data() {
		final Table tableToto = new Table(null, "Toto");
		final Column colTotoA = tableToto.new Column("a", String.class);
		final Column colTotoB = tableToto.new Column("b", String.class);
		final Table tableTata = new Table(null, "Tata");
		final Column colTataA = tableTata.new Column("a", String.class);
		final Column colTataB = tableTata.new Column("b", String.class);
		
		return new Object[][] {
				// NB: utilisation du {{ }} cat il faut renvoyer un SelectQuery, en fluent on peut pas
				{ new SelectQuery() {{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y"); }},
					"select Toto.a, Tata.b from Toto inner join Tata on x = y" },
				{ new SelectQuery() {{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y").where(colTotoB, "= 1"); }},
					"select Toto.a, Tata.b from Toto inner join Tata on x = y where Toto.b = 1" },
				{ new SelectQuery() {{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y").where(colTotoB, "= 1").groupBy(colTotoB); }},
					"select Toto.a, Tata.b from Toto inner join Tata on x = y where Toto.b = 1 group by Toto.b" },
				{ new SelectQuery() {{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y").where(colTotoB, "= 1").groupBy(colTotoB).having("sum(", colTotoB, ") > 1"); }},
					"select Toto.a, Tata.b from Toto inner join Tata on x = y where Toto.b = 1 group by Toto.b having sum(Toto.b) > 1" },
				{ new SelectQuery() {{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y").where(colTotoB, "= 1").and(colTataA, "= 4").groupBy(colTotoB).add(colTataB).having("sum(", colTotoB, ") > 1").and("count(id) = 0"); }},
					"select Toto.a, Tata.b from Toto inner join Tata on x = y where Toto.b = 1 and Tata.a = 4 group by Toto.b, Tata.b having sum(Toto.b) > 1 and count(id) = 0" },
		};
	}
	
	@Test(dataProvider = TEST_TO_SQL_DATA)
	public void testToSQL(SelectQuery selectQuery, String expected) {
		SelectQueryBuilder testInstance = new SelectQueryBuilder(selectQuery);
		assertEquals(expected, testInstance.toSQL());
	}
}