package org.gama.stalactite.query.builder;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.model.SelectQuery;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class SelectQueryBuilderTest {
	
	@DataProvider
	public static Object[][] testToSQL_data() {
		final Table tableToto = new Table(null, "Toto");
		final Column colTotoA = tableToto.new Column("a", String.class);
		final Column colTotoB = tableToto.new Column("b", String.class);
		final Table tableTata = new Table(null, "Tata");
		final Column colTataA = tableTata.new Column("a", String.class);
		final Column colTataB = tableTata.new Column("b", String.class);
		
		return new Object[][] {
				// NB: utilisation du {{ }} cat il faut renvoyer un SelectQuery, en fluent on peut pas
				{ new SelectQuery() {{ select(colTotoA, colTotoB).from(tableToto); }},
					"select Toto.a, Toto.b from Toto" },
				{ new SelectQuery() {{ select(colTotoA, colTotoB).from(tableToto, "t"); }},
					"select t.a, t.b from Toto as t" },
				{ new SelectQuery() {{ select(colTotoA, colTotoB, colTataA, colTataB).from(tableToto, "to").crossJoin(tableTata); }},
					"select to.a, to.b, Tata.a, Tata.b from Toto as to cross join Tata" },
				{ new SelectQuery() {{ select(colTotoA, colTotoB, colTataA, colTataB).from(tableToto, "to").crossJoin(tableTata, "ta"); }},
					"select to.a, to.b, ta.a, ta.b from Toto as to cross join Tata as ta" },
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
	
	@Test
	@UseDataProvider("testToSQL_data")
	public void testToSQL(SelectQuery selectQuery, String expected) {
		SelectQueryBuilder testInstance = new SelectQueryBuilder(selectQuery);
		assertEquals(expected, testInstance.toSQL());
	}
}