package org.stalactite.query.builder;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Map;

import org.gama.lang.collection.Maps;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.stalactite.query.model.Criteria;
import org.stalactite.query.model.Where;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author guillaume.mary
 */
public class WhereBuilderTest {

	private static final String TEST_TO_SQL_DATA = "testToSQLData";
	
	@DataProvider(name = TEST_TO_SQL_DATA)
	public Object[][] testToSQL_data() {
		Table tableToto = new Table(null, "Toto");
		Column colA = tableToto.new Column("a", String.class);
		Column colB = tableToto.new Column("b", String.class);
		Map<Table, String> tableAliases = Maps.asMap(tableToto, "t");

		return new Object[][] {
				// test écriture Where
				{ new Where(colA, "= 28").and(colB, "= 1"), Collections.EMPTY_MAP, "Toto.a = 28 and Toto.b = 1" },
				{ new Where(colA, "= 28").or(colB, "= 1"), Collections.EMPTY_MAP, "Toto.a = 28 or Toto.b = 1" },
				// test écriture Where + alias
				{ new Where(colA, "= 28").and(colB, "= 1"), tableAliases, "t.a = 28 and t.b = 1" },
				{ new Where(colA, "= 28").or(colB, "= 1"), tableAliases, "t.a = 28 or t.b = 1" },
				// test écriture Criteria
				{ new Criteria(colA, "= 28").and(colB, "= 1"), tableAliases, "t.a = 28 and t.b = 1" },
				{ new Criteria(colA, "= 28").or(colB, "= 1"), tableAliases, "t.a = 28 or t.b = 1" },
				// test écriture Where composé de Criteria
				{ new Where(colA, "= 28").and(new Criteria(colB, "= 1")), tableAliases, "t.a = 28 and (t.b = 1)" },
				{ new Where(colA, "= 28").or(new Criteria(colB, "= 1")), tableAliases, "t.a = 28 or (t.b = 1)" },
				// test écriture Where composé de Criteria un peu plus compliqué
				{ new Where(colA, "= 28").or(new Criteria(colB, "= 1").and(colB, "< -1")), tableAliases, "t.a = 28 or (t.b = 1 and t.b < -1)" },
				{ new Where(colA, "= 28").or(new Criteria(colB, "= 1").or(colB, "< -1")), tableAliases, "t.a = 28 or (t.b = 1 or t.b < -1)" },
				// test écriture Where composé de Criteria également composé
				{ new Where(colA, "= 28").or(new Criteria(colB, "= 1").and(colB, "< -1").and(new Criteria(colA, "= 42"))), tableAliases, "t.a = 28 or (t.b = 1 and t.b < -1 and (t.a = 42))" },
				{ new Where(colA, "= 28").or(new Criteria(colB, "= 1").and(colB, "< -1").or(new Criteria(colA, "= 42"))), tableAliases, "t.a = 28 or (t.b = 1 and t.b < -1 or (t.a = 42))" },
		};
		
	}
	
	@Test(dataProvider = TEST_TO_SQL_DATA)
	public void testToSQL(Criteria where, Map<Table, String> tableAliases, String expected) {
		WhereBuilder testInstance = new WhereBuilder(where, tableAliases);
		assertEquals(expected, testInstance.toSQL());
	}

}