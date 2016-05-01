package org.gama.stalactite.query.builder;

import java.util.Collections;
import java.util.Map;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.collection.Maps;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.model.Criteria;
import org.gama.stalactite.query.model.Where;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class WhereBuilderTest {

	@DataProvider
	public static Object[][] testToSQL_data() {
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
	
	@Test
	@UseDataProvider("testToSQL_data")
	public void testToSQL(Criteria where, Map<Table, String> tableAliases, String expected) {
		WhereBuilder testInstance = new WhereBuilder(where, tableAliases);
		assertEquals(expected, testInstance.toSQL());
	}

}