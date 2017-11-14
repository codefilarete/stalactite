package org.gama.stalactite.query.builder;

import java.util.Collections;
import java.util.Map;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.collection.Maps;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.model.CriteriaChain;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.gama.stalactite.query.model.QueryEase.filter;
import static org.gama.stalactite.query.model.QueryEase.where;
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
				// test simple Where
				{ where(colA, "= 28").and(colB, "= 1"), Collections.EMPTY_MAP, "Toto.a = 28 and Toto.b = 1" },
				{ where(colA, "= 28").or(colB, "= 1"), Collections.EMPTY_MAP, "Toto.a = 28 or Toto.b = 1" },
				// test simple Where + alias
				{ where(colA, "= 28").and(colB, "= 1"), tableAliases, "t.a = 28 and t.b = 1" },
				{ where(colA, "= 28").or(colB, "= 1"), tableAliases, "t.a = 28 or t.b = 1" },
				// test Criteria
				{ filter(colA, "= 28").and(colB, "= 1"), tableAliases, "t.a = 28 and t.b = 1" },
				{ filter(colA, "= 28").or(colB, "= 1"), tableAliases, "t.a = 28 or t.b = 1" },
				// test Where composed of Criteria
				{ where(colA, "= 28").and(filter(colB, "= 1")), tableAliases, "t.a = 28 and (t.b = 1)" },
				{ where(colA, "= 28").or(filter(colB, "= 1")), tableAliases, "t.a = 28 or (t.b = 1)" },
				// test more complex Where composed of Criteria
				{ where(colA, "= 28").or(filter(colB, "= 1").and(colB, "< -1")), tableAliases, "t.a = 28 or (t.b = 1 and t.b < -1)" },
				{ where(colA, "= 28").or(filter(colB, "= 1").or(colB, "< -1")), tableAliases, "t.a = 28 or (t.b = 1 or t.b < -1)" },
				// test Where composed of Criteria composed of Criteria
				{ where(colA, "= 28").or(filter(colB, "= 1").and(colB, "< -1").and(filter(colA, "= 42"))), tableAliases, "t.a = 28 or (t.b = 1 and t.b < -1 and (t.a = 42))" },
				{ where(colA, "= 28").or(filter(colB, "= 1").and(colB, "< -1").or(filter(colA, "= 42"))), tableAliases, "t.a = 28 or (t.b = 1 and t.b < -1 or (t.a = 42))" },
				// test Where composed of String Criteria
				{ where(colA, "= 28").or(filter("t.b = 1").and("t.b < -1")), tableAliases, "t.a = 28 or (t.b = 1 and t.b < -1)" },
				{ where(colA, "= 28").or(filter( "t.b = 1").or("t.b < -1")), tableAliases, "t.a = 28 or (t.b = 1 or t.b < -1)" },
		};
		
	}
	
	@Test
	@UseDataProvider("testToSQL_data")
	public void testToSQL(CriteriaChain where, Map<Table, String> tableAliases, String expected) {
		WhereBuilder testInstance = new WhereBuilder(where, tableAliases);
		assertEquals(expected, testInstance.toSQL());
	}

}