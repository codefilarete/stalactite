package org.gama.stalactite.query.builder;

import java.util.Collections;
import java.util.Map;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.collection.Maps;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.model.Select;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class SelectBuilderTest {

	@DataProvider
	public static Object[][] testToSQL_data() {
		Table tableToto = new Table(null, "Toto");
		Column colTotoA = tableToto.new Column("a", String.class);
		Column colTotoB = tableToto.new Column("b", String.class);
		Table tableTata = new Table(null, "Tata");
		Column colTataA = tableTata.new Column("a", String.class);
		Column colTataB = tableTata.new Column("b", String.class);
		Map<Table, String> tableAliases = Maps.asMap(tableToto, "to").add(tableTata, "ta");

		Map<Table, String> emptyMap = Collections.emptyMap();
		return new Object[][]{
				{ new Select().add("a"), tableAliases, "a" },
				{ new Select().distinct().add("a"), tableAliases, "distinct a" },
				{ new Select().add("a").distinct(), tableAliases, "distinct a" },
				{ new Select().add("a", "b"), tableAliases, "a, b" },
				{ new Select().add(colTotoA), emptyMap, "Toto.a" },
				{ new Select().add(colTotoA), tableAliases, "to.a" },
				{ new Select().add(colTotoA, colTotoB), emptyMap, "Toto.a, Toto.b" },
				{ new Select().add(colTotoA, colTotoB), tableAliases, "to.a, to.b" },
				{ new Select().add(colTotoA, colTataB), emptyMap, "Toto.a, Tata.b" },
				{ new Select().add(colTotoA, colTataB), tableAliases, "to.a, ta.b" },
				{ new Select().add(colTotoA, colTataB).distinct(), tableAliases, "distinct to.a, ta.b" },
				{ new Select().add(colTotoA, "A"), emptyMap, "Toto.a as A" },
				{ new Select().add(colTotoA, "A"), tableAliases, "to.a as A" },
		};
	}

	@Test
	@UseDataProvider("testToSQL_data")
	public void testToSQL(Select from, Map<Table, String> tableAliases, String expected) {
		SelectBuilder testInstance = new SelectBuilder(from, tableAliases);
		assertEquals(expected, testInstance.toSQL());
	}
}