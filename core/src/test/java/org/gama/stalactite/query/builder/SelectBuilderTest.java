package org.gama.stalactite.query.builder;

import java.util.Collections;
import java.util.Map;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.Select;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.gama.stalactite.query.model.Operators.max;
import static org.gama.stalactite.query.model.Operators.min;

/**
 * @author Guillaume Mary
 */
public class SelectBuilderTest {

	public static Object[][] testToSQL_data() {
		Table tableToto = new Table(null, "Toto");
		Column colTotoA = tableToto.addColumn("a", String.class);
		Column colTotoB = tableToto.addColumn("b", String.class);
		Table tableTata = new Table(null, "Tata");
		Column colTataA = tableTata.addColumn("a", String.class);
		Column colTataB = tableTata.addColumn("b", String.class);
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
				{ new Select().add(min(colTotoA), max(colTotoA)), tableAliases, "min(to.a), max(to.a)" },
				{ new Select().add(Arrays.asList(colTotoA, colTotoB)).add(colTataA), emptyMap, "Toto.a, Toto.b, Tata.a" },
		};
	}
	
	@ParameterizedTest
	@MethodSource("testToSQL_data")
	public void testToSQL(Select select, Map<Table, String> tableAliases, String expected) {
		SelectBuilder testInstance = new SelectBuilder(select, tableAliases);
		assertThat(testInstance.toSQL()).isEqualTo(expected);
	}
}