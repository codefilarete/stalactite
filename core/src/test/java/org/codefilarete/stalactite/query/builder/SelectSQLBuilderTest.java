package org.codefilarete.stalactite.query.builder;

import java.util.Collections;
import java.util.Map;

import org.codefilarete.stalactite.query.builder.FunctionSQLBuilderFactory.FunctionSQLBuilder;
import org.codefilarete.stalactite.query.builder.SelectSQLBuilderFactory.SelectSQLBuilder;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.api.SelectChain;
import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.query.Operators.max;
import static org.codefilarete.stalactite.query.Operators.min;

/**
 * @author Guillaume Mary
 */
public class SelectSQLBuilderTest {

	public static Object[][] toSQL_data() {
		Table tableToto = new Table(null, "Toto");
		Column<Table, Integer> colTotoA = tableToto.addColumn("a", Integer.class);
		Column<Table, Integer> colTotoB = tableToto.addColumn("b", Integer.class);
		Table tableTata = new Table(null, "Tata");
		Column<Table, String> colTataA = tableTata.addColumn("a", String.class);
		Column<Table, String> colTataB = tableTata.addColumn("b", String.class);
		Map<Table, String> tableAliases = Maps.asMap(tableToto, "to").add(tableTata, "ta");

		Map<Table, String> emptyMap = Collections.emptyMap();
		return new Object[][]{
				{ new Select().add("a", String.class), tableAliases, "a" },
				{ new Select().add("a", String.class).as("aa"), tableAliases, "a as aa" },
				{ new Select().distinct().add("a", String.class), tableAliases, "distinct a" },
				{ new Select().add("a", String.class).distinct(), tableAliases, "distinct a" },
				{ new Select().add("a", String.class).add("b", String.class), tableAliases, "a, b" },
				{ new Select().add(colTotoA), emptyMap, "Toto.a" },
				{ new Select().add(colTotoA), tableAliases, "to.a" },
				{ new Select().add(colTotoA, colTotoB), emptyMap, "Toto.a, Toto.b" },
				{ new Select().add(colTotoA, colTotoB), tableAliases, "to.a, to.b" },
				{ new Select().add(colTotoA, colTataB), emptyMap, "Toto.a, Tata.b" },
				{ new Select().add(colTotoA, colTataB), tableAliases, "to.a, ta.b" },
				{ new Select().add(colTotoA, colTataB).distinct(), tableAliases, "distinct to.a, ta.b" },
				{ new Select().add(colTotoA, "A"), emptyMap, "Toto.a as A" },
				{ new Select().add(colTotoA, "A"), tableAliases, "to.a as A" },
				{ new Select().add(min(colTotoA), "tutu"), tableAliases, "min(to.a) as tutu" },
				{ new Select().add(min(colTotoA), max(colTotoA)), tableAliases, "min(to.a), max(to.a)" },
				{ new Select().add(Arrays.asList(colTotoA, colTotoB)).add(colTataA), emptyMap, "Toto.a, Toto.b, Tata.a" },
		};
	}
	
	@ParameterizedTest
	@MethodSource("toSQL_data")
	public void toSQL(SelectChain<Select> select, Map<Table, String> tableAliases, String expected) {
		SelectSQLBuilder testInstance = new SelectSQLBuilder(select.getSelect(), new DMLNameProvider(tableAliases::get), new FunctionSQLBuilder(new DMLNameProvider(tableAliases), new DefaultTypeMapping()));
		assertThat(testInstance.toSQL()).isEqualTo(expected);
	}
}
