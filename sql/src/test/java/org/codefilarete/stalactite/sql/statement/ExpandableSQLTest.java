package org.codefilarete.stalactite.sql.statement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.stalactite.sql.statement.ExpandableSQL.ExpandableParameter;
import org.codefilarete.stalactite.sql.statement.SQLParameterParser.Parameter;
import org.codefilarete.stalactite.sql.statement.SQLParameterParser.ParsedSQL;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
public class ExpandableSQLTest {

	private static Parameter paramB = new Parameter("B");
	private static Parameter paramC = new Parameter("C");
	
	public static Object[][] testExpandableParameters_data() {
		return new Object[][] {
				{ Arrays.asList("select a from Toto where b = ", paramB, " and c = ", paramC),
						Maps.asMap("B", 18).add("C", 23),
						"select a from Toto where b = ? and c = ?",
						Maps.asMap("B", Arrays.asList(1)).add("C", Arrays.asList(2)) },
				{ Arrays.asList("select a from Toto where b = ", paramB, " and c = ", paramC),
						Maps.asMap("B", (Object) Arrays.asList(20, 30, 40)).add("C", 23),
						"select a from Toto where b = ?, ?, ? and c = ?",
						Maps.asMap("B", Arrays.asList(1, 2, 3)).add("C", Arrays.asList(4)) },
				{ Arrays.asList("select a from Toto where b = ", paramB, " and c = ", paramC, " and b = ", paramB),
						Maps.asMap("B", (Object) Arrays.asList(20, 30, 40)).add("C", 17),
						"select a from Toto where b = ?, ?, ? and c = ? and b = ?, ?, ?",
						Maps.asMap("B", Arrays.asList(1, 2, 3, 5, 6, 7)).add("C", Arrays.asList(4)) },
				{ Arrays.asList("select a from Toto where b = ", paramB, " and c = ", paramC, " and b = ", paramB, " and c = ", paramC),
						Maps.asMap("B", (Object) Arrays.asList(20, 30, 40)).add("C", Arrays.asList(17, 23)),
						"select a from Toto where b = ?, ?, ? and c = ?, ? and b = ?, ?, ? and c = ?, ?",
						Maps.asMap("B", Arrays.asList(1, 2, 3, 6, 7, 8)).add("C", Arrays.asList(4, 5, 9, 10)) },
				// same parameter twice, next to each other
				{ Arrays.asList("select a from Toto where b = ", paramB, " and b = ", paramB, " and c = ", paramC),
						Maps.asMap("B", (Object) Arrays.asList(20, 30, 40)).add("C", Arrays.asList(17, 23)),
						"select a from Toto where b = ?, ?, ? and b = ?, ?, ? and c = ?, ?",
						Maps.asMap("B", Arrays.asList(1, 2, 3, 4, 5, 6)).add("C", Arrays.asList(7, 8)) },
				// same parameter twice, next to each other, other order
				{ Arrays.asList("select a from Toto where c = ", paramC, " and b = ", paramB, " and b = ", paramB),
						Maps.asMap("B", (Object) Arrays.asList(20, 30, 40)).add("C", Arrays.asList(17, 23)),
						"select a from Toto where c = ?, ? and b = ?, ?, ? and b = ?, ?, ?",
						Maps.asMap("B", Arrays.asList(3, 4, 5, 6, 7, 8)).add("C", Arrays.asList(1, 2)) },
		};
	}
	
	@ParameterizedTest
	@MethodSource("testExpandableParameters_data")
	public void testExpandableParameters(List<Object> sqlSnippets, Map<String, Object> values,
										 String expectedPreparedSql, Map<String, Iterable<Integer>> expectedIndexedValues) {
		Map<String, Parameter> params = new HashMap<>();
		for (Object sqlSnippet : sqlSnippets) {
			if (sqlSnippet instanceof Parameter) {
				params.put(((Parameter) sqlSnippet).getName(), (Parameter) sqlSnippet);
			}
		}

		ParsedSQL parsedSQL = new ParsedSQL(sqlSnippets, params);
		ExpandableSQL testInstance = new ExpandableSQL(parsedSQL, ExpandableSQL.sizes(values));
		assertThat(testInstance.getPreparedSQL()).isEqualTo(expectedPreparedSql);

		List<Parameter> expectedParams = Arrays.asList(paramB, paramC);
		for (Parameter expectedParam : expectedParams) {
			ExpandableParameter expandableParameter = testInstance.getExpandableParameters().get(expectedParam.getName());
			int i = 0;
			for (Integer expectedIndex : expectedIndexedValues.get(expectedParam.getName())) {
				assertThat(expandableParameter.getMarkIndexes()[i++]).isEqualTo((int) expectedIndex);
			}
		}
	}
}