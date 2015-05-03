package org.stalactite.dml;

import static org.junit.Assert.*;

import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import org.stalactite.dml.ExpandableSQL.ExpandableParameter;
import org.stalactite.dml.SQLParameterParser.Parameter;
import org.stalactite.dml.SQLParameterParser.ParsedSQL;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.EntryFactoryHashMap;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author mary
 */
public class ExpandableSQLTest {

	private static final String TEST_EXPANDABLE_PARAMETERS_DATA = "testExpandableParametersData";
	
	private Parameter paramB = new Parameter("B");
	private Parameter paramC = new Parameter("C");
	
	private List<Entry<Integer, Object>> asEntries(Object ... indexValuePairs) {
		List<Entry<Integer, Object>> entries = new ArrayList<>(indexValuePairs.length/2);
		for (int i = 0; i < indexValuePairs.length; i+=2) {
			Integer index = (Integer) indexValuePairs[i];
			Object value = indexValuePairs[i+1];
			entries.add(new SimpleEntry<>(index, value));
		}
		return entries;
	}

	@DataProvider(name = TEST_EXPANDABLE_PARAMETERS_DATA)
	public Object[][] testExpandableParameters_data() {
		return new Object[][] {
				{ Arrays.asList("select a from Toto where b = ", paramB, " and c = ", paramC),
						Maps.asMap("B", 18).add("C", 23),
						"select a from Toto where b = ? and c = ?",
						Maps.asMap("B", asEntries(1, 18)).add("C", asEntries(2, 23)) },
				{ Arrays.asList("select a from Toto where b = ", paramB, " and c = ", paramC),
						Maps.asMap("B", (Object) Arrays.asList(20, 30, 40)).add("C", 23),
						"select a from Toto where b = ?, ?, ? and c = ?",
						Maps.asMap("B", asEntries(1, 20, 2, 30, 3, 40)).add("C", asEntries(4, 23)) },
				{ Arrays.asList("select a from Toto where b = ", paramB, " and c = ", paramC, " and b = ", paramB),
						Maps.asMap("B", (Object) Arrays.asList(20, 30, 40)).add("C", 23),
						"select a from Toto where b = ?, ?, ? and c = ? and b = ?, ?, ?",
						Maps.asMap("B", asEntries(1, 20, 2, 30, 3, 40, 5, 20, 6, 30, 7, 40)).add("C", asEntries(4, 23)) },
		};
	}

	@Test(dataProvider = TEST_EXPANDABLE_PARAMETERS_DATA)
	public void testExpandableParameters(List<Object> sqlSnippets, Map<String, Object> values,
										 String expectedPreparedSql, Map<String, List<Entry<Integer, Object>>> expectedIndexedValues) {
		Map<String, Parameter> params = new HashMap<>();
		for (Object sqlSnippet : sqlSnippets) {
			if (sqlSnippet instanceof Parameter) {
				params.put(((Parameter) sqlSnippet).getName(), (Parameter) sqlSnippet);
			}
		}

		ParsedSQL parsedSQL = new ParsedSQL(sqlSnippets, params);
		ExpandableSQL testInstance = new ExpandableSQL(parsedSQL, values);
		assertEquals(expectedPreparedSql, testInstance.getPreparedSQL());

		Map<String, List<ExpandableParameter>> mappedParameter = new EntryFactoryHashMap<String, List<ExpandableParameter>>(2) {
			@Override
			public List<ExpandableParameter> createInstance(String input) {
				return new ArrayList<>();
			}
		};
		for (ExpandableParameter expandableParameter : testInstance.getExpandableParameters()) {
			mappedParameter.get(expandableParameter.getParameterName()).add(expandableParameter);
		}

		List<Parameter> expectedParams = Arrays.asList(paramB, paramC);
		for (Parameter expectedParam : expectedParams) {
			List<ExpandableParameter> expParams = mappedParameter.get(expectedParam.getName());
			assertNotNull(expParams);
			assertFalse(expParams.isEmpty());
			List<Entry<Integer, Object>> indexes = new ArrayList<>();
			for (ExpandableParameter expParam : expParams) {
				// NB: on passe par Copy pour un ajout plus facile (sinon ajout par Iterator)
				indexes.addAll(Iterables.copy(expParam.iterator()));
			}
			assertEquals(expectedIndexedValues.get(expectedParam.getName()), indexes);
		}
	}
}