package org.gama.stalactite.dml;

import java.util.HashMap;
import java.util.regex.Matcher;

import org.gama.stalactite.dml.SQLParameterParser.CollectionParameter;
import org.gama.stalactite.dml.SQLParameterParser.Parameter;
import org.gama.stalactite.dml.SQLParameterParser.ParsedSQL;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author mary
 */
public class SQLParameterParserTest {
	
	public static final String TEST_PARSE_DATA = "testParseData";
	private static final String TEST_PARSE_DATA_IN = "testParseDataIn";
	private static final String TEST_PARSE_DATA_IN_MATCHER = "testParseDataInMatcher";

	@DataProvider(name = TEST_PARSE_DATA)
	private Object[][] testParse_data() {
		Parameter paramB = new Parameter("B");
		Parameter paramC = new Parameter("C");
		return new Object[][] {
				// ne doit rien casser si aucun paramètre
				{"select a from Toto where b = 1", new ParsedSQL(Arrays.asList((Object) "select a from Toto where b = 1"),
						new HashMap<String, Parameter>())},
				// un seul paramètre, à la fin
				{"select a from Toto where b = :B", new ParsedSQL(Arrays.asList("select a from Toto where b = ", paramB),
						Maps.asMap("B", paramB))},
				// un seul paramètre, pas à la fin
				{"select a from Toto where b = :B and c = 1", new ParsedSQL(Arrays.asList("select a from Toto where b = ", paramB, " and c = 1"),
						Maps.asMap("B", paramB))},
				// 2 paramètres
				{"select a from Toto where b = :B and c = :C", new ParsedSQL(Arrays.asList("select a from Toto where b = ", paramB, " and c = ", paramC),
						Maps.asMap("B", paramB).add("C", paramC))},
		};
	}

	@Test(dataProvider = TEST_PARSE_DATA)
	public void testParse(String sql, ParsedSQL expectedResult) {
		SQLParameterParser testInstance = new SQLParameterParser(sql);
		ParsedSQL parsedSQL = testInstance.parse();
		Assert.assertEquals(parsedSQL.getSqlSnippets(), expectedResult.getSqlSnippets());
		Assert.assertEquals(parsedSQL.getParametersMap(), expectedResult.getParametersMap());
	}
	
	@DataProvider(name = TEST_PARSE_DATA_IN_MATCHER)
	private Object[][] testParse_inMatcher_data() {
		return new Object[][] {
				{ " in (", true},
				{ "    in    (   ", true},
				{ "where in(", true},
				{ "wherein(", false},
				{ "in", false},
				{ " i ( ", false},
				{ "i(", false},
		};
	}
	
	@Test(dataProvider = TEST_PARSE_DATA_IN_MATCHER)
	public void testParse_inMatcher(String sql, boolean expectedResult) {
		Matcher matcher = SQLParameterParser.IN_PATTERN.matcher(sql);
		Assert.assertEquals(matcher.find(), expectedResult);
	}
	
	@DataProvider(name = TEST_PARSE_DATA_IN)
	private Object[][] testParse_in_data() {
		CollectionParameter paramB = new CollectionParameter("B");
		CollectionParameter paramC = new CollectionParameter("C");
		return new Object[][] {
				// un seul paramètre, à la fin
				{"select a from Toto where b in (:B)", new ParsedSQL(Arrays.asList("select a from Toto where b in (", paramB, ")"),
						Maps.asMap("B", (Parameter) paramB))},
				// 2 paramètres
				{"select a from Toto where b in (:B) and c in (:C)", new ParsedSQL(Arrays.asList("select a from Toto where b in (", paramB, ") and c in (", paramC, ")"),
						Maps.asMap("B", (Parameter) paramB).add("C", paramC))},
		};
	}

	@Test(dataProvider = TEST_PARSE_DATA_IN)
	public void testParse_in(String sql, ParsedSQL expectedResult) {
		SQLParameterParser testInstance = new SQLParameterParser(sql);
		ParsedSQL parsedSQL = testInstance.parse();
		org.junit.Assert.assertEquals(expectedResult.getSqlSnippets(), parsedSQL.getSqlSnippets());
		org.junit.Assert.assertEquals(expectedResult.getParametersMap(), parsedSQL.getParametersMap());
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Parameter name can't be empty at position 30")
	public void testParse_error_causeEmptyParamName() throws Exception {
		// un paramètre sans nom => erreur
		String sql = "select a from Toto where b = :";
		SQLParameterParser testInstance = new SQLParameterParser(sql);
		testInstance.parse();
	}

}