package org.gama.sql.dml;

import java.util.Collections;
import java.util.HashMap;
import java.util.regex.Matcher;

import org.gama.lang.collection.Maps;
import org.gama.sql.dml.SQLParameterParser.CollectionParameter;
import org.gama.sql.dml.SQLParameterParser.Parameter;
import org.gama.sql.dml.SQLParameterParser.ParsedSQL;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.gama.lang.collection.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Guillaume Mary
 */
public class SQLParameterParserTest {
	
	public static Object[][] testParse_data() {
		Parameter paramB = new Parameter("B");
		Parameter paramC = new Parameter("C");
		return new Object[][] {
				// should not break anything since no parameter
				{ "select a from Toto where b = 1", new ParsedSQL(asList((Object) "select a from Toto where b = 1"),
						new HashMap<>()) },
				// only one parameter, at the end
				{ "select a from Toto where b = :B", new ParsedSQL(asList("select a from Toto where b = ", paramB),
						Maps.asMap("B", paramB)) },
				// only one parameter, not at the end
				{ "select a from Toto where b = :B and c = 1", new ParsedSQL(asList("select a from Toto where b = ", paramB, " and c = 1"),
						Maps.asMap("B", paramB)) },
				// 2 parameters
				{ "select a from Toto where b = :B and c = :C", new ParsedSQL(asList("select a from Toto where b = ", paramB, " and c = ", 
						paramC),
						Maps.asMap("B", paramB).add("C", paramC)) },
				// parameters and quotes
				{ "select a from Toto where b = 'hello :D !' and c = ':C' and d = :B", new ParsedSQL(asList("select a from Toto where b = 'hello :D !' and c = ':C' and d = ", paramB),
						Maps.asMap("B", paramB)) },
				{ "select a from Toto where b = \"hello :D !\" and c = \":C\" and d = ':B'", new ParsedSQL(asList("select a from Toto where b = \"hello :D !\" and c = \":C\" and d = ':B'"),
						Collections.emptyMap()) },
		};
	}
	
	@ParameterizedTest
	@MethodSource("testParse_data")
	public void testParse(String sql, ParsedSQL expectedResult) {
		SQLParameterParser testInstance = new SQLParameterParser(sql);
		ParsedSQL parsedSQL = testInstance.parse();
		assertEquals(expectedResult.getSqlSnippets(), parsedSQL.getSqlSnippets());
		assertEquals(expectedResult.getParametersMap(), parsedSQL.getParametersMap());
	}
	
	public static Object[][] testParse_inMatcher_data() {
		return new Object[][] {
				{ " in (", true },
				{ "    in    (   ", true },
				{ "where in(", true },
				{ "wherein(", false },
				{ "in", false },
				{ " i ( ", false },
				{ "i(", false },
		};
	}
	
	@ParameterizedTest
	@MethodSource("testParse_inMatcher_data")
	public void testParse_inMatcher(String sql, boolean expectedResult) {
		Matcher matcher = SQLParameterParser.IN_PATTERN.matcher(sql);
		assertEquals(expectedResult, matcher.find());
	}
	
	public static Object[][] testParse_in_data() {
		CollectionParameter paramB = new CollectionParameter("B");
		CollectionParameter paramC = new CollectionParameter("C");
		Parameter paramD = new Parameter("D");
		return new Object[][] {
				// only one parameter, at the end
				{ "select a from Toto where b in (:B)",
						new ParsedSQL(asList("select a from Toto where b in (", paramB, ")"),
								Maps.asMap("B", (Parameter) paramB)) },
				// 2 parameters "in"
				{ "select a from Toto where b in (:B) and c in (:C)",
						new ParsedSQL(asList("select a from Toto where b in (", paramB, ") and c in (", paramC, ")"),
								Maps.asMap("B", (Parameter) paramB).add("C", paramC)) },
				// 2 parameters "in" and "=" mix
				{ "select a from Toto where b in (:B) and d = :D",
						new ParsedSQL(asList("select a from Toto where b in (", paramB, ") and d = ", paramD),
								Maps.asMap("B", (Parameter) paramB).add("D", paramD)) },
		};
	}
	
	@ParameterizedTest
	@MethodSource("testParse_in_data")
	public void testParse_in(String input, ParsedSQL expectedResult) {
		SQLParameterParser testInstance = new SQLParameterParser(input);
		ParsedSQL parsedSQL = testInstance.parse();
		assertEquals(expectedResult.getSqlSnippets(), parsedSQL.getSqlSnippets());
		assertEquals(expectedResult.getParametersMap(), parsedSQL.getParametersMap());
	}
	
	@Test
	public void testParse_error_causeEmptyParamName() {
		// a parameter without name => error
		String sql = "select a from Toto where b = :";
		SQLParameterParser testInstance = new SQLParameterParser(sql);
		assertThrows(IllegalArgumentException.class, testInstance::parse, "Parameter name can't be empty at position 30");
	}
	
}