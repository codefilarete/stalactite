package org.gama.sql.dml;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

import org.gama.lang.collection.Maps;
import org.gama.lang.collection.Maps.ChainingMap;
import org.gama.sql.binder.ParameterBinder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.gama.sql.binder.DefaultParameterBinders.INTEGER_BINDER;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * @author Guillaume Mary
 */
public class SQLStatementTest {
	
	@Rule
	public final ExpectedException expectedException = ExpectedException.none();
	
	@Test
	public void testApplyValue_missingBinder_exceptionIsThrown() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Missing binder for [b] for values {a=1, b=2} in \"dummy sql\"");
		SQLStatement<String> testInstance = new SQLStatementStub(Maps.asMap("a", INTEGER_BINDER));
		testInstance.setValues(Maps.asMap("a", 1).add("b", 2));
		testInstance.applyValues(mock(PreparedStatement.class));
	}
	
	@Test
	public void testApplyValue_missingValue_exceptionIsThrown() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Missing value for parameters [a] in values {b=2} in \"dummy sql\"");
		SQLStatement<String> testInstance = new SQLStatementStub(Maps.asMap("a", INTEGER_BINDER));
		testInstance.setValues(Maps.asMap("b", 2));
		testInstance.applyValues(mock(PreparedStatement.class));
	}
	
	@Test
	public void testApplyValue_allBindersPresent_doApplyValueIsCalled() {
		Map<String, Object> appliedValues = new HashMap<>();
		SQLStatement<String> testInstance = new SQLStatementStub(Maps.asMap("a", (ParameterBinder) INTEGER_BINDER).add("b", INTEGER_BINDER)) {
			@Override
			protected void doApplyValue(String key, Object value, PreparedStatement statement) {
				appliedValues.put(key, value);
			}
		};
		ChainingMap<String, Integer> expectedValues = Maps.asMap("a", 1).add("b", 2);
		testInstance.setValues(expectedValues);
		testInstance.applyValues(mock(PreparedStatement.class));
		assertEquals(expectedValues, appliedValues);
	}
	
	private static class SQLStatementStub extends SQLStatement<String> {
		
		public SQLStatementStub(Map<String, ParameterBinder> paramBinders) {
			super(paramBinders);
		}
		
		@Override
		public String getSQL() {
			// we don't need sql in our test
			return "dummy sql";
		}
		
		@Override
		protected void doApplyValue(String key, Object value, PreparedStatement statement) {
			// nothing to do
		}
	}
}