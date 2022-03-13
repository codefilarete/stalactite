package org.codefilarete.stalactite.sql.statement;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.collection.Maps.ChainingMap;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_BINDER;
import static org.mockito.Mockito.mock;

/**
 * @author Guillaume Mary
 */
public class SQLStatementTest {
	
	@Test
	public void testApplyValue_missingBinder_exceptionIsThrown() {
		SQLStatement<String> testInstance = new SQLStatementStub(Maps.asMap("a", INTEGER_BINDER));
		testInstance.setValues(Maps.asMap("a", 1).add("b", 2));
		assertThatExceptionOfType(IllegalArgumentException.class).as("Missing binder for [b] for values {a=1, b=2} in \"dummy sql\"").isThrownBy(() -> testInstance.applyValues(mock(PreparedStatement.class)));
	}
	
	@Test
	public void testApplyValue_missingValue_exceptionIsThrown() {
		SQLStatement<String> testInstance = new SQLStatementStub(Maps.asMap("a", INTEGER_BINDER));
		testInstance.setValues(Maps.asMap("b", 2));
		assertThatExceptionOfType(IllegalArgumentException.class).as("Missing value for parameters [a] in values {b=2} in \"dummy sql\"").isThrownBy(() -> testInstance.applyValues(mock(PreparedStatement.class)));
	}
	
	@Test
	public void testApplyValue_allBindersPresent_doApplyValueIsCalled() {
		Map<String, Object> appliedValues = new HashMap<>();
		SQLStatement<String> testInstance = new SQLStatementStub(Maps.asMap("a", (PreparedStatementWriter) INTEGER_BINDER).add("b", INTEGER_BINDER)) {
			@Override
			protected void doApplyValue(String key, Object value, PreparedStatement statement) {
				appliedValues.put(key, value);
			}
		};
		ChainingMap<String, Integer> expectedValues = Maps.asMap("a", 1).add("b", 2);
		testInstance.setValues(expectedValues);
		testInstance.applyValues(mock(PreparedStatement.class));
		assertThat(appliedValues).isEqualTo(expectedValues);
	}
	
	private static class SQLStatementStub extends SQLStatement<String> {
		
		public SQLStatementStub(Map<String, PreparedStatementWriter> paramBinders) {
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