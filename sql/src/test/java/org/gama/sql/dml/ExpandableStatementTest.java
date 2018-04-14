package org.gama.sql.dml;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

import org.gama.lang.InvocationHandlerSupport;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.PreparedStatementWriter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
public class ExpandableStatementTest {
	
	public static Object[][] testDoApplyValue_data() {
		ParameterBinder parameterBinderMock = InvocationHandlerSupport.mock(ParameterBinder.class);
		return new Object[][] {
				{ "select * from Toto where id = :a",
						Maps.asMap("a", 17), Maps.asMap("a", parameterBinderMock),
						Maps.asMap(1, 17)},
				{ "select * from Toto where id = :a",
						Maps.asMap("a", Arrays.asList(17)), Maps.asMap("a", parameterBinderMock),
						Maps.asMap(1, 17)},
				{ "select * from Toto where id = :a",
						Maps.asMap("a", Arrays.asList(17, 23)), Maps.asMap("a", parameterBinderMock),
						Maps.asMap(1, 17).add(2, 23)},
				// with parameter before
				{ "select * from Toto where time = :b and id = :a",
						Maps.asMap("a", 17).add("b", -17), Maps.asMap("a", parameterBinderMock).add("b", parameterBinderMock),
						Maps.asMap(2, 17).add(1, -17)},
				{ "select * from Toto where time = :b and id = :a",
						Maps.asMap("a", (Object) Arrays.asList(17)).add("b", -17), Maps.asMap("a", parameterBinderMock).add("b", parameterBinderMock),
						Maps.asMap(2, 17).add(1, -17)},
				{ "select * from Toto where time = :b and id = :a",
						Maps.asMap("a", (Object) Arrays.asList(17, 23)).add("b", -17), Maps.asMap("a", parameterBinderMock).add("b", parameterBinderMock),
						Maps.asMap(2, 17).add(3, 23).add(1, -17)},
				// with twins parameters 
				{ "select * from Toto where id = :a or id = :a",
						Maps.asMap("a", 17), Maps.asMap("a", parameterBinderMock),
						Maps.asMap(1, 17).add(2, 17)},
				{ "select * from Toto where id = :a or id = :a",
						Maps.asMap("a", Arrays.asList(17, 23, 31)), Maps.asMap("a", parameterBinderMock),
						Maps.asMap(1, 17).add(2, 23).add(3, 31).add(4, 17).add(5, 23).add(6, 31)},
				// with parameter inserted between twins
				{ "select * from Toto where id = :a and time = :b or id = :a",
						Maps.asMap("a", 17).add("b", -17), Maps.asMap("a", parameterBinderMock).add("b", parameterBinderMock),
						Maps.asMap(1, 17).add(2, -17).add(3, 17)},
				{ "select * from Toto where id = :a and time = :b or id = :a",
						Maps.asMap("a", (Object) Arrays.asList(17, 23, 31)).add("b", -17), Maps.asMap("a", parameterBinderMock).add("b", parameterBinderMock),
						Maps.asMap(1, 17).add(2, 23).add(3, 31).add(4, -17).add(5, 17).add(6, 23).add(7, 31)},
		};
	}
	
	@ParameterizedTest
	@MethodSource("testDoApplyValue_data")
	public void testDoApplyValue(String sql, Map<String, Object> paramValues, Map<String, PreparedStatementWriter> binders, Map<Integer, Integer> expectedIndexes) {
		Map<Integer, Object> appliedIndexedValues = new HashMap<>();
		ExpandableStatement<String> testInstance = new StringParamedSQL(sql, binders) {
			@Override
			protected <T> void doApplyValue(int index, T value, PreparedStatementWriter<T> paramBinder, PreparedStatement statement) {
				appliedIndexedValues.put(index, value);
				super.doApplyValue(index, value, paramBinder, statement);
			}
		};
		for (Map.Entry<String, Object> paramValue : paramValues.entrySet()) {
			testInstance.setValue(paramValue.getKey(), paramValue.getValue());
		}
		testInstance.getSQL();
		testInstance.applyValues(InvocationHandlerSupport.mock(PreparedStatement.class));
		assertEquals(new HashMap<>(expectedIndexes), appliedIndexedValues);
		
	}
	
}