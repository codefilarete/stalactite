package org.gama.sql.result;

import java.sql.SQLException;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.lang.trace.IncrementableInt;
import org.junit.jupiter.api.Test;

import static org.gama.sql.binder.DefaultResultSetReaders.INTEGER_READER;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
public class ColumnConsumerTest {
	
	/**
	 * A test based on an {@link IncrementableInt} that would take its value from a {@link java.sql.ResultSet}
	 */
	@Test
	public void testApply() throws SQLException {
		IncrementableInt targetInstance = new IncrementableInt();
		ColumnConsumer<IncrementableInt, Integer> testInstance = new ColumnConsumer<>("a", INTEGER_READER, IncrementableInt::increment);
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap("a", (Object) 42).add("b", -42),
				Maps.asMap("a", 666)
		));
		
		resultSet.next();
		testInstance.assemble(targetInstance, resultSet);
		assertEquals(42, targetInstance.getValue());
		resultSet.next();
		testInstance.assemble(targetInstance, resultSet);
		// 42 + 666 = 708
		assertEquals(708, targetInstance.getValue());
	}
	
}