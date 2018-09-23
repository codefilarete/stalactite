package org.gama.sql.result;

import java.sql.SQLException;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.lang.trace.ModifiableInt;
import org.junit.jupiter.api.Test;

import static org.gama.sql.binder.DefaultResultSetReaders.INTEGER_READER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Guillaume Mary
 */
public class ColumnConsumerTest {
	
	/**
	 * A test based on an {@link ModifiableInt} that will take its value from a {@link java.sql.ResultSet}
	 */
	@Test
	public void testAssemble() throws SQLException {
		ModifiableInt targetInstance = new ModifiableInt();
		ColumnConsumer<ModifiableInt, Integer> testInstance = new ColumnConsumer<>("a", INTEGER_READER, ModifiableInt::increment);
		
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
	
	@Test
	public void testCopyWithAlias() throws SQLException {
		ModifiableInt targetInstance = new ModifiableInt();
		ColumnConsumer<ModifiableInt, Integer> srcInstance = new ColumnConsumer<>("a", INTEGER_READER, ModifiableInt::increment);
		ColumnConsumer<ModifiableInt, Integer> testInstance = srcInstance.copyWithAliases(n -> n + "_new");
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap("a_new", (Object) 42).add("b_new", -42),
				Maps.asMap("a_new", 666)
		));
		
		resultSet.next();
		assertEquals("Column doesn't exist : a",
				assertThrows(SQLException.class, () -> srcInstance.assemble(targetInstance, resultSet)).getMessage());
		
		testInstance.assemble(targetInstance, resultSet);
		assertEquals(42, targetInstance.getValue());
		resultSet.next();
		testInstance.assemble(targetInstance, resultSet);
		// 42 + 666 = 708
		assertEquals(708, targetInstance.getValue());
	}
}