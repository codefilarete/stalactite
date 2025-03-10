package org.codefilarete.stalactite.sql.result;

import java.sql.SQLException;

import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.trace.MutableInt;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders.INTEGER_READER;

/**
 * @author Guillaume Mary
 */
public class ColumnConsumerTest {
	
	/**
	 * A test based on an {@link MutableInt} that will take its value from a {@link java.sql.ResultSet}
	 */
	@Test
	public void testAssemble() throws SQLException {
		MutableInt targetInstance = new MutableInt();
		ColumnConsumer<MutableInt, Integer> testInstance = new ColumnConsumer<>("a", INTEGER_READER, MutableInt::increment);
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap("a", (Object) 42).add("b", -42),
				Maps.asMap("a", 666)
		));
		
		resultSet.next();
		testInstance.assemble(targetInstance, resultSet);
		assertThat(targetInstance.getValue()).isEqualTo(42);
		resultSet.next();
		testInstance.assemble(targetInstance, resultSet);
		// 42 + 666 = 708
		assertThat(targetInstance.getValue()).isEqualTo(708);
	}
	
	@Test
	public void testCopyWithAlias() throws SQLException {
		MutableInt targetInstance = new MutableInt();
		ColumnConsumer<MutableInt, Integer> srcInstance = new ColumnConsumer<>("a", INTEGER_READER, MutableInt::increment);
		ColumnConsumer<MutableInt, Integer> testInstance = srcInstance.copyWithAliases(n -> n + "_new");
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap("a_new", (Object) 42).add("b_new", -42),
				Maps.asMap("a_new", 666)
		));
		
		resultSet.next();
		assertThatThrownBy(() -> srcInstance.assemble(targetInstance, resultSet))
				.isInstanceOf(BindingException.class)
				.hasMessage("Error while reading column 'a'")
				.hasCause(new SQLException("Column doesn't exist : a"));
		
		testInstance.assemble(targetInstance, resultSet);
		assertThat(targetInstance.getValue()).isEqualTo(42);
		resultSet.next();
		testInstance.assemble(targetInstance, resultSet);
		// 42 + 666 = 708
		assertThat(targetInstance.getValue()).isEqualTo(708);
	}
}