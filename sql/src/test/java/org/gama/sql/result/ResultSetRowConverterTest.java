package org.gama.sql.result;

import java.sql.SQLException;
import java.util.function.BiConsumer;

import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.lang.trace.IncrementableInt;
import org.junit.Test;

import static org.gama.sql.binder.DefaultResultSetReaders.INTEGER_READER;
import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class ResultSetRowConverterTest {
	
	@Test
	public void testConvert_basicUseCase() throws SQLException {
		// The default IncrementableInt that takes its value from "a". Reinstanciated on each row.
		ResultSetRowConverter<Integer, IncrementableInt> testInstance = new ResultSetRowConverter<>("a", INTEGER_READER, IncrementableInt::new);
		// The secondary that will increment the same IncrementableInt by column "b" value
		testInstance.add(new ColumnConsumer<>("b", INTEGER_READER, (BiConsumer<IncrementableInt, Integer>) (t, i) -> t.increment(Objects.preventNull(i, 0))));
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap("a", (Object) 42).add("b", 1),
				Maps.asMap("a", 666)
		));
		
		resultSet.next();
		assertEquals(43, testInstance.convert(resultSet).getValue());
		resultSet.next();
		// no change on this one because there's no "b" column on the row and we took null into account during incrementation
		assertEquals(666, testInstance.convert(resultSet).getValue());
	}
	
	/**
	 * A test based on an {@link IncrementableInt} that would take its value from a {@link java.sql.ResultSet}
	 */
	@Test
	public void testConvert_shareInstanceOverRows() throws SQLException {
		// The default IncrementableInt that takes its value from "a". Shared over rows (class attribute)
		IncrementableInt sharedInstance = new IncrementableInt(0);
		ResultSetRowConverter<Integer, IncrementableInt> testInstance = new ResultSetRowConverter<>("a", INTEGER_READER, i -> {
			sharedInstance.increment(i);
			return sharedInstance;
		});
		// The secondary that will increment the same IncrementableInt by column "b" value
		testInstance.add(new ColumnConsumer<>("b", INTEGER_READER, (t, i) -> sharedInstance.increment(Objects.preventNull(i, 0))));
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap("a", (Object) 42).add("b", 1),
				Maps.asMap("a", 666)
		));
		
		resultSet.next();
		assertEquals(43, testInstance.convert(resultSet).getValue());
		resultSet.next();
		// no change on this one because there's no "b" column on the row and we took null into account during incrementation
		assertEquals(709, testInstance.convert(resultSet).getValue());
	}
	
}