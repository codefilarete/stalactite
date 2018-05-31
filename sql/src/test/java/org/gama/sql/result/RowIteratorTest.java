package org.gama.sql.result;

import org.gama.lang.collection.Maps;
import org.gama.sql.binder.DefaultResultSetReaders;
import org.gama.sql.dml.SQLStatement.BindingException;
import org.junit.jupiter.api.Test;

import static org.gama.lang.collection.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Guillaume Mary
 */
public class RowIteratorTest {
	
	@Test
	public void testConvert() {
		RowIterator testInstance = new RowIterator(null,
				Maps.asMap("toto", DefaultResultSetReaders.INTEGER_READER));
		assertEquals("Can't read column toto because ResultSet contains unexpected type j.l.String", assertThrows(BindingException.class, () -> {
			InMemoryResultSet rs = new InMemoryResultSet(asList(Maps.asMap("toto", "string value")));
			rs.next();
			testInstance.convert(rs);
		}).getMessage());
	}
}