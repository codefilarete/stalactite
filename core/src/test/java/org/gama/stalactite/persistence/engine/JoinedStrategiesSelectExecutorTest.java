package org.gama.stalactite.persistence.engine;

import java.sql.SQLException;

import org.gama.lang.collection.Arrays;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.ToBeanRowTransformer;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
public class JoinedStrategiesSelectExecutorTest {
	
	private Dialect dummyDialect;
	private ClassMappingStrategy dummyStrategy;
	
	@Before
	public void setUp() {
		dummyDialect = new Dialect(new JavaTypeToSqlTypeMapping());
		dummyStrategy = mock(ClassMappingStrategy.class);
	}
	
	@Test
	public void testTransform() throws SQLException, NoSuchMethodException {
		// preventing from NullPointerException during JoinedStrategiesSelectExecutor instanciation
		when(dummyStrategy.getTargetTable()).thenReturn(new Table("toto"));
		
		JoinedStrategiesSelectExecutor testInstance = new JoinedStrategiesSelectExecutor(dummyStrategy, dummyDialect, null);
		
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class));
		
//		when(dummyStrategy.transform(any(Row.class))).thenReturn(new Toto(1L, "toto"));
		Row row1 = new Row().add("id", 1L).add("name", "toto");
		Object result = testInstance.transform(Arrays.asList(row1).iterator());
		
		// seems
		assertTrue(result instanceof Toto);
		Toto typedResult = (Toto) result;
		assertEquals(new Toto(1L, "toto").name, typedResult.name);
	}
	
	private static class Toto {
		private Long id;
		private String name;
		
		public Toto() {
		}
		
		public Toto(Long id, String name) {
			this.id = id;
			this.name = name;
		}
	}
	
}