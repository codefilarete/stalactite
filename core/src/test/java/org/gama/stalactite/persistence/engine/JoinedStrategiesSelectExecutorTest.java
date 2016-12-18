package org.gama.stalactite.persistence.engine;

import java.sql.SQLException;
import java.util.function.BiConsumer;

import org.gama.lang.collection.Arrays;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.ToBeanRowTransformer;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
	public void testTransform_with1strategy() throws SQLException, NoSuchMethodException {
		// preventing from NullPointerException during JoinedStrategiesSelectExecutor instanciation
		when(dummyStrategy.getTargetTable()).thenReturn(new Table("toto"));
		
		JoinedStrategiesSelectExecutor testInstance = new JoinedStrategiesSelectExecutor(dummyStrategy, dummyDialect, null);
		
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class));
		
		Row row1 = new Row().add("id", 1L).add("name", "toto");
		Object result = testInstance.transform(Arrays.asList(row1).iterator());
		
		assertTrue(result instanceof Toto);
		Toto typedResult = (Toto) result;
		assertEquals("toto", typedResult.name);
	}
	
	@Test
	public void testTransform_with2strategies() throws SQLException, NoSuchMethodException {
		// preventing from NullPointerException during JoinedStrategiesSelectExecutor instanciation
		when(dummyStrategy.getTargetTable()).thenReturn(new Table("toto"));
		
		JoinedStrategiesSelectExecutor testInstance = new JoinedStrategiesSelectExecutor(dummyStrategy, dummyDialect, null);
		
		ClassMappingStrategy joinedStrategy = mock(ClassMappingStrategy.class);
		when(joinedStrategy.getClassToPersist()).thenReturn(Tata.class);
		
		// defining the target table is not necessary for the test case but it is technically, otherwise we get a NullPointerException
		Table tataTable = new Table("tata");
		Column dummyJoinColumn = tataTable.new Column("a", long.class);
		when(joinedStrategy.getTargetTable()).thenReturn(tataTable);
		
		// completing the test case: adding the joined strategy
		testInstance.addComplementaryTables(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, joinedStrategy, (BiConsumer<Toto, Tata>) Toto::setOneToOne,
				null, dummyJoinColumn);
		
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class));
		when(joinedStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Tata.class));
		
		Row row1 = new Row()
				// for Toto instance
				.add("id", 1L).add("name", "toto")
				// for Tata instance
				.add("firstName", "tata");
		Object result = testInstance.transform(Arrays.asList(row1).iterator());
		
		assertTrue(result instanceof Toto);
		Toto typedResult = (Toto) result;
		assertEquals("toto", typedResult.name);
		assertNotNull(typedResult.oneToOne);
		// firstName is filled because we put "firstName" into the Row
		assertEquals("tata", typedResult.oneToOne.firstName);
	}
	
	@Test
	public void testTransform_with3strategies() throws SQLException, NoSuchMethodException {
		// preventing from NullPointerException during JoinedStrategiesSelectExecutor instanciation
		when(dummyStrategy.getTargetTable()).thenReturn(new Table("toto"));
		
		JoinedStrategiesSelectExecutor testInstance = new JoinedStrategiesSelectExecutor(dummyStrategy, dummyDialect, null);
		
		ClassMappingStrategy joinedStrategy1 = mock(ClassMappingStrategy.class);
		when(joinedStrategy1.getClassToPersist()).thenReturn(Tata.class);
		
		ClassMappingStrategy joinedStrategy2 = mock(ClassMappingStrategy.class);
		when(joinedStrategy2.getClassToPersist()).thenReturn(Titi.class);
		
		// defining the target table is not necessary for the test case but it is technically, otherwise we get a NullPointerException
		Table tataTable = new Table("tata");
		Column dummyJoinColumn1 = tataTable.new Column("a", long.class);
		when(joinedStrategy1.getTargetTable()).thenReturn(tataTable);
		
		Table titiTable = new Table("titi");
		Column dummyJoinColumn2 = titiTable.new Column("a", long.class);
		when(joinedStrategy2.getTargetTable()).thenReturn(titiTable);
		
		// completing the test case: adding the joined strategy
		String joinedStrategy1Name = testInstance.addComplementaryTables(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, joinedStrategy1,
				(BiConsumer<Toto, Tata>) Toto::setOneToOne,
				null, dummyJoinColumn1);
		testInstance.addComplementaryTables(joinedStrategy1Name, joinedStrategy2, (BiConsumer<Tata, Titi>) Tata::setOneToOne,
				null, dummyJoinColumn2);
		
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class));
		when(joinedStrategy1.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Tata.class));
		when(joinedStrategy2.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Titi.class));
		
		Row row1 = new Row()
				// for Toto instance
				.add("id", 1L).add("name", "toto")
				// for Tata instance
				.add("firstName", "tata")
				// for Titi instance
				.add("lastName", "titi");
		Object result = testInstance.transform(Arrays.asList(row1).iterator());
		
		assertTrue(result instanceof Toto);
		Toto typedResult = (Toto) result;
		assertEquals("toto", typedResult.name);
		assertNotNull(typedResult.oneToOne);
		// firstName is filled because we put "firstName" into the Row
		assertEquals("tata", typedResult.oneToOne.firstName);
		// joined instance must be filled
		assertNotNull(typedResult.oneToOne.oneToOne);
		// firstName is filled because we put "firstName" into the Row
		assertEquals("titi", typedResult.oneToOne.oneToOne.lastName);
	}
	
	
	public static class Toto {
		private Long id;
		private String name;
		private Tata oneToOne;
		
		public Toto() {
		}
		
		public Toto(Long id, String name) {
			this.id = id;
			this.name = name;
		}
		
		public void setOneToOne(Tata oneToOne) {
			this.oneToOne = oneToOne;
		}
	}
	
	public static class Tata {
		private String firstName;
		private Titi oneToOne;
		
		public void setOneToOne(Titi oneToOne) {
			this.oneToOne = oneToOne;
		}
	}
	
	public static class Titi {
		private String lastName;
	}
}