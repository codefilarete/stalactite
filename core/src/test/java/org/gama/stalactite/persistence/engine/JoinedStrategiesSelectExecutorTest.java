package org.gama.stalactite.persistence.engine;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.lang.collection.Arrays;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.engine.JoinedStrategiesSelect.StrategyJoins;
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
		when(dummyStrategy.getClassToPersist()).thenReturn(Toto.class);
	}
	
	@Test
	public void testTransform_with1strategy() throws SQLException, NoSuchMethodException {
		// preventing from NullPointerException during JoinedStrategiesSelectExecutor instanciation
		Table totoTable = new Table("toto");
		totoTable.new Column("id", long.class).primaryKey();
		when(dummyStrategy.getTargetTable()).thenReturn(totoTable);
		
		JoinedStrategiesSelectExecutor testInstance = new JoinedStrategiesSelectExecutor(dummyStrategy, dummyDialect, null);
		
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class));
		
		Row row1 = new Row().add("toto_id", 1L).add("name", "toto");
		Object result = testInstance.transform(Arrays.asList(row1).iterator());
		
		assertNotNull(result);
		assertEquals(Toto.class, result.getClass());
		Toto typedResult = (Toto) result;
		assertEquals("toto", typedResult.name);
	}
	
	/**
	 * Test case with a root strategy joined with another one : @OneToOne case
	 */
	@Test
	public void testTransform_with2strategies() throws SQLException, NoSuchMethodException {
		// preventing from NullPointerException during JoinedStrategiesSelectExecutor instanciation
		Table totoTable = new Table("toto");
		totoTable.new Column("id", long.class).primaryKey();
		when(dummyStrategy.getTargetTable()).thenReturn(totoTable);
		
		JoinedStrategiesSelectExecutor testInstance = new JoinedStrategiesSelectExecutor(dummyStrategy, dummyDialect, null);
		
		ClassMappingStrategy joinedStrategy = mock(ClassMappingStrategy.class);
		when(joinedStrategy.getClassToPersist()).thenReturn(Tata.class);
		
		// defining the target table is not necessary for the test case but it is technically, otherwise we get a NullPointerException
		Table tataTable = new Table("tata");
		tataTable.new Column("id", long.class).primaryKey();
		Column dummyJoinColumn = tataTable.new Column("a", long.class);
		when(joinedStrategy.getTargetTable()).thenReturn(tataTable);
		
		// completing the test case: adding the joined strategy
		testInstance.addComplementaryTables(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, joinedStrategy,
				(BiConsumer<Toto, Tata>) Toto::setOneToOne,
				(Function<Toto, Tata>) Toto::getOneToOne, dummyJoinColumn, dummyJoinColumn, null);
		
		// Telling mocks which instance to create
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class));
		when(joinedStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Tata.class));
		
		Row row1 = new Row()
				// for Toto instance
				.add("toto_id", 1L).add("name", "toto")
				// for Tata instance
				.add("tata_id", 1L).add("name", "toto")
				.add("firstName", "tata");
		Object result = testInstance.transform(Arrays.asList(row1).iterator());
		
		assertNotNull(result);
		assertEquals(Toto.class, result.getClass());
		Toto typedResult = (Toto) result;
		assertEquals("toto", typedResult.name);
		assertNotNull(typedResult.oneToOne);
		// firstName is filled because we put "firstName" into the Row
		assertEquals("tata", typedResult.oneToOne.firstName);
	}
	
	/**
	 * Test case with a root strategy joined with 2 others by deep : nested @OneToOne case
	 */
	@Test
	public void testTransform_with3strategies_deep() throws SQLException, NoSuchMethodException {
		// preventing from NullPointerException during JoinedStrategiesSelectExecutor instanciation
		Table totoTable = new Table("toto");
		totoTable.new Column("id", long.class).primaryKey();
		when(dummyStrategy.getTargetTable()).thenReturn(totoTable);
		
		JoinedStrategiesSelectExecutor testInstance = new JoinedStrategiesSelectExecutor(dummyStrategy, dummyDialect, null);
		
		ClassMappingStrategy joinedStrategy1 = mock(ClassMappingStrategy.class);
		when(joinedStrategy1.getClassToPersist()).thenReturn(Tata.class);
		
		ClassMappingStrategy joinedStrategy2 = mock(ClassMappingStrategy.class);
		when(joinedStrategy2.getClassToPersist()).thenReturn(Titi.class);
		
		// defining the target table is not necessary for the test case but it is technically, otherwise we get a NullPointerException
		Table tataTable = new Table("tata");
		tataTable.new Column("id", long.class).primaryKey();
		Column dummyJoinColumn1 = tataTable.new Column("a", long.class);
		when(joinedStrategy1.getTargetTable()).thenReturn(tataTable);
		
		Table titiTable = new Table("titi");
		titiTable.new Column("id", long.class).primaryKey();
		Column dummyJoinColumn2 = titiTable.new Column("a", long.class);
		when(joinedStrategy2.getTargetTable()).thenReturn(titiTable);
		
		// completing the test case: adding the depth-1 strategy
		String joinedStrategy1Name = testInstance.addComplementaryTables(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, joinedStrategy1,
				(BiConsumer<Toto, Tata>) Toto::setOneToOne,
				(Function<Toto, Tata>) Toto::getOneToOne, dummyJoinColumn1, dummyJoinColumn1, null);
		// completing the test case: adding the depth-2 strategy
		testInstance.addComplementaryTables(joinedStrategy1Name, joinedStrategy2, (BiConsumer<Tata, Titi>) Tata::setOneToOne,
				null, dummyJoinColumn2, dummyJoinColumn2, null);
		
		// Telling mocks which instance to create
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class));
		when(joinedStrategy1.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Tata.class));
		when(joinedStrategy2.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Titi.class));
		
		Row row1 = new Row()
				// for Toto instance
				.add("toto_id", 1L).add("name", "toto")
				// for Tata instance
				.add("tata_id", 1L)
				.add("firstName", "tata")
				// for Titi instance
				.add("titi_id", 1L)
				.add("lastName", "titi");
		Object result = testInstance.transform(Arrays.asList(row1).iterator());
		
		assertNotNull(result);
		assertEquals(Toto.class, result.getClass());
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
	
	/**
	 * Test case with a root strategy joined with 2 others flat : side-by-side @OneToOne case
	 */
	@Test
	public void testTransform_with3strategies_flat() throws SQLException, NoSuchMethodException {
		// preventing from NullPointerException during JoinedStrategiesSelectExecutor instanciation
		Table totoTable = new Table("toto");
		totoTable.new Column("id", long.class).primaryKey();
		when(dummyStrategy.getTargetTable()).thenReturn(totoTable);
		
		JoinedStrategiesSelectExecutor testInstance = new JoinedStrategiesSelectExecutor(dummyStrategy, dummyDialect, null);
		
		ClassMappingStrategy joinedStrategy1 = mock(ClassMappingStrategy.class);
		when(joinedStrategy1.getClassToPersist()).thenReturn(Tata.class);
		
		ClassMappingStrategy joinedStrategy2 = mock(ClassMappingStrategy.class);
		when(joinedStrategy2.getClassToPersist()).thenReturn(Titi.class);
		
		// defining the target table is not necessary for the test case but it is technically, otherwise we get a NullPointerException
		Table tataTable = new Table("tata");
		tataTable.new Column("id", long.class).primaryKey();
		Column dummyJoinColumn1 = tataTable.new Column("a", long.class);
		when(joinedStrategy1.getTargetTable()).thenReturn(tataTable);
		
		Table titiTable = new Table("titi");
		titiTable.new Column("id", long.class).primaryKey();
		Column dummyJoinColumn2 = titiTable.new Column("a", long.class);
		when(joinedStrategy2.getTargetTable()).thenReturn(titiTable);
		
		// completing the test case: adding the joined strategy
		testInstance.addComplementaryTables(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, joinedStrategy1,
				(BiConsumer<Toto, Tata>) Toto::setOneToOne,
				null, dummyJoinColumn1, dummyJoinColumn1, null);
		// completing the test case: adding the 2nd joined strategy
		testInstance.addComplementaryTables(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, joinedStrategy2,
				(BiConsumer<Toto, Titi>) Toto::setOneToOneOther,
				null, dummyJoinColumn2, dummyJoinColumn2, null);
		
		// Telling mocks which instance to create
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class));
		when(joinedStrategy1.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Tata.class));
		when(joinedStrategy2.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Titi.class));
		
		Row row1 = new Row()
				// for Toto instance
				.add("toto_id", 1L).add("name", "toto")
				// for Tata instance
				.add("tata_id", 1L)
				.add("firstName", "tata")
				// for Titi instance
				.add("titi_id", 1L)
				.add("lastName", "titi");
		Object result = testInstance.transform(Arrays.asList(row1).iterator());
		
		assertNotNull(result);
		assertEquals(Toto.class, result.getClass());
		Toto typedResult = (Toto) result;
		assertEquals("toto", typedResult.name);
		assertNotNull(typedResult.oneToOne);
		// firstName is filled because we put "firstName" into the Row
		assertEquals("tata", typedResult.oneToOne.firstName);
		// joined instance must be filled
		assertNotNull(typedResult.oneToOneOther);
		// firstName is filled because we put "firstName" into the Row
		assertEquals("titi", typedResult.oneToOneOther.lastName);
	}
	
	/**
	 * Test case with a root strategy joined with another one : @OneToMany case
	 */
	@Test
	public void testTransform_with2strategies_oneToMany() throws SQLException, NoSuchMethodException {
		// preventing from NullPointerException during JoinedStrategiesSelectExecutor instanciation
		Table totoTable = new Table("toto");
		totoTable.new Column("id", long.class).primaryKey();
		when(dummyStrategy.getTargetTable()).thenReturn(totoTable);
		
		JoinedStrategiesSelect testInstance = new JoinedStrategiesSelect(dummyStrategy, c -> dummyDialect.getColumnBinderRegistry().getBinder(c));
		
		ClassMappingStrategy joinedStrategy = mock(ClassMappingStrategy.class);
		when(joinedStrategy.getClassToPersist()).thenReturn(Tata.class);
		
		// defining the target table is not necessary for the test case but it is technically, otherwise we get a NullPointerException
		Table tataTable = new Table("tata");
		tataTable.new Column("id", long.class).primaryKey();
		Column dummyJoinColumn = tataTable.new Column("a", long.class);
		when(joinedStrategy.getTargetTable()).thenReturn(tataTable);
		
		// completing the test case: adding the joined strategy
		testInstance.add(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, joinedStrategy,
				null, dummyJoinColumn, false,
				(BiConsumer<Toto, Collection<Tata>>) Toto::setOneToMany, (Function<Toto, Collection<Tata>>) Toto::getOneToMany, ArrayList.class);
		
		// Telling mocks which instance to create
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class));
		when(joinedStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Tata.class));
		
		Row row1 = new Row()
				// for Toto instance
				.add("toto_id", 1L).add("name", "toto")
				// for Tata instance
				.add("tata_id", 1L)
				.add("firstName", "tata1");
		Row row2 = new Row()
				// for Toto instance
				.add("toto_id", 1L).add("name", "toto")
				// for Tata instance
				.add("tata_id", 2L)
				.add("firstName", "tata2");
		List<Toto> result = JoinedStrategiesSelectExecutor.transform(Arrays.asList(row1, row2).iterator(),
				(StrategyJoins<Toto>) testInstance.getStrategyJoins(JoinedStrategiesSelect.FIRST_STRATEGY_NAME));
		
		assertEquals(1, result.size());
		Toto firstResult = result.get(0);
		assertEquals("toto", firstResult.name);
		assertNotNull(firstResult.oneToMany);
		assertTrue(firstResult.oneToMany instanceof ArrayList);
		// firstName is filled because we put "firstName" into the Row
		Set<String> oneToManyResult = firstResult.oneToMany.stream().map((tata -> tata.firstName)).collect(Collectors.toSet());
		assertEquals(Arrays.asSet("tata1", "tata2"), oneToManyResult);
	}
	
	
	public static class Toto {
		private Long id;
		private String name;
		private Tata oneToOne;
		private Titi oneToOneOther;
		private Collection<Tata> oneToMany;
		
		public Toto() {
		}
		
		public Toto(Long id, String name) {
			this.id = id;
			this.name = name;
		}
		
		public void setOneToOne(Tata oneToOne) {
			this.oneToOne = oneToOne;
		}
		
		public Tata getOneToOne() {
			return oneToOne;
		}
		
		public void setOneToOneOther(Titi oneToOneOther) {
			this.oneToOneOther = oneToOneOther;
		}
		
		public void setOneToMany(Collection<Tata> oneToMany) {
			this.oneToMany = oneToMany;
		}
		
		public Collection<Tata> getOneToMany() {
			return oneToMany;
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