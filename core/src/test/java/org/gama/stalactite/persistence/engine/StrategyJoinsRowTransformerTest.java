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
import org.gama.lang.collection.Iterables;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.engine.JoinedStrategiesSelect.StrategyJoins;
import org.gama.stalactite.persistence.engine.JoinedStrategiesSelect.StrategyJoins.Join;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.ToBeanRowTransformer;
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
public class StrategyJoinsRowTransformerTest {
	
	private ClassMappingStrategy dummyStrategy;
	
	@Before
	public void setUp() {
		dummyStrategy = mock(ClassMappingStrategy.class);
		when(dummyStrategy.getClassToPersist()).thenReturn(Toto.class);
	}
	
	@Test
	public void testTransform_with1strategy() throws SQLException, NoSuchMethodException {
		// defining the Table is mandatory and overall its primary key since the transformer requires it to read and find the entity in the cache
		Table totoTable = new Table("toto");
		totoTable.new Column("id", long.class).primaryKey();
		when(dummyStrategy.getTargetTable()).thenReturn(totoTable);
		
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class));
		
		StrategyJoinsRowTransformer testInstance = new StrategyJoinsRowTransformer(new StrategyJoins<>(dummyStrategy));
		Row row1 = new Row().add("toto_id", 1L).add("name", "toto");
		List result = testInstance.transform(Arrays.asList(row1));
		
		Object firstObject = Iterables.first(result);
		assertNotNull(firstObject);
		assertEquals(Toto.class, firstObject.getClass());
		Toto typedResult = (Toto) firstObject;
		assertEquals("toto", typedResult.name);
	}
	
	/**
	 * Test case with a root strategy joined with another one : @OneToOne case
	 */
	@Test
	public void testTransform_with2strategies() throws SQLException, NoSuchMethodException {
		// defining the Table is mandatory and overall its primary key since the transformer requires it to read and find the entity in the cache
		Table totoTable = new Table("toto");
		totoTable.new Column("id", long.class).primaryKey();
		when(dummyStrategy.getTargetTable()).thenReturn(totoTable);
		
		StrategyJoins rootStrategyJoins = new StrategyJoins<>(dummyStrategy);
		
		ClassMappingStrategy joinedStrategy = mock(ClassMappingStrategy.class);
		when(joinedStrategy.getClassToPersist()).thenReturn(Tata.class);
		
		// defining the target table is not necessary for the test case but it is technically, otherwise we get a NullPointerException
		Table tataTable = new Table("tata");
		tataTable.new Column("id", long.class).primaryKey();
		Column dummyJoinColumn = tataTable.new Column("a", long.class);
		when(joinedStrategy.getTargetTable()).thenReturn(tataTable);
		
		// completing the test case: adding the joined strategy
		rootStrategyJoins.add(joinedStrategy, dummyJoinColumn, dummyJoinColumn, false,
				(BiConsumer<Toto, Tata>) Toto::setOneToOne,
				(Function<Toto, Tata>) Toto::getOneToOne, null);
		
		// Telling mocks which instance to create
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class));
		when(joinedStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Tata.class));
		
		StrategyJoinsRowTransformer testInstance = new StrategyJoinsRowTransformer(rootStrategyJoins);
		Row row1 = new Row()
				// for Toto instance
				.add("toto_id", 1L).add("name", "toto")
				// for Tata instance
				.add("tata_id", 1L).add("name", "toto")
				.add("firstName", "tata");
		List result = testInstance.transform(Arrays.asList(row1));
		
		Object firstObject = Iterables.first(result);
		assertNotNull(firstObject);
		assertEquals(Toto.class, firstObject.getClass());
		Toto typedResult = (Toto) firstObject;
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
		// defining the Table is mandatory and overall its primary key since the transformer requires it to read and find the entity in the cache
		Table totoTable = new Table("toto");
		totoTable.new Column("id", long.class).primaryKey();
		when(dummyStrategy.getTargetTable()).thenReturn(totoTable);
		
		StrategyJoins rootStrategyJoins = new StrategyJoins<>(dummyStrategy);
		
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
		Join joinedStrategy1Name = rootStrategyJoins.add(joinedStrategy1, dummyJoinColumn1, dummyJoinColumn1, false,
				(BiConsumer<Toto, Tata>) Toto::setOneToOne, (Function<Toto, Tata>) Toto::getOneToOne, null);
		// completing the test case: adding the depth-2 strategy
		joinedStrategy1Name.getStrategy().add(joinedStrategy2, dummyJoinColumn2, dummyJoinColumn2, false,
				(BiConsumer<Tata, Titi>) Tata::setOneToOne, null, null);
		
		// Telling mocks which instance to create
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class));
		when(joinedStrategy1.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Tata.class));
		when(joinedStrategy2.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Titi.class));
		
		StrategyJoinsRowTransformer testInstance = new StrategyJoinsRowTransformer(rootStrategyJoins);
		Row row = new Row()
				// for Toto instance
				.add("toto_id", 1L).add("name", "toto")
				// for Tata instance
				.add("tata_id", 1L)
				.add("firstName", "tata")
				// for Titi instance
				.add("titi_id", 1L)
				.add("lastName", "titi");
		List result = testInstance.transform(Arrays.asList(row));
		
		Object firstObject = Iterables.first(result);
		assertNotNull(firstObject);
		assertEquals(Toto.class, firstObject.getClass());
		Toto typedResult = (Toto) firstObject;
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
		// defining the Table is mandatory and overall its primary key since the transformer requires it to read and find the entity in the cache
		Table totoTable = new Table("toto");
		totoTable.new Column("id", long.class).primaryKey();
		when(dummyStrategy.getTargetTable()).thenReturn(totoTable);
		
		StrategyJoins rootStrategyJoins = new StrategyJoins<>(dummyStrategy);
		
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
		rootStrategyJoins.add(joinedStrategy1, dummyJoinColumn1, dummyJoinColumn1, false,
				(BiConsumer<Toto, Tata>) Toto::setOneToOne,
				null, null);
		// completing the test case: adding the 2nd joined strategy
		rootStrategyJoins.add(joinedStrategy2, dummyJoinColumn2, dummyJoinColumn2, false,
				(BiConsumer<Toto, Titi>) Toto::setOneToOneOther,
				null, null);
		
		// Telling mocks which instance to create
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class));
		when(joinedStrategy1.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Tata.class));
		when(joinedStrategy2.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Titi.class));
		
		StrategyJoinsRowTransformer testInstance = new StrategyJoinsRowTransformer(rootStrategyJoins);
		Row row = new Row()
				// for Toto instance
				.add("toto_id", 1L).add("name", "toto")
				// for Tata instance
				.add("tata_id", 1L)
				.add("firstName", "tata")
				// for Titi instance
				.add("titi_id", 1L)
				.add("lastName", "titi");
		List result = testInstance.transform(Arrays.asList(row));
		
		Object firstObject = Iterables.first(result);
		assertNotNull(firstObject);
		assertEquals(Toto.class, firstObject.getClass());
		Toto typedResult = (Toto) firstObject;
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
		// defining the Table is mandatory and overall its primary key since the transformer requires it to read and find the entity in the cache
		Table totoTable = new Table("toto");
		totoTable.new Column("id", long.class).primaryKey();
		when(dummyStrategy.getTargetTable()).thenReturn(totoTable);
		
		StrategyJoins rootStrategyJoins = new StrategyJoins<>(dummyStrategy);
		
		ClassMappingStrategy joinedStrategy = mock(ClassMappingStrategy.class);
		when(joinedStrategy.getClassToPersist()).thenReturn(Tata.class);
		
		// defining the target table is not necessary for the test case but it is technically, otherwise we get a NullPointerException
		Table tataTable = new Table("tata");
		tataTable.new Column("id", long.class).primaryKey();
		Column dummyJoinColumn = tataTable.new Column("a", long.class);
		when(joinedStrategy.getTargetTable()).thenReturn(tataTable);
		
		// completing the test case: adding the joined strategy
		rootStrategyJoins.add(joinedStrategy,
				null, dummyJoinColumn, false,
				(BiConsumer<Toto, Collection<Tata>>) Toto::setOneToMany, (Function<Toto, Collection<Tata>>) Toto::getOneToMany, ArrayList.class);
		
		// Telling mocks which instance to create
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class));
		when(joinedStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Tata.class));
		
		StrategyJoinsRowTransformer testInstance = new StrategyJoinsRowTransformer(rootStrategyJoins);
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
		List<Toto> result = testInstance.transform(Arrays.asList(row1, row2));
		
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