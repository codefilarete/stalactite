package org.gama.stalactite.persistence.engine.cascade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.StrategyJoins;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.StrategyJoins.Join;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.ToBeanRowTransformer;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
public class StrategyJoinsRowTransformerTest {
	
	private ClassMappingStrategy dummyStrategy;
	
	@BeforeEach
	public void setUp() {
		dummyStrategy = mock(ClassMappingStrategy.class);
		when(dummyStrategy.getClassToPersist()).thenReturn(Toto.class);
	}
	
	@Test
	public void testTransform_with1strategy() {
		// defining the Table is mandatory and overall its primary key since the transformer requires it to read and find the entity in the cache
		Table totoTable = new Table("toto");
		Column totoColumnId = totoTable.addColumn("id", long.class).primaryKey();
		Column totoColumnName = totoTable.addColumn("name", String.class);
		when(dummyStrategy.getTargetTable()).thenReturn(totoTable);
		
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class, totoTable, false));
		
		StrategyJoinsRowTransformer testInstance = new StrategyJoinsRowTransformer(new StrategyJoins<>(dummyStrategy));
		Row row1 = buildRow(
				Maps.asMap(totoColumnId, (Object) 1L)
						.add(totoColumnName, "toto"),
				testInstance.getAliasProvider());
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
	public void testTransform_with2strategies_oneToOne() {
		// defining the Table is mandatory and overall its primary key since the transformer requires it to read and find the entity in the cache
		Table totoTable = new Table("toto");
		Column totoColumnId = totoTable.addColumn("id", long.class).primaryKey();
		Column totoColumnName = totoTable.addColumn("name", String.class);
		when(dummyStrategy.getTargetTable()).thenReturn(totoTable);
		
		StrategyJoins rootStrategyJoins = new StrategyJoins<>(dummyStrategy);
		
		ClassMappingStrategy joinedStrategy = mock(ClassMappingStrategy.class);
		when(joinedStrategy.getClassToPersist()).thenReturn(Tata.class);
		
		// defining the target table is not necessary for the test case but it is technically, otherwise we get a NullPointerException
		Table tataTable = new Table("tata");
		Column tataColumnId = tataTable.addColumn("id", long.class).primaryKey();
		Column tataColumnFirstName = tataTable.addColumn("firstName", String.class);
		Column dummyJoinColumn = tataTable.addColumn("a", long.class);
		when(joinedStrategy.getTargetTable()).thenReturn(tataTable);
		
		// completing the test case: adding the joined strategy
		rootStrategyJoins.add(joinedStrategy, dummyJoinColumn, dummyJoinColumn, false, BeanRelationFixer.of(Toto::setOneToOne));
		
		// Telling mocks which instance to create
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class, totoTable, false));
		when(joinedStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Tata.class, tataTable, false));
		
		StrategyJoinsRowTransformer testInstance = new StrategyJoinsRowTransformer(rootStrategyJoins);
		Row row1 = buildRow(
				Maps.asMap(totoColumnId, (Object) 1L)
						.add(totoColumnName, "toto")
						.add(tataColumnId, 1L)
						.add(tataColumnFirstName, "tata"),
				testInstance.getAliasProvider());
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
	public void testTransform_with3strategies_deep() {
		// defining the Table is mandatory and overall its primary key since the transformer requires it to read and find the entity in the cache
		Table totoTable = new Table("toto");
		Column totoColumnId = totoTable.addColumn("id", long.class).primaryKey();
		Column totoColumnName = totoTable.addColumn("name", String.class);
		when(dummyStrategy.getTargetTable()).thenReturn(totoTable);
		
		StrategyJoins rootStrategyJoins = new StrategyJoins<>(dummyStrategy);
		
		ClassMappingStrategy joinedStrategy1 = mock(ClassMappingStrategy.class);
		when(joinedStrategy1.getClassToPersist()).thenReturn(Tata.class);
		
		ClassMappingStrategy joinedStrategy2 = mock(ClassMappingStrategy.class);
		when(joinedStrategy2.getClassToPersist()).thenReturn(Titi.class);
		
		// defining the target table is not necessary for the test case but it is technically, otherwise we get a NullPointerException
		Table tataTable = new Table("tata");
		Column tataColumnId = tataTable.addColumn("id", long.class).primaryKey();
		Column tataColumnFirstName = tataTable.addColumn("firstName", String.class);
		Column dummyJoinColumn1 = tataTable.addColumn("a", long.class);
		when(joinedStrategy1.getTargetTable()).thenReturn(tataTable);
		
		Table titiTable = new Table("titi");
		Column titiColumnId = titiTable.addColumn("id", long.class).primaryKey();
		Column titiColumnLastName = titiTable.addColumn("lastName", String.class);
		Column dummyJoinColumn2 = titiTable.addColumn("a", long.class);
		when(joinedStrategy2.getTargetTable()).thenReturn(titiTable);
		
		// completing the test case: adding the depth-1 strategy
		Join joinedStrategy1Name = rootStrategyJoins.add(joinedStrategy1, dummyJoinColumn1, dummyJoinColumn1, false,
				BeanRelationFixer.of(Toto::setOneToOne));
		// completing the test case: adding the depth-2 strategy
		joinedStrategy1Name.getStrategy().add(joinedStrategy2, dummyJoinColumn2, dummyJoinColumn2, false,
				BeanRelationFixer.of(Tata::setOneToOne));
		
		// Telling mocks which instance to create
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class, totoTable, false));
		when(joinedStrategy1.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Tata.class, tataTable, false));
		when(joinedStrategy2.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Titi.class, titiTable, false));
		
		StrategyJoinsRowTransformer testInstance = new StrategyJoinsRowTransformer(rootStrategyJoins);
		Row row = buildRow(
				Maps.asMap(totoColumnId, (Object) 1L)
						.add(totoColumnName, "toto")
						.add(tataColumnId, 1L)
						.add(tataColumnFirstName, "tata")
						.add(titiColumnId, 1L)
						.add(titiColumnLastName, "titi"),
				testInstance.getAliasProvider());
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
	public void testTransform_with3strategies_flat() {
		// defining the Table is mandatory and overall its primary key since the transformer requires it to read and find the entity in the cache
		Table totoTable = new Table("toto");
		Column totoColumnId = totoTable.addColumn("id", long.class).primaryKey();
		Column totoColumnName = totoTable.addColumn("name", String.class);
		when(dummyStrategy.getTargetTable()).thenReturn(totoTable);
		
		StrategyJoins rootStrategyJoins = new StrategyJoins<>(dummyStrategy);
		
		ClassMappingStrategy joinedStrategy1 = mock(ClassMappingStrategy.class);
		when(joinedStrategy1.getClassToPersist()).thenReturn(Tata.class);
		
		ClassMappingStrategy joinedStrategy2 = mock(ClassMappingStrategy.class);
		when(joinedStrategy2.getClassToPersist()).thenReturn(Titi.class);
		
		// defining the target table is not necessary for the test case but it is technically, otherwise we get a NullPointerException
		Table tataTable = new Table("tata");
		Column tataColumnId = tataTable.addColumn("id", long.class).primaryKey();
		Column tataColumnFirstName = tataTable.addColumn("firstName", String.class);
		Column dummyJoinColumn1 = tataTable.addColumn("a", long.class);
		when(joinedStrategy1.getTargetTable()).thenReturn(tataTable);
		
		Table titiTable = new Table("titi");
		Column titiColumnId = titiTable.addColumn("id", long.class).primaryKey();
		Column titiColumnLastName = titiTable.addColumn("lastName", String.class);
		Column dummyJoinColumn2 = titiTable.addColumn("a", long.class);
		when(joinedStrategy2.getTargetTable()).thenReturn(titiTable);
		
		// completing the test case: adding the joined strategy
		rootStrategyJoins.add(joinedStrategy1, dummyJoinColumn1, dummyJoinColumn1, false,
				BeanRelationFixer.of(Toto::setOneToOne));
		// completing the test case: adding the 2nd joined strategy
		rootStrategyJoins.add(joinedStrategy2, dummyJoinColumn2, dummyJoinColumn2, false,
				BeanRelationFixer.of(Toto::setOneToOneOther));
		
		// Telling mocks which instance to create
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class, totoTable, false));
		when(joinedStrategy1.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Tata.class, tataTable, false));
		when(joinedStrategy2.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Titi.class, titiTable, false));
		
		StrategyJoinsRowTransformer testInstance = new StrategyJoinsRowTransformer(rootStrategyJoins);
		Row row = buildRow(
				Maps.asMap(totoColumnId, (Object) 1L)
						.add(totoColumnName, "toto")
						.add(tataColumnId, 1L)
						.add(tataColumnFirstName, "tata")
						.add(titiColumnId, 1L)
						.add(titiColumnLastName, "titi"),
				testInstance.getAliasProvider());
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
	public void testTransform_with2strategies_oneToMany() {
		// defining the Table is mandatory and overall its primary key since the transformer requires it to read and find the entity in the cache
		Table totoTable = new Table("toto");
		Column totoColumnId = totoTable.addColumn("id", long.class).primaryKey();
		Column totoColumnName = totoTable.addColumn("name", String.class);
		when(dummyStrategy.getTargetTable()).thenReturn(totoTable);
		
		StrategyJoins rootStrategyJoins = new StrategyJoins<>(dummyStrategy);
		
		ClassMappingStrategy joinedStrategy = mock(ClassMappingStrategy.class);
		when(joinedStrategy.getClassToPersist()).thenReturn(Tata.class);
		
		// defining the target table is not necessary for the test case but it is technically, otherwise we get a NullPointerException
		Table tataTable = new Table("tata");
		Column tataColumnId = tataTable.addColumn("id", long.class).primaryKey();
		Column tataColumnFirstName = tataTable.addColumn("firstName", String.class).primaryKey();
		Column dummyJoinColumn = tataTable.addColumn("a", long.class);
		when(joinedStrategy.getTargetTable()).thenReturn(tataTable);
		
		// completing the test case: adding the joined strategy
		rootStrategyJoins.add(joinedStrategy,
				null, dummyJoinColumn, false,
				BeanRelationFixer.of(Toto::setOneToMany, Toto::getOneToMany, ArrayList::new));
		
		// Telling mocks which instance to create
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class, totoTable, false));
		when(joinedStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Tata.class, tataTable, false));
		
		StrategyJoinsRowTransformer testInstance = new StrategyJoinsRowTransformer(rootStrategyJoins);
		
		Row row1 = buildRow(
				Maps.asMap(totoColumnId, (Object) 1L)
						.add(totoColumnName, "toto")
						.add(tataColumnId, 1L)
						.add(tataColumnFirstName, "tata1"),
				testInstance.getAliasProvider());
		Row row2 = buildRow(
				Maps.asMap(totoColumnId, (Object) 1L)
						.add(totoColumnName, "we don't care")
						.add(tataColumnId, 2L)
						.add(tataColumnFirstName, "tata2"),
				testInstance.getAliasProvider());
		List<Toto> result = testInstance.transform(Arrays.asList(row1, row2));
		
		assertEquals(1, result.size());
		Toto firstResult = result.get(0);
		assertEquals(1L, (Object) firstResult.id);
		assertEquals("toto", firstResult.name);
		assertNotNull(firstResult.oneToMany);
		assertTrue(firstResult.oneToMany instanceof ArrayList);
		// firstName is filled because we put "firstName" into the Row
		List<String> oneToManyResult = firstResult.oneToMany.stream().map((tata -> tata.firstName)).collect(Collectors.toList());
		assertEquals(Arrays.asList("tata1", "tata2"), oneToManyResult);
	}
	
	private static Row buildRow(Map<Column, Object> data, Function<Column, String> aliasProvider) {
		Row result = new Row();
		data.forEach((key, value) -> result.add(aliasProvider.apply(key), value));
		return result;
	}
	
	@Test
	public void testTransform_withTwiceSameStrategies_oneToOne() {
		// defining the Table is mandatory and overall its primary key since the transformer requires it to read and find the entity in the cache
		Table totoTable1 = new Table("toto");
		Column totoColumnId = totoTable1.addColumn("id", long.class).primaryKey();
		Column totoColumnName = totoTable1.addColumn("name", String.class);
		when(dummyStrategy.getTargetTable()).thenReturn(totoTable1);
		
		StrategyJoins rootStrategyJoins = new StrategyJoins<>(dummyStrategy);
		
		// completing the test case: adding the joined strategy
		Table totoTable2 = new Table("toto");
		Column toto2ColumnId = totoTable2.addColumn("id", long.class).primaryKey();
		Column toto2ColumnName = totoTable2.addColumn("name", String.class);
		ClassMappingStrategy dummyStrategy2 = mock(ClassMappingStrategy.class);
		when(dummyStrategy2.getClassToPersist()).thenReturn(Toto.class);
		when(dummyStrategy2.getTargetTable()).thenReturn(totoTable2);
		rootStrategyJoins.add(dummyStrategy2,
				null, toto2ColumnId, false,
				BeanRelationFixer.of(Toto::setSibling));
		
		
		// Telling mocks which instance to create
		when(dummyStrategy.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class, totoTable1, false));
		when(dummyStrategy2.getRowTransformer()).thenReturn(new ToBeanRowTransformer<>(Toto.class, totoTable2, false));
		
		StrategyJoinsRowTransformer<Toto> testInstance = new StrategyJoinsRowTransformer<>(rootStrategyJoins);
		
		Function<Column, String> aliasGenerator = c -> (c.getTable() == totoTable1 ? "table1_" : "table2_") + c.getName();
		// we give the aliases to our test instance
		Comparator<Column> columnComparator = (c1, c2) -> aliasGenerator.apply(c1).compareToIgnoreCase(aliasGenerator.apply(c2));
		testInstance.setAliases(Maps.asComparingMap(columnComparator, totoColumnId, aliasGenerator.apply(totoColumnId))
				.add(totoColumnName, aliasGenerator.apply(totoColumnName))
				.add(toto2ColumnId, aliasGenerator.apply(toto2ColumnId))
				.add(toto2ColumnName, aliasGenerator.apply(toto2ColumnName)));
		// the row must math the aliases given to the instance
		Row row = buildRow(
				Maps.asComparingMap(columnComparator, totoColumnId, (Object) 1L)
						.add(totoColumnName, "toto1")
						.add(toto2ColumnId, 2L)
						.add(toto2ColumnName, "toto2"),
				aliasGenerator
		);
		
		// executing the test
		List<Toto> result = testInstance.transform(Arrays.asList(row));
		
		// checking
		assertEquals(1, result.size());
		Toto firstResult = result.get(0);
		assertEquals((Object) 1L, firstResult.id);
		assertEquals((Object) 2L, firstResult.getSibling().id);
		assertEquals("toto1", firstResult.name);
		assertEquals("toto2", firstResult.getSibling().name);
	}
	
	
	public static class Toto {
		private Long id;
		private String name;
		private Tata oneToOne;
		private Titi oneToOneOther;
		private Collection<Tata> oneToMany;
		private Toto sibling;
		
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
		
		public Toto getSibling() {
			return sibling;
		}
		
		public void setSibling(Toto sibling) {
			this.sibling = sibling;
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