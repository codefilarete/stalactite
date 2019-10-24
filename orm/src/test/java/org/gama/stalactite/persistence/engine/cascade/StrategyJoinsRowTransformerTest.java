package org.gama.stalactite.persistence.engine.cascade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.StrategyJoins;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.StrategyJoins.Join;
import org.gama.stalactite.persistence.id.assembly.IdentifierAssembler;
import org.gama.stalactite.persistence.id.assembly.SimpleIdentifierAssembler;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.mapping.ToBeanRowTransformer;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
public class StrategyJoinsRowTransformerTest {
	
	private ClassMappingStrategy<Toto, Long, Table> rootStrategy;
	private Table totoTable;
	private Column totoColumnId;
	private Column totoColumnName;
	
	@BeforeEach
	public void setUp() {
		rootStrategy = mock(ClassMappingStrategy.class);
		when(rootStrategy.getClassToPersist()).thenReturn(Toto.class);
		// defining the Table is mandatory and overall its primary key since the transformer requires it to read and find the entity in the cache
		totoTable = new Table("toto");
		totoColumnId = totoTable.addColumn("id", long.class).primaryKey();
		totoColumnName = totoTable.addColumn("name", String.class);
		when(rootStrategy.getTargetTable()).thenReturn(totoTable);
		// adding IdentifierAssembler to the root strategy
		IdMappingStrategy totoIdMappingStrategyMock = mock(IdMappingStrategy.class);
		when(rootStrategy.getIdMappingStrategy()).thenReturn(totoIdMappingStrategyMock);
		IdentifierAssembler totoIdentifierAssembler = new SimpleIdentifierAssembler(totoColumnId);
		when(totoIdMappingStrategyMock.getIdentifierAssembler()).thenReturn(totoIdentifierAssembler);
	}
	
	@Test
	public void testTransform_with1strategy() {
		when(rootStrategy.copyTransformerWithAliases(any())).thenAnswer(new ToBeanRowTransformerAnswer<>(Toto.class, totoTable));
		
		StrategyJoinsRowTransformer testInstance = new StrategyJoinsRowTransformer(new StrategyJoins<>(rootStrategy));
		Row row1 = buildRow(
				Maps.asMap(totoColumnId, (Object) 1L)
						.add(totoColumnName, "toto"),
				testInstance.getAliasProvider());
		List result = testInstance.transform(Arrays.asList(row1), 1, new HashMap<>());
		
		Object firstObject = Iterables.first(result);
		assertNotNull(firstObject);
		assertEquals(Toto.class, firstObject.getClass());
		Toto typedResult = (Toto) firstObject;
		assertEquals("toto", typedResult.name);
	}
	
	/**
	 * Test case with a root strategy joined with another one : one-to-one case
	 */
	@Test
	public void testTransform_with2strategies_oneToOne() {
		StrategyJoins<Toto, ?> rootStrategyJoins = new StrategyJoins<>(rootStrategy);
		
		// creating another strategy that will be joined to the root one (goal of this test)
		ClassMappingStrategy<Tata, Long, Table> joinedStrategy = mock(ClassMappingStrategy.class);
		when(joinedStrategy.getClassToPersist()).thenReturn(Tata.class);
		
		// defining the target table is not necessary for the test case but it is technically, otherwise we get a NullPointerException
		Table tataTable = new Table("tata");
		Column tataColumnId = tataTable.addColumn("id", long.class).primaryKey();
		Column tataColumnFirstName = tataTable.addColumn("firstName", String.class);
		Column dummyJoinColumn = tataTable.addColumn("a", long.class);
		when(joinedStrategy.getTargetTable()).thenReturn(tataTable);
		
		// adding IdentifierAssembler to the joined strategy
		fixIdentifierAssembler(joinedStrategy, tataColumnId);
		
		// completing the test case: adding the joined strategy
		rootStrategyJoins.add(joinedStrategy, dummyJoinColumn, dummyJoinColumn, false, BeanRelationFixer.of(Toto::setOneToOne));
		
		
		// Telling mocks which instance to create
		when(rootStrategy.copyTransformerWithAliases(any())).thenAnswer(new ToBeanRowTransformerAnswer<>(Toto.class, totoTable));
		when(joinedStrategy.copyTransformerWithAliases(any())).thenAnswer(new ToBeanRowTransformerAnswer<>(Tata.class, tataTable));
		
		StrategyJoinsRowTransformer<Toto> testInstance = new StrategyJoinsRowTransformer<>(rootStrategyJoins);
		Row row1 = buildRow(
				Maps.asMap(totoColumnId, (Object) 1L)
						.add(totoColumnName, "toto")
						.add(tataColumnId, 1L)
						.add(tataColumnFirstName, "tata"),
				testInstance.getAliasProvider());
		List result = testInstance.transform(Arrays.asList(row1), 1, new HashMap<>());
		
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
	 * Test case with a root strategy joined with 2 others by deep : nested one-to-one case
	 */
	@Test
	public void testTransform_with3strategies_deep() {
		StrategyJoins rootStrategyJoins = new StrategyJoins<>(rootStrategy);
		
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
		// adding IdentifierAssembler to the joined strategy
		fixIdentifierAssembler(joinedStrategy1, tataColumnId);
		
		Table titiTable = new Table("titi");
		Column titiColumnId = titiTable.addColumn("id", long.class).primaryKey();
		Column titiColumnLastName = titiTable.addColumn("lastName", String.class);
		Column dummyJoinColumn2 = titiTable.addColumn("a", long.class);
		when(joinedStrategy2.getTargetTable()).thenReturn(titiTable);
		// adding IdentifierAssembler to the joined strategy
		fixIdentifierAssembler(joinedStrategy2, titiColumnId);
		
		// completing the test case: adding the depth-1 strategy
		Join joinedStrategy1Name = rootStrategyJoins.add(joinedStrategy1, dummyJoinColumn1, dummyJoinColumn1, false,
				BeanRelationFixer.of(Toto::setOneToOne));
		// completing the test case: adding the depth-2 strategy
		joinedStrategy1Name.getStrategy().add(joinedStrategy2, dummyJoinColumn2, dummyJoinColumn2, false,
				BeanRelationFixer.of(Tata::setOneToOne));
		
		// Telling mocks which instance to create
		when(rootStrategy.copyTransformerWithAliases(any())).thenAnswer(new ToBeanRowTransformerAnswer<>(Toto.class, totoTable));
		when(joinedStrategy1.copyTransformerWithAliases(any())).thenAnswer(new ToBeanRowTransformerAnswer<>(Tata.class, tataTable));
		when(joinedStrategy2.copyTransformerWithAliases(any())).thenAnswer(new ToBeanRowTransformerAnswer<>(Titi.class, titiTable));
		
		StrategyJoinsRowTransformer testInstance = new StrategyJoinsRowTransformer(rootStrategyJoins);
		Row row = buildRow(
				Maps.asMap(totoColumnId, (Object) 1L)
						.add(totoColumnName, "toto")
						.add(tataColumnId, 1L)
						.add(tataColumnFirstName, "tata")
						.add(titiColumnId, 1L)
						.add(titiColumnLastName, "titi"),
				testInstance.getAliasProvider());
		List result = testInstance.transform(Arrays.asList(row), 1, new HashMap<>());
		
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
	 * Test case with a root strategy joined with 2 others flat : side-by-side one-to-one case
	 */
	@Test
	public void testTransform_with3strategies_flat() {
		StrategyJoins rootStrategyJoins = new StrategyJoins<>(rootStrategy);
		
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
		// adding IdentifierAssembler to the joined strategy
		fixIdentifierAssembler(joinedStrategy1, tataColumnId);
		
		Table titiTable = new Table("titi");
		Column titiColumnId = titiTable.addColumn("id", long.class).primaryKey();
		Column titiColumnLastName = titiTable.addColumn("lastName", String.class);
		Column dummyJoinColumn2 = titiTable.addColumn("a", long.class);
		when(joinedStrategy2.getTargetTable()).thenReturn(titiTable);
		// adding IdentifierAssembler to the joined strategy
		fixIdentifierAssembler(joinedStrategy2, titiColumnId);
		
		// completing the test case: adding the joined strategy
		rootStrategyJoins.add(joinedStrategy1, dummyJoinColumn1, dummyJoinColumn1, false,
				BeanRelationFixer.of(Toto::setOneToOne));
		// completing the test case: adding the 2nd joined strategy
		rootStrategyJoins.add(joinedStrategy2, dummyJoinColumn2, dummyJoinColumn2, false,
				BeanRelationFixer.of(Toto::setOneToOneOther));
		
		// Telling mocks which instance to create
		when(rootStrategy.copyTransformerWithAliases(any())).thenAnswer(new ToBeanRowTransformerAnswer<>(Toto.class, totoTable));
		when(joinedStrategy1.copyTransformerWithAliases(any())).thenAnswer(new ToBeanRowTransformerAnswer<>(Tata.class, tataTable));
		when(joinedStrategy2.copyTransformerWithAliases(any())).thenAnswer(new ToBeanRowTransformerAnswer<>(Titi.class, titiTable));
		
		StrategyJoinsRowTransformer testInstance = new StrategyJoinsRowTransformer(rootStrategyJoins);
		Row row = buildRow(
				Maps.asMap(totoColumnId, (Object) 1L)
						.add(totoColumnName, "toto")
						.add(tataColumnId, 1L)
						.add(tataColumnFirstName, "tata")
						.add(titiColumnId, 1L)
						.add(titiColumnLastName, "titi"),
				testInstance.getAliasProvider());
		List result = testInstance.transform(Arrays.asList(row), 1, new HashMap<>());
		
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
	 * Test case with a root strategy joined with another one : one-to-many case
	 */
	@Test
	public void testTransform_with2strategies_oneToMany() {
		StrategyJoins rootStrategyJoins = new StrategyJoins<>(rootStrategy);
		
		ClassMappingStrategy joinedStrategy = mock(ClassMappingStrategy.class);
		when(joinedStrategy.getClassToPersist()).thenReturn(Tata.class);
		
		// defining the target table is not necessary for the test case but it is technically, otherwise we get a NullPointerException
		Table tataTable = new Table("tata");
		Column tataColumnId = tataTable.addColumn("id", long.class).primaryKey();
		Column tataColumnFirstName = tataTable.addColumn("firstName", String.class).primaryKey();
		Column dummyJoinColumn = tataTable.addColumn("a", long.class);
		when(joinedStrategy.getTargetTable()).thenReturn(tataTable);
		
		// adding IdentifierAssembler to the joined strategy
		fixIdentifierAssembler(joinedStrategy, tataColumnId);
		
		// completing the test case: adding the joined strategy
		rootStrategyJoins.add(joinedStrategy,
				null, dummyJoinColumn, false,
				BeanRelationFixer.of(Toto::setOneToMany, Toto::getOneToMany, ArrayList::new));
		
		// Telling mocks which instance to create
		when(rootStrategy.copyTransformerWithAliases(any())).thenAnswer(new ToBeanRowTransformerAnswer<>(Toto.class, totoTable));
		when(joinedStrategy.copyTransformerWithAliases(any())).thenAnswer(new ToBeanRowTransformerAnswer<>(Tata.class, tataTable));
		
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
		List<Toto> result = testInstance.transform(Arrays.asList(row1, row2), 1, new HashMap<>());
		
		assertEquals(1, result.size());
		Toto firstResult = Iterables.first(result);
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
		
		StrategyJoins rootStrategyJoins = new StrategyJoins<>(rootStrategy);
		
		// completing the test case: adding the joined strategy
		Table totoTable2 = new Table("toto");
		Column toto2ColumnId = totoTable2.addColumn("id", long.class).primaryKey();
		Column toto2ColumnName = totoTable2.addColumn("name", String.class);
		ClassMappingStrategy joinedStrategy = mock(ClassMappingStrategy.class);
		when(joinedStrategy.getClassToPersist()).thenReturn(Toto.class);
		when(joinedStrategy.getTargetTable()).thenReturn(totoTable2);
		// adding IdentifierAssembler to the joined strategy
		fixIdentifierAssembler(joinedStrategy, toto2ColumnId);
		
		rootStrategyJoins.add(joinedStrategy, null, toto2ColumnId, false, BeanRelationFixer.of(Toto::setSibling));
		
		
		// Telling mocks which instance to create
		when(rootStrategy.copyTransformerWithAliases(any())).thenAnswer(new ToBeanRowTransformerAnswer<>(Toto.class, totoTable));
		when(joinedStrategy.copyTransformerWithAliases(any())).thenAnswer(new ToBeanRowTransformerAnswer<>(Toto.class, totoTable2));
		
		StrategyJoinsRowTransformer<Toto> testInstance = new StrategyJoinsRowTransformer<>(rootStrategyJoins);
		
		Function<Column, String> aliasGenerator = c -> (c.getTable() == totoTable ? "table1_" : "table2_") + c.getName();
		// we give the aliases to our test instance
		Comparator<Column> columnComparator = (c1, c2) -> aliasGenerator.apply(c1).compareToIgnoreCase(aliasGenerator.apply(c2));
		Map<Column, String> aliases = Maps.asComparingMap(columnComparator, totoColumnId, aliasGenerator.apply(totoColumnId))
				.add(totoColumnName, aliasGenerator.apply(totoColumnName))
				.add(toto2ColumnId, aliasGenerator.apply(toto2ColumnId))
				.add(toto2ColumnName, aliasGenerator.apply(toto2ColumnName));
		testInstance.setAliasProvider(aliases::get);
		// the row must math the aliases given to the instance
		Row row = buildRow(
				Maps.asComparingMap(columnComparator, totoColumnId, (Object) 1L)
						.add(totoColumnName, "toto1")
						.add(toto2ColumnId, 2L)
						.add(toto2ColumnName, "toto2"),
				aliasGenerator
		);
		
		// executing the test
		List<Toto> result = testInstance.transform(Arrays.asList(row), 1, new HashMap<>());
		
		// checking
		assertEquals(1, result.size());
		Toto firstResult = Iterables.first(result);
		assertEquals((Object) 1L, firstResult.id);
		assertEquals((Object) 2L, firstResult.getSibling().id);
		assertEquals("toto1", firstResult.name);
		assertEquals("toto2", firstResult.getSibling().name);
	}
	
	
	/**
	 * Adds a {@link SimpleIdentifierAssembler} to the given strategy, for the given column that serves as a single primary key column 
	 */
	private static void fixIdentifierAssembler(ClassMappingStrategy strategy, Column primaryKey) {
		IdMappingStrategy tataIdMappingStrategyMock = mock(IdMappingStrategy.class);
		when(strategy.getIdMappingStrategy()).thenReturn(tataIdMappingStrategyMock);
		IdentifierAssembler tataIdentifierAssembler = new SimpleIdentifierAssembler(primaryKey);
		when(tataIdMappingStrategyMock.getIdentifierAssembler()).thenReturn(tataIdentifierAssembler);
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
		private Long id;
		private String firstName;
		private Titi oneToOne;
		
		public void setOneToOne(Titi oneToOne) {
			this.oneToOne = oneToOne;
		}
	}
	
	public static class Titi {
		private Long id;
		private String lastName;
	}
	
	private static class ToBeanRowTransformerAnswer<C> implements Answer<ToBeanRowTransformer<C>> {
		
		private final Class<C> instanceClass;
		private final Table table;
		
		private ToBeanRowTransformerAnswer(Class<C> instanceClass,  Table table) {
			this.instanceClass = instanceClass;
			this.table = table;
		}
		
		@Override
		public ToBeanRowTransformer<C> answer(InvocationOnMock invocation) {
			return new ToBeanRowTransformer<>(instanceClass, table, false).copyWithAliases((ColumnedRow) invocation.getArguments()[0]);
		}
	}
}