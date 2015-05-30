package org.gama.stalactite.persistence.sql.dml;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import org.gama.lang.collection.PairIterator;
import org.gama.stalactite.persistence.mapping.StatementValues;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.mapping.PersistentFieldHarverster;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CRUDOperationTest {
	
	private Connection connection;
	
	private Map<Field, Column> totoClassMapping;
	private Column colA;
	private Column colB;
	private Column colC;
	private PreparedStatement preparedStatement;
	private ArgumentCaptor<Integer> valueCaptor;
	private ArgumentCaptor<Integer> indexCaptor;
	private ArgumentCaptor<String> sqlCaptor;
	private InsertOperation testInstance;

	@BeforeMethod
	public void setUp() throws SQLException {
		// Necessary to CRUDOperations and RowIterator which need JavaTypeToSqlTypeMapping
		PersistenceContext.setCurrent(new PersistenceContext(null, new Dialect(new JavaTypeToSqlTypeMapping())));
		
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Table totoClassTable = new Table(null, "Toto");
		totoClassMapping = persistentFieldHarverster.mapFields(Toto.class, totoClassTable);
		Map<String, Column> columns = totoClassTable.mapColumnsOnName();
		colA = columns.get("a");
		colA.setPrimaryKey(true);
		colB = columns.get("b");
		colC = columns.get("c");
		
		preparedStatement = mock(PreparedStatement.class);
		when(preparedStatement.executeBatch()).thenReturn(new int[] {1});
		connection = mock(Connection.class);
		sqlCaptor = ArgumentCaptor.forClass(String.class);
		when(connection.prepareStatement(sqlCaptor.capture())).thenReturn(preparedStatement);
		valueCaptor = ArgumentCaptor.forClass(Integer.class);
		indexCaptor = ArgumentCaptor.forClass(Integer.class);
		
		testInstance = new InsertOperation("insert into Toto(A, B) values (?, ?)", Maps.asMap(colA, 1).add(colB, 2));
	}
	
	@Test
	public void testApply_oneInsert() throws Exception {
		StatementValues values = new StatementValues();
		values.putUpsertValue(colA, 123);
		values.putUpsertValue(colB, 456);
		testInstance.apply(values, connection);
		testInstance.execute();
		
		verify(connection, times(1)).prepareStatement(anyString());
		verify(preparedStatement, times(1)).addBatch();
		verify(preparedStatement, times(2)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("insert into Toto(A, B) values (?, ?)", sqlCaptor.getValue());
		Set<Map.Entry<Integer, Integer>> indexValuePairs = buildPairs(indexCaptor.getAllValues(), valueCaptor.getAllValues());
		assertEquals(pairChain(1, 123)._(2, 456)._(), indexValuePairs);
		
		// a second insert must'nt a second PreparedStatement
		Mockito.reset(connection, preparedStatement);	// clear counters of sensors
		testInstance.apply(values, connection);
		// prepared statement already created, should'nt happen again
		verify(connection, times(0)).prepareStatement(anyString());
	}
	
	@Test
	public void testApply_multiInsert() throws Exception {
		StatementValues values1 = new StatementValues();
		values1.putUpsertValue(colA, 1);
		values1.putUpsertValue(colB, 2);
		StatementValues values2 = new StatementValues();
		values2.putUpsertValue(colA, 3);
		values2.putUpsertValue(colB, 4);
		StatementValues values3 = new StatementValues();
		values3.putUpsertValue(colA, 5);
		values3.putUpsertValue(colB, 6);
		testInstance.apply(Arrays.asList(values1, values2, values3), connection);
		testInstance.execute();
		
		verify(connection, times(1)).prepareStatement(anyString());
		verify(preparedStatement, times(3)).addBatch();
		verify(preparedStatement, times(6)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("insert into Toto(A, B) values (?, ?)", sqlCaptor.getValue());
		Set<Map.Entry<Integer, Integer>> indexValuePairs = buildPairs(indexCaptor.getAllValues(), valueCaptor.getAllValues());
		assertEquals(pairChain(1, 1)._(2, 2)._(1, 3)._(2, 4)._(1, 5)._(2, 6)._(), indexValuePairs);
	}
	
	private static <K, V> LinkedHashSet<Map.Entry<K, V>> buildPairs(Iterable<K> values1, Iterable<V> values2) {
		LinkedHashSet<Map.Entry<K, V>> indexValuePairs = new LinkedHashSet<>();
		PairIterator<K, V> pairIterator = new PairIterator<>(values1, values2);
		while (pairIterator.hasNext()) {
			Map.Entry<K, V> pair = pairIterator.next();
			indexValuePairs.add(new AbstractMap.SimpleEntry<>(pair.getKey(), pair.getValue()));
		}
		return indexValuePairs;
	}
	
	private static <K, V> PairChain<K,V> pairChain(K k, V v) {
		return new PairChain<K, V>()._(k, v);
	}
	
	private static class PairChain<K, V> {
		Set<Map.Entry<K, V>> toReturn = new HashSet<>();
		public PairChain<K, V> _(K k, V v) {
			toReturn.add(new AbstractMap.SimpleEntry<>(k, v));
			return this;
		}
		public Set<Map.Entry<K, V>> _() {
			return toReturn;
		}
	}

	private static class Toto {
		private Integer a, b, c;
	}
}