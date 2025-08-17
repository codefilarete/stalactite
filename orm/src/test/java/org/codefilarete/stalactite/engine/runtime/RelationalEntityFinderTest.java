package org.codefilarete.stalactite.engine.runtime;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.InMemoryResultSet;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.INNER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RelationalEntityFinderTest {
	
	@Test
	<T extends Table<T>> void select() throws SQLException {
		
		// mocking executeQuery not to return null because select method will use the in-memory ResultSet
		String totoIdAlias = "Toto_id";
		String totoAAlias = "Toto_a";
		String totoBAlias = "Toto_b";
		String totoQAlias = "Toto_q";
		String tataIdAlias = "Tata_id";
		ResultSet resultSetMock = new InMemoryResultSet(Arrays.asList(
				Maps.asMap(totoIdAlias, (Object) 7).add(totoAAlias, 1).add(totoBAlias, 2).add(tataIdAlias, 7),
				Maps.asMap(totoIdAlias, (Object) 13).add(totoAAlias, 1).add(totoBAlias, 2).add(tataIdAlias, 13),
				Maps.asMap(totoIdAlias, (Object) 17).add(totoAAlias, 1).add(totoBAlias, 2).add(tataIdAlias, 17),
				Maps.asMap(totoIdAlias, (Object) 23).add(totoAAlias, 1).add(totoBAlias, 2).add(tataIdAlias, 23)
		));
		PreparedStatement preparedStatement = mock(PreparedStatement.class);
		when(preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		Table totoTable = new Table("Toto");
		Column totoColId = totoTable.addColumn("id", Integer.class).primaryKey();
		PrimaryKey<?, Integer> totoPrimaryKey = totoTable.getPrimaryKey();
		// column for "noise" in select
		Column totoNameColumn = totoTable.addColumn("name", String.class);
		
		Map<ReversibleAccessor<Toto, Object>, Column<T, Object>> totoClassMapping = (Map) new ValueAccessPointMap<>();
		totoClassMapping.put(Accessors.propertyAccessor(Toto.class, "id"), totoColId);
		
		ClassMapping<Toto, Integer, T> totoMapping = new ClassMapping<>(
				Toto.class,
				(T) totoTable,
				totoClassMapping,
				Accessors.propertyAccessor(Toto.class, "id"),
				// Basic mapping to prevent NullPointerException, even if it's not the goal of our test
				new AlreadyAssignedIdentifierManager<>(Integer.class, c -> {
				}, c -> false));
		
		Table tataTable = new Table("Tata");
		Column tataColId = tataTable.addColumn("id", Integer.class);
		tataColId.primaryKey();
		PrimaryKey<?, Integer> tataPrimaryKey = tataTable.getPrimaryKey();
		// column for "noise" in select
		Column tataNameColumn = tataTable.addColumn("name", String.class);
		
		Map<ReversibleAccessor<Tata, Object>, Column<T, Object>> tataClassMapping = (Map) new ValueAccessPointMap<>();
		tataClassMapping.put(Accessors.propertyAccessor(Tata.class, "id"), tataColId);
		
		
		ClassMapping<Tata, Integer, T> tataMapping = new ClassMapping<>(
				Tata.class,
				(T) totoTable,
				tataClassMapping,
				Accessors.propertyAccessor(Tata.class, "id"),
				// Basic mapping to prevent NullPointerException, even if it's not the goal of our test
				new AlreadyAssignedIdentifierManager<>(Integer.class, c -> {
				}, c -> false));
		
		// Given following tree:
		// Toto.id = Tata.id (X)
		EntityJoinTree<Toto, Integer> entityJoinTree = new EntityJoinTree<>(new EntityMappingAdapter<>(totoMapping), totoMapping.getTargetTable());
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_JOIN_NAME, new EntityMappingAdapter<>(tataMapping), Accessors.accessorByMethodReference(Toto::getTata),
                totoPrimaryKey, tataPrimaryKey, null, INNER, Toto::setTata, Collections.emptySet());
		
		ConnectionProvider connectionProvider = Mockito.mock(ConnectionProvider.class);
		Connection connectionMock = mock(Connection.class);
		when(connectionProvider.giveConnection()).thenReturn(connectionMock);
		when(connectionMock.prepareStatement(any())).thenReturn(preparedStatement);
		
		RelationalEntityFinder<Toto, Integer, ?> testInstance = new RelationalEntityFinder<>(entityJoinTree, connectionProvider, new DefaultDialect(), true);
		
		Set<Toto> totos = testInstance.selectFromQueryBean("select Toto.id as Toto_id, Tata.id as Tata_id from Toto inner join Tata on Toto.id = Tata.id" +
				" where Toto.id = :toto_id", Maps.asMap("toto_id", 7));
		Toto expectedResult1 = new Toto(7, null, null);
		expectedResult1.setTata(new Tata(7));
		Toto expectedResult2 = new Toto(13, null, null);
		expectedResult2.setTata(new Tata(13));
		Toto expectedResult3 = new Toto(17, null, null);
		expectedResult3.setTata(new Tata(17));
		Toto expectedResult4 = new Toto(23, null, null);
		expectedResult4.setTata(new Tata(23));
		assertThat(totos)
				.usingRecursiveFieldByFieldElementComparator()
				.containsExactlyInAnyOrder(expectedResult1, expectedResult2, expectedResult3, expectedResult4);
	}
	
	private static class Toto {
		private Integer id;
		private Integer a, b, x, y, z;
		private Set<Integer> q;
		
		private Tata tata;
		
		public Toto() {
		}
		
		public Toto(Integer id, Integer a, Integer b) {
			this(id, a, b, null, null, null);
		}
		
		public Toto(Integer id, Integer a, Integer b, Integer x, Integer y, Integer z) {
			this.id = id;
			this.a = a;
			this.b = b;
			this.x = x;
			this.y = y;
			this.z = z;
		}
		
		public Toto(Integer a, Integer b, Integer x, Integer y, Integer z) {
			this.a = a;
			this.b = b;
			this.x = x;
			this.y = y;
			this.z = z;
		}
		
		public Integer getId() {
			return id;
		}
		
		public Integer getA() {
			return a;
		}
		
		public void setQ(Set<Integer> q) {
			this.q = q;
		}
		
		public Set<Integer> getQ() {
			return q;
		}
		
		public void setTata(Tata tata) {
			this.tata = tata;
		}
		
		public Tata getTata() {
			return tata;
		}
		
		@Override
		public String toString() {
			return getClass().getSimpleName() + "["
					+ Maps.asMap("id", (Object) id).add("a", a).add("b", b).add("x", x).add("y", y).add("z", z).add("tata", tata)
					+ "]";
		}
	}
	
	private static class Tata {
		
		private Integer id;
		
		private String prop1;
		
		public Tata() {
		}
		
		public Tata(Integer id) {
			this.id = id;
		}
		
		public Integer getId() {
			return id;
		}
		
		public String getProp1() {
			return prop1;
		}
		
		@Override
		public String toString() {
			return getClass().getSimpleName() + "["
					+ Maps.asMap("id", (Object) id).add("prop1", prop1)
					+ "]";
		}
	}
	
}