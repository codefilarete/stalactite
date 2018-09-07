package org.gama.stalactite.persistence.engine.cascade;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.reflection.Accessors;
import org.gama.reflection.IReversibleAccessor;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.dml.ReadOperation;
import org.gama.sql.result.InMemoryResultSet;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.sql.test.MariaDBEmbeddableDataSource;
import org.gama.stalactite.persistence.engine.DDLDeployer;
import org.gama.stalactite.persistence.id.assembly.ComposedIdentifierAssembler;
import org.gama.stalactite.persistence.id.assembly.IdentifierAssembler;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.ComposedIdMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdAccessor;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.HSQLDBDialect.HSQLDBTypeMapping;
import org.gama.stalactite.persistence.sql.ddl.DDLSchemaGenerator;
import org.gama.stalactite.persistence.sql.dml.ColumnParamedSelect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
public class JoinedStrategiesSelectExecutorTest {
	
	public static Object[][] selectData() {
		Table tableWith1ColumnedPK = new Table("Toto");
		Column id1PK = tableWith1ColumnedPK.addColumn("id1", long.class).primaryKey();
		Table tableWith2ColumnedPK = new Table("Toto");
		Column id2PK1 = tableWith2ColumnedPK.addColumn("id1", long.class).primaryKey();
		Column id2PK2 = tableWith2ColumnedPK.addColumn("id2", long.class).primaryKey();
		Table tableWith3ColumnedPK = new Table("Toto");
		Column id3PK1 = tableWith3ColumnedPK.addColumn("id1", long.class).primaryKey();
		Column id3PK2 = tableWith3ColumnedPK.addColumn("id2", long.class).primaryKey();
		Column id3PK3 = tableWith3ColumnedPK.addColumn("id3", long.class).primaryKey();
		
		return new Object[][] {
				{ tableWith1ColumnedPK, Arrays.asList(
						"select Toto.id1 as Toto_id1 from Toto where Toto.id1 in (?, ?, ?)",
						"select Toto.id1 as Toto_id1 from Toto where Toto.id1 in (?)") },
				{ tableWith2ColumnedPK, Arrays.asList(
						"select Toto.id1 as Toto_id1, Toto.id2 as Toto_id2 from Toto where (Toto.id1, Toto.id2) in ((?, ?), (?, ?), (?, ?))",
						"select Toto.id1 as Toto_id1, Toto.id2 as Toto_id2 from Toto where (Toto.id1, Toto.id2) in ((?, ?))") },
				{ tableWith3ColumnedPK, Arrays.asList(
						"select Toto.id1 as Toto_id1, Toto.id2 as Toto_id2, Toto.id3 as Toto_id3 from Toto where (Toto.id1, Toto.id2, Toto.id3)" +
						" in ((?, ?, ?), (?, ?, ?), (?, ?, ?))",
						"select Toto.id1 as Toto_id1, Toto.id2 as Toto_id2, Toto.id3 as Toto_id3 from Toto where (Toto.id1, Toto.id2, Toto.id3)" +
						" in ((?, ?, ?))") },
		};
	}
	
	@ParameterizedTest
	@MethodSource("selectData")
	public void testSelect(Table targetTable, List<String> expectedSql) {
		ClassMappingStrategy<Object, Object, Table> classMappingStrategy = JoinedStrategiesSelectTest.buildMappingStrategyMock(targetTable);
		// mocking to prevent NPE from JoinedStrategiesSelectExecutor constructor
		IdMappingStrategy idMappingStrategyMock = mock(IdMappingStrategy.class);
		when(classMappingStrategy.getIdMappingStrategy()).thenReturn(idMappingStrategyMock);
		
		List<String> capturedSQL = new ArrayList<>();
		Dialect dialect = new Dialect();
		// we set a in operator size to test overflow
		dialect.setInOperatorMaxSize(3);
		JoinedStrategiesSelectExecutor testInstance = new JoinedStrategiesSelectExecutor(classMappingStrategy, dialect, mock(ConnectionProvider.class)) {
			@Override
			List execute(ConnectionProvider connectionProvider, String sql, Collection idsParcels, Map inOperatorValueIndexes) {
				capturedSQL.add(sql);
				return Collections.emptyList();
			}
		};
		testInstance.select(Arrays.asList(11, 17, 23, 29));
		assertEquals(expectedSql, capturedSQL);
	}
	
	@Test
	public void testExecute() {
		Table targetTable = new Table("Toto");
		Column id = targetTable.addColumn("id", long.class).primaryKey();
		
		ClassMappingStrategy<Object, Object, Table> classMappingStrategy = JoinedStrategiesSelectTest.buildMappingStrategyMock(targetTable);
		// mocking to prevent NPE from JoinedStrategiesSelectExecutor constructor
		IdMappingStrategy idMappingStrategyMock = mock(IdMappingStrategy.class);
		when(classMappingStrategy.getIdMappingStrategy()).thenReturn(idMappingStrategyMock);
		
		// mocking to provide entity values
		IdentifierAssembler identifierAssemblerMock = mock(IdentifierAssembler.class);
		when(idMappingStrategyMock.getIdentifierAssembler()).thenReturn(identifierAssemblerMock);
		Map<Column<Table, Object>, Object> idValuesPerEntity = Maps.asMap(id, Arrays.asList(10, 20));
		when(identifierAssemblerMock.getColumnValues(anyList())).thenReturn(idValuesPerEntity);
		
		// mocking ResultSet transformation because we don't care about it in this test
		ReadOperation readOperationMock = mock(ReadOperation.class);
		when(readOperationMock.execute()).thenReturn(new InMemoryResultSet(Collections.emptyList()));
		when(readOperationMock.getSqlStatement()).thenReturn(new ColumnParamedSelect("", new HashMap<>(), new HashMap<>(), new HashMap<>()));
		
		// we're going to check if values are correctly passed to the underlying ReadOperation
		JoinedStrategiesSelectExecutor testInstance = new JoinedStrategiesSelectExecutor<>(classMappingStrategy, new Dialect(), mock(ConnectionProvider.class));
		ArgumentCaptor<Map> capturedValues = ArgumentCaptor.forClass(Map.class);
		testInstance.execute(readOperationMock, Arrays.asList(1, 2));
		
		verify(readOperationMock).setValues(capturedValues.capture());
		assertEquals(Maps.asMap(id, Arrays.asList(10, 20)), capturedValues.getValue());
	}
	
	public static Object[][] datasources() {
		return new Object[][] {
				// NB: Derby can't be tested because it doesn't support "tupled in"
				new Object[] { new HSQLDBInMemoryDataSource() },
				new Object[] { new MariaDBEmbeddableDataSource(3406) },
				
		};
	}
	
	@ParameterizedTest
	@MethodSource("datasources")
	public void testExecute_realLife_composedId(DataSource dataSource) throws SQLException {
		Table targetTable = new Table("Toto");
		Column<Table, Long> id1 = targetTable.addColumn("id1", long.class).primaryKey();
		Column<Table, Long> id2 = targetTable.addColumn("id2", long.class).primaryKey();
		Column<Table, String> name = targetTable.addColumn("name", String.class);
		
		JdbcConnectionProvider connectionProvider = new JdbcConnectionProvider(dataSource);
		DDLSchemaGenerator ddlSchemaGenerator = new DDLSchemaGenerator(new HSQLDBTypeMapping());
		ddlSchemaGenerator.addTables(targetTable);
		DDLDeployer ddlDeployer = new DDLDeployer(ddlSchemaGenerator, connectionProvider);
		ddlDeployer.deployDDL();
		
		Toto entity1 = new Toto(100, 1, "entity1");
		Toto entity2 = new Toto(200, 2, "entity2");
		Connection currentConnection = connectionProvider.getCurrentConnection();
		PreparedStatement insertStatement = currentConnection.prepareStatement("insert into Toto(id1, id2, name) values (?, ?, ?)");
		insertStatement.setLong(1, entity1.id1);
		insertStatement.setLong(2, entity1.id2);
		insertStatement.setString(3, entity1.name);
		insertStatement.addBatch();
		insertStatement.setLong(1, entity2.id1);
		insertStatement.setLong(2, entity2.id2);
		insertStatement.setString(3, entity2.name);
		insertStatement.addBatch();
		insertStatement.executeBatch();
		insertStatement.close();
		currentConnection.commit();
		
		IdAccessor<Toto, Toto> idAccessor =
				IdAccessor.idAccessor(Accessors.accessorByMethodReference(SerializableFunction.identity(), (toto, toto2) -> {
					toto.id1 = toto2.id1;
					toto.id2 = toto2.id2;
				}));
		ClassMappingStrategy<Toto, Toto, Table> classMappingStrategy = new ClassMappingStrategy<Toto, Toto, Table>(Toto.class, targetTable,
				(Map)
				Maps.asMap((IReversibleAccessor) Accessors.accessorByMethodReference(Toto::getId1, Toto::setId1), (Column) id1)
						.add(Accessors.accessorByMethodReference(Toto::getId2, Toto::setId2), id2)
						.add(Accessors.accessorByMethodReference(Toto::getName, Toto::setName), name),
				new ComposedIdMappingStrategy<>(idAccessor, new AlreadyAssignedIdentifierManager<>(Toto.class), new ComposedIdentifierAssembler<Toto>(targetTable) {
					@Override
					protected Toto assemble(Map<Column, Object> primaryKeyElements) {
						return new Toto((long) primaryKeyElements.get(id1), (long) primaryKeyElements.get(id2));
					}
					
					@Override
					public <T extends Table<T>> Map<Column<T, Object>, Object> getColumnValues(@Nonnull Toto id) {
						return Maps.asMap((Column<T, Object>) (Column) id1, (Object) id.id1).add((Column<T, Object>) (Column) id2, id.id2);
					}
				})
		);
		
		// Checking that selected entities by their id are those expected
		JoinedStrategiesSelectExecutor<Toto, Toto> testInstance = new JoinedStrategiesSelectExecutor<>(classMappingStrategy, new Dialect(), connectionProvider);
		List<Toto> select = testInstance.select(Arrays.asList(new Toto(100, 1)));
		assertEquals(Arrays.asList(entity1).toString(), select.toString());
		
		select = testInstance.select(Arrays.asList(new Toto(100, 1), new Toto(200, 2)));
		assertEquals(Arrays.asList(entity1, entity2).toString(), select.toString());
	}
	
	private static class Toto {
		private long id1;
		private long id2;
		private String name;
		
		public Toto() {
		}
		
		public Toto(long id1, long id2) {
			this.id1 = id1;
			this.id2 = id2;
		}
		
		public Toto(long id1, long id2, String name) {
			this.id1 = id1;
			this.id2 = id2;
			this.name = name;
		}
		
		public long getId1() {
			return id1;
		}
		
		public void setId1(long id1) {
			this.id1 = id1;
		}
		
		public long getId2() {
			return id2;
		}
		
		public void setId2(long id2) {
			this.id2 = id2;
		}
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		@Override
		public String toString() {
			return "Toto{id1=" + id1 + ", id2=" + id2 + ", name='" + name + '\'' + '}';
		}
	}
}