package org.gama.stalactite.persistence.engine.runtime;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.gama.lang.Duo;
import org.gama.lang.collection.Maps;
import org.gama.lang.exception.Exceptions;
import org.gama.lang.function.Sequence;
import org.gama.lang.function.ThrowingBiFunction;
import org.gama.reflection.Accessors;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.InMemoryCounterIdentifierGenerator;
import org.gama.stalactite.persistence.id.assembly.ComposedIdentifierAssembler;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.BeforeInsertIdentifierManager;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.ComposedIdMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdAccessor;
import org.gama.stalactite.persistence.mapping.PersistentFieldHarverster;
import org.gama.stalactite.persistence.mapping.SinglePropertyIdAccessor;
import org.gama.stalactite.persistence.sql.dml.WriteOperationFactory;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.DataSourceConnectionProvider;
import org.gama.stalactite.sql.dml.SQLStatement;
import org.gama.stalactite.sql.dml.WriteOperation;
import org.gama.stalactite.sql.dml.WriteOperation.RowCountListener;
import org.gama.stalactite.test.PairSetList;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
abstract class AbstractDMLExecutorTest {
	
	protected JdbcMock jdbcMock;
	private Sequence<Integer> sequence;
	protected WriteOperationFactory noRowCountCheckWriteOperationFactory;
	
	@BeforeEach
	void init() {
		this.jdbcMock = new JdbcMock();
		// the sequence is initialized before each test to make it restart from 0 else some assertion will fail
		// in particular insertion ones
		this.sequence = new InMemoryCounterIdentifierGenerator();
		
		noRowCountCheckWriteOperationFactory = new WriteOperationFactory() {
			
			@Override
			protected <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator,
																		   ConnectionProvider connectionProvider,
																		   ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider,
																		   RowCountListener rowCountListener) {
				// we d'ont care about row count checker in thoses tests, so every statement will be created without it
				return new WriteOperation<ParamType>(sqlGenerator, connectionProvider, NOOP_COUNT_CHECKER) {
					@Override
					protected void prepareStatement(Connection connection) throws SQLException {
						this.preparedStatement = statementProvider.apply(connection, getSQL());
					}
				};
			}
		};
	}
	
	protected PersistenceConfiguration<Toto, Integer, Table> giveDefaultPersistenceConfiguration() {
		PersistenceConfiguration<Toto, Integer, Table> toReturn = new PersistenceConfiguration<>();

		Table targetTable = new Table("Toto");
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Map<PropertyAccessor<Toto, Object>, Column<Table, Object>> mappedFileds = persistentFieldHarverster.mapFields(Toto.class, targetTable);
		PropertyAccessor<Toto, Integer> primaryKeyAccessor = Accessors.propertyAccessor(persistentFieldHarverster.getField("a"));
		persistentFieldHarverster.getColumn(primaryKeyAccessor).primaryKey();
		IdentifierInsertionManager<Toto, Integer> identifierGenerator = new BeforeInsertIdentifierManager<>(
			new SinglePropertyIdAccessor<>(primaryKeyAccessor), sequence, Integer.class);

		toReturn.classMappingStrategy = new ClassMappingStrategy<>(
			Toto.class,
			targetTable,
			mappedFileds,
			primaryKeyAccessor,
			identifierGenerator);
		toReturn.targetTable = targetTable;
		
		return toReturn;
	}

	/**
	 * Gives a persistence configuration of {@link Toto} class which id is composed of {@code Toto.a} and {@code Toto.b} fields
	 */
	protected PersistenceConfiguration<Toto, Toto, Table> giveIdAsItselfPersistenceConfiguration() {
		Table targetTable = new Table("Toto");
		Column colA = targetTable.addColumn("a", Integer.class).primaryKey();
		Column colB = targetTable.addColumn("b", Integer.class).primaryKey();
		Column colC = targetTable.addColumn("c", Integer.class);
		IdAccessor<Toto, Toto> idAccessor = new IdAccessor<Toto, Toto>() {
			@Override
			public Toto getId(Toto toto) {
				return toto;
			}

			@Override
			public void setId(Toto toto, Toto identifier) {
				toto.a = identifier.a;
				toto.b = identifier.b;
			}
		};

		ComposedIdentifierAssembler<Toto> composedIdentifierAssembler = new ComposedIdentifierAssembler<Toto>(targetTable.getPrimaryKey().getColumns()) {
			@Override
			protected Toto assemble(Map<Column, Object> primaryKeyElements) {
				// No need to be implemented because we're on a delete test case, but it may be something 
				// like this :
				return new Toto((Integer) primaryKeyElements.get(colA), (Integer) primaryKeyElements.get(colB), null);
			}

			@Override
			public <T extends Table<T>> Map<Column<T, Object>, Object> getColumnValues(@Nonnull Toto id) {
				return Maps.asMap((Column<T, Object>) colA, (Object) id.a).add(colB, id.b);
			}
		};

		PersistenceConfiguration<Toto, Toto, Table> toReturn = new PersistenceConfiguration<>();

		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Map<PropertyAccessor<Toto, Object>, Column<Table, Object>> mappedFileds = persistentFieldHarverster.mapFields(Toto.class, targetTable);
		ComposedIdMappingStrategy<Toto, Toto> idMappingStrategy = new ComposedIdMappingStrategy<>(idAccessor,
			new AlreadyAssignedIdentifierManager<>(Toto.class, c -> {}, c -> false),
			composedIdentifierAssembler);

		toReturn.classMappingStrategy = new ClassMappingStrategy<>(
			Toto.class,
			targetTable,
			mappedFileds,
			idMappingStrategy);
		toReturn.targetTable = targetTable;

		return toReturn;
	}

	/**
	 * Gives a persistence configuration of {@link Tata} class which id is {@link ComposedId}
	 */
	protected PersistenceConfiguration<Tata, ComposedId, Table> giveComposedIdPersistenceConfiguration() {
		Table targetTable = new Table("Tata");
		Column colA = targetTable.addColumn("a", Integer.class).primaryKey();
		Column colB = targetTable.addColumn("b", Integer.class).primaryKey();
		Column colC = targetTable.addColumn("c", Integer.class);
		IdAccessor<Tata, ComposedId> idAccessor = new IdAccessor<Tata, ComposedId>() {
			@Override
			public ComposedId getId(Tata tata) {
				return tata.id;
			}

			@Override
			public void setId(Tata tata, ComposedId identifier) {
				tata.id = identifier;
			}
		};
		
		ComposedIdentifierAssembler<ComposedId> composedIdentifierAssembler = new ComposedIdentifierAssembler<ComposedId>(targetTable.getPrimaryKey().getColumns()) {
			@Override
			protected ComposedId assemble(Map<Column, Object> primaryKeyElements) {
				// No need to be implemented because we're on a delete test case, but it may be something 
				// like this :
				return new ComposedId((Integer) primaryKeyElements.get(colA), (Integer) primaryKeyElements.get(colB));
			}

			@Override
			public <T extends Table<T>> Map<Column<T, Object>, Object> getColumnValues(@Nonnull ComposedId id) {
				return Maps.asMap((Column<T, Object>) colA, (Object) id.a).add(colB, id.b);
			}
		};

		PersistenceConfiguration<Tata, ComposedId, Table> toReturn = new PersistenceConfiguration<>();

		ComposedIdMappingStrategy<Tata, ComposedId> idMappingStrategy = new ComposedIdMappingStrategy<>(idAccessor,
			new AlreadyAssignedIdentifierManager<>(ComposedId.class, c -> {}, c -> false),
			composedIdentifierAssembler);

		toReturn.classMappingStrategy = new ClassMappingStrategy<>(
			Tata.class,
			targetTable,
			Maps.asMap(Accessors.accessorByField(Tata.class, "c"), colC),
			idMappingStrategy);
		toReturn.targetTable = targetTable;

		return toReturn;
	}


	protected static class JdbcMock {
		
		protected PreparedStatement preparedStatement;
		protected ArgumentCaptor<Integer> valueCaptor;
		protected ArgumentCaptor<Integer> indexCaptor;
		protected ArgumentCaptor<String> sqlCaptor;
		protected ConnectionProvider transactionManager;
		protected Connection connection;
		
		protected JdbcMock() {
			try {
				preparedStatement = mock(PreparedStatement.class);
				when(preparedStatement.executeLargeBatch()).thenReturn(new long[]{1});

				connection = mock(Connection.class);
				// PreparedStatement.getConnection() must gives that instance of connection because of SQLOperation that checks
				// weither or not it should prepare statement
				when(preparedStatement.getConnection()).thenReturn(connection);
				sqlCaptor = ArgumentCaptor.forClass(String.class);
				when(connection.prepareStatement(sqlCaptor.capture())).thenReturn(preparedStatement);
				when(connection.prepareStatement(sqlCaptor.capture(), anyInt())).thenReturn(preparedStatement);

				valueCaptor = ArgumentCaptor.forClass(Integer.class);
				indexCaptor = ArgumentCaptor.forClass(Integer.class);

				DataSource dataSource = mock(DataSource.class);
				when(dataSource.getConnection()).thenReturn(connection);
				transactionManager = new DataSourceConnectionProvider(dataSource);
			} catch (SQLException e) {
				// this should not happen since every thing is mocked, left as safeguard, and avoid catching
				// exception by caller which don't know what to do with the exception else same thing as here
				throw Exceptions.asRuntimeException(e);
			}
		}
	}
	
	protected static class PersistenceConfiguration<C, I, T extends Table> {
		
		protected ClassMappingStrategy<C, I, T> classMappingStrategy;
		protected Table targetTable;
	}
	
	public static void assertCapturedPairsEqual(JdbcMock jdbcMock, PairSetList<Integer, Integer> expectedPairs) {
		List<Duo<Integer, Integer>> obtainedPairs = PairSetList.toPairs(jdbcMock.indexCaptor.getAllValues(), jdbcMock.valueCaptor.getAllValues());
		List<Set<Duo<Integer, Integer>>> obtained = new ArrayList<>();
		int startIndex = 0;
		// rearranging obtainedPairs into some packets, as those of expectedPairs
		for (Set<Duo<Integer, Integer>> expectedPair : expectedPairs.asList()) {
			obtained.add(new HashSet<>(obtainedPairs.subList(startIndex, startIndex += expectedPair.size())));
		}
		assertThat(obtained).isEqualTo(expectedPairs.asList());
	}
	
	/**
	 * Class to be persisted
	 */
	protected static class Toto {
		protected Integer a;
		protected Integer b;
		protected Integer c;
		
		public Toto() {
		}
		
		public Toto(Integer a, Integer b, Integer c) {
			this.a = a;
			this.b = b;
			this.c = c;
		}
		
		/**
		 * Contructor that doesn't set identifier. Created to test identifier auto-insertion.
		 * @param b
		 * @param c
		 */
		public Toto(Integer b, Integer c) {
			this.b = b;
			this.c = c;
		}
		
		/** Implemented to ease comparison on tests */
		@Override
		public String toString() {
			return "Toto{a=" + a + ", b=" + b + ", c=" + c + '}';
		}
	}
	
	/**
	 * Class to be persisted
	 */
	protected static class Tata {
		protected ComposedId id;
		protected Integer c;
		
		public Tata() {
		}
		
		public Tata(Integer a, Integer b, Integer c) {
			this.id = new ComposedId(a, b);
			this.c = c;
		}
		
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Tata tata = (Tata) o;
			return Objects.equals(id, tata.id) &&
					Objects.equals(c, tata.c);
		}
		
		@Override
		public int hashCode() {
			return Objects.hash(id, c);
		}
		
		/** Implemented to ease comparison on tests */
		@Override
		public String toString() {
			return "Tata{id=" + id + ", c=" + c + '}';
		}
	}
	
	/**
	 * Composed identifier
	 */
	protected static class ComposedId {
		protected Integer a;
		protected Integer b;
		
		public ComposedId() {
		}
		
		public ComposedId(Integer a, Integer b) {
			this.a = a;
			this.b = b;
		}
		
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			ComposedId that = (ComposedId) o;
			return Objects.equals(a, that.a) &&
					Objects.equals(b, that.b);
		}
		
		@Override
		public int hashCode() {
			return Objects.hash(a, b);
		}
		
		/** Implemented to ease comparison on tests */
		@Override
		public String toString() {
			return "ComposedId{a=" + a + ", b=" + b + '}';
		}
	}
	
}