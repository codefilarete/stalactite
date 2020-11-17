package org.gama.stalactite.persistence.engine.runtime;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import org.gama.lang.Duo;
import org.gama.lang.collection.Maps;
import org.gama.lang.exception.NotImplementedException;
import org.gama.lang.function.Sequence;
import org.gama.reflection.Accessors;
import org.gama.reflection.IMutator;
import org.gama.reflection.IReversibleAccessor;
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
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.gama.stalactite.test.PairSetList;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractDMLExecutorTest {
	
	@BeforeEach
	void initSequence() {
		InMemoryCounterIdentifierGenerator.INSTANCE.reset();
	}
	
	protected static abstract class AbstractDataSet<C, I> {
		
		protected final Dialect dialect = new Dialect(new JavaTypeToSqlTypeMapping()
				.with(Integer.class, "int"));
		
		protected PersistenceConfiguration<C, I, org.gama.stalactite.persistence.structure.Table> persistenceConfiguration;
		
		protected PreparedStatement preparedStatement;
		protected ArgumentCaptor<Integer> valueCaptor;
		protected ArgumentCaptor<Integer> indexCaptor;
		protected ArgumentCaptor<String> statementArgCaptor;
		protected JdbcConnectionProvider transactionManager;
		protected Connection connection;
		
		protected AbstractDataSet() throws SQLException {
			PersistenceConfigurationBuilder persistenceConfigurationBuilder = newPersistenceConfigurationBuilder();
			persistenceConfiguration = persistenceConfigurationBuilder.build();
			
			preparedStatement = mock(PreparedStatement.class);
			when(preparedStatement.executeBatch()).thenReturn(new int[] { 1 });
			
			connection = mock(Connection.class);
			// PreparedStatement.getConnection() must gives that instance of connection because of SQLOperation that checks
			// weither or not it should prepare statement
			when(preparedStatement.getConnection()).thenReturn(connection);
			statementArgCaptor = ArgumentCaptor.forClass(String.class);
			when(connection.prepareStatement(statementArgCaptor.capture())).thenReturn(preparedStatement);
			when(connection.prepareStatement(statementArgCaptor.capture(), anyInt())).thenReturn(preparedStatement);
			
			valueCaptor = ArgumentCaptor.forClass(Integer.class);
			indexCaptor = ArgumentCaptor.forClass(Integer.class);
			
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenReturn(connection);
			transactionManager = new JdbcConnectionProvider(dataSource);
		}
		
		protected abstract PersistenceConfigurationBuilder newPersistenceConfigurationBuilder();
	}
	
	protected static class DataSet extends AbstractDataSet<Toto, Integer> {
		
		protected DataSet() throws SQLException {
		}
		
		protected PersistenceConfigurationBuilder newPersistenceConfigurationBuilder() {
			return new PersistenceConfigurationBuilder<Toto, Integer, org.gama.stalactite.persistence.structure.Table>()
					.withTableAndClass("Toto", Toto.class, (mappedClass, primaryKeyField) -> {
						Sequence<Integer> instance = InMemoryCounterIdentifierGenerator.INSTANCE;
						IdentifierInsertionManager<Toto, Integer> identifierGenerator = new BeforeInsertIdentifierManager<>(
								new SinglePropertyIdAccessor<>(primaryKeyField), instance, Integer.class);
						return new ClassMappingStrategy<Toto, Integer, org.gama.stalactite.persistence.structure.Table>(
								mappedClass.mappedClass,
								mappedClass.targetTable,
								(Map) mappedClass.persistentFieldHarverster.getFieldToColumn(),
								primaryKeyField,
								identifierGenerator);
					})
					.withPrimaryKeyFieldName("a");
		}
	}
	
	protected static class DataSetWithComposedId extends AbstractDataSet<Toto, Toto> {
		
		protected DataSetWithComposedId() throws SQLException {
			super();
		}
		
		@Override
		protected PersistenceConfigurationBuilder newPersistenceConfigurationBuilder() {
			Table toto = new Table("Toto");
			Column colA = toto.addColumn("a", Integer.class).primaryKey();
			Column colB = toto.addColumn("b", Integer.class).primaryKey();
			Column colC = toto.addColumn("c", Integer.class);
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
			
			return new PersistenceConfigurationBuilder<Toto, Toto, Table>() {
				@Override
				protected IReversibleAccessor<Toto, Toto> buildPrimaryKeyAccessor(TableAndClass<Toto> tableAndClass) {
					return new IReversibleAccessor<Toto, Toto>() {
						
						@Override
						public Toto get(Toto toto) {
							return toto;
						}
						
						@Override
						public IMutator<Toto, Toto> toMutator() {
							throw new NotImplementedException("Because it has no purpose in those tests");
						}
					};
				}
			}
					.withTableAndClass("Toto", Toto.class, (mappedClass, primaryKeyField) ->
							new ClassMappingStrategy<Toto, Toto, Table>(
									Toto.class,
									toto,
									(Map) mappedClass.persistentFieldHarverster.getFieldToColumn(),
									new ComposedIdMappingStrategy<>(idAccessor, new AlreadyAssignedIdentifierManager<>(Toto.class, c -> {}, c -> false),
											new ComposedIdentifierAssembler<Toto>(toto.getPrimaryKey().getColumns()) {
												@Override
												protected Toto assemble(Map<Column, Object> primaryKeyElements) {
													// No need to be implemented id because we're on a delete test case, but it may be something 
													// like this :
													return new Toto((Integer) primaryKeyElements.get(colA), (Integer) primaryKeyElements.get(colB), null);
												}
												
												@Override
												public <T extends Table<T>> Map<Column<T, Object>, Object> getColumnValues(@Nonnull Toto id) {
													return Maps.asMap((Column<T, Object>) colA, (Object) id.a).add(colB, id.b);
												}
											})))
					;
		}
	}
	
	protected static class DataSetWithComposedId2 extends AbstractDataSet<Tata, ComposedId> {
		
		protected DataSetWithComposedId2() throws SQLException {
			super();
		}
		
		@Override
		protected PersistenceConfigurationBuilder newPersistenceConfigurationBuilder() {
			Table tata = new Table("Tata");
			Column colA = tata.addColumn("a", Integer.class).primaryKey();
			Column colB = tata.addColumn("b", Integer.class).primaryKey();
			Column colC = tata.addColumn("c", Integer.class);
			IdAccessor<Tata, ComposedId> idAccessor = new IdAccessor<Tata, ComposedId>() {
				@Override
				public ComposedId getId(Tata toto) {
					return toto.id;
				}
				
				@Override
				public void setId(Tata toto, ComposedId identifier) {
					toto.id = identifier;
				}
			};
			
			return new PersistenceConfigurationBuilder<Tata, ComposedId, Table>() {
				@Override
				protected IReversibleAccessor<Tata, ComposedId> buildPrimaryKeyAccessor(TableAndClass<Tata> tableAndClass) {
					return new IReversibleAccessor<Tata, ComposedId>() {
						
						@Override
						public ComposedId get(Tata tata) {
							return tata.id;
						}
						
						@Override
						public IMutator<Tata, ComposedId> toMutator() {
							throw new NotImplementedException("Because it has no purpose in those tests");
						}
					};
				}
				
				/** Overriden to skip automatic mapping by {@link PersistentFieldHarverster} to avoid wrong mapping */
				protected TableAndClass<Tata> map(Class<Tata> mappedClass, String tableName) {
					return new TableAndClass<>(tata, mappedClass, null);
				}
			}
					.withTableAndClass("Tata", Tata.class, (mappedClass, primaryKeyField) ->
							new ClassMappingStrategy<Tata, ComposedId, Table>(
									Tata.class,
									tata,
									Maps.asMap(Accessors.accessorByField(Tata.class, "c"), colC),
									new ComposedIdMappingStrategy<>(idAccessor, new AlreadyAssignedIdentifierManager<>(ComposedId.class, c -> {}, c -> false),
											new ComposedIdentifierAssembler<ComposedId>(tata.getPrimaryKey().getColumns()) {
												@Override
												protected ComposedId assemble(Map<Column, Object> primaryKeyElements) {
													// No need to be implemented id because we're on a delete test case, but it may be something 
													// like this :
													return new ComposedId((Integer) primaryKeyElements.get(colA), (Integer) primaryKeyElements.get(colB));
												}
												
												@Override
												public <T extends Table<T>> Map<Column<T, Object>, Object> getColumnValues(@Nonnull ComposedId id) {
													return Maps.asMap((Column<T, Object>) colA, (Object) id.a).add(colB, id.b);
												}
											})))
					;
		}
	}
	
	protected static class PersistenceConfiguration<C, I, T extends Table> {
		
		protected ClassMappingStrategy<C, I, T> classMappingStrategy;
		protected Table targetTable;
	}
	
	protected static class PersistenceConfigurationBuilder<C, I, T extends Table> {
		
		private Class<C> mappedClass;
		private BiFunction<TableAndClass<C>, IReversibleAccessor<C, I>, ClassMappingStrategy<C, I, T>> classMappingStrategyBuilder;
		private String tableName;
		private String primaryKeyFieldName;
		
		public PersistenceConfigurationBuilder withTableAndClass(String tableName, Class<C> mappedClass,
						BiFunction<TableAndClass<C>, IReversibleAccessor<C, I>, ClassMappingStrategy<C, I, T>> classMappingStrategyBuilder) {
			this.tableName = tableName;
			this.mappedClass = mappedClass;
			this.classMappingStrategyBuilder = classMappingStrategyBuilder;
			return this;
		}
		
		public PersistenceConfigurationBuilder withPrimaryKeyFieldName(String primaryKeyFieldName) {
			this.primaryKeyFieldName = primaryKeyFieldName;
			return this;
		}
		
		protected PersistenceConfiguration build() {
			PersistenceConfiguration toReturn = new PersistenceConfiguration();
			
			TableAndClass<C> tableAndClass = map(mappedClass, tableName);
			IReversibleAccessor<C, I> primaryKeyAccessor = buildPrimaryKeyAccessor(tableAndClass);
			
			toReturn.classMappingStrategy = classMappingStrategyBuilder.apply(tableAndClass, primaryKeyAccessor);
			toReturn.targetTable = tableAndClass.targetTable;
			
			return toReturn;
		}
		
		protected IReversibleAccessor<C, I> buildPrimaryKeyAccessor(TableAndClass<C> tableAndClass) {
			return Accessors.propertyAccessor(tableAndClass.configurePrimaryKey(primaryKeyFieldName));
		}
		
		protected TableAndClass<C> map(Class<C> mappedClass, String tableName) {
			Table targetTable = new Table(tableName);
			PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
			persistentFieldHarverster.mapFields(mappedClass, targetTable);
			return new TableAndClass<>(targetTable, mappedClass, persistentFieldHarverster);
		}
		
		protected static class TableAndClass<T> {
			
			protected final Table targetTable;
			protected final Class<T> mappedClass;
			protected final PersistentFieldHarverster persistentFieldHarverster;
			
			public TableAndClass(Table<?> targetTable, Class<T> mappedClass, PersistentFieldHarverster persistentFieldHarverster) {
				this.targetTable = targetTable;
				this.mappedClass = mappedClass;
				this.persistentFieldHarverster = persistentFieldHarverster;
			}
			
			protected Field configurePrimaryKey(String primaryKeyFieldName) {
				Field primaryKeyField = persistentFieldHarverster.getField(primaryKeyFieldName);
				Column primaryKey = persistentFieldHarverster.getColumn(Accessors.propertyAccessor(primaryKeyField));
				primaryKey.setPrimaryKey(true);
				return primaryKeyField;
			}
		}
		
	}
	
	public static void assertCapturedPairsEqual(AbstractDataSet dataSet, PairSetList<Integer, Integer> expectedPairs) {
		List<Duo<Integer, Integer>> obtainedPairs = PairSetList.toPairs(dataSet.indexCaptor.getAllValues(), dataSet.valueCaptor.getAllValues());
		List<Set<Duo<Integer, Integer>>> obtained = new ArrayList<>();
		int startIndex = 0;
		for (Set<Duo<Integer, Integer>> expectedPair : expectedPairs.asList()) {
			obtained.add(new HashSet<>(obtainedPairs.subList(startIndex, startIndex += expectedPair.size())));
		}
		assertEquals(expectedPairs.asList(), obtained);
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