package org.codefilarete.stalactite.engine.runtime;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.codefilarete.reflection.AccessorByField;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.InMemoryCounterIdentifierGenerator;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.mapping.PersistentFieldHarvester;
import org.codefilarete.stalactite.mapping.SinglePropertyIdAccessor;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.BeforeInsertIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Maps;

/**
 * @author Guillaume Mary
 */
abstract class DMLExecutorTest {
	
	protected static <T extends Table<T>> PersistenceConfiguration<Toto, Integer, T> giveDefaultPersistenceConfiguration() {
		PersistenceConfiguration<Toto, Integer, T> toReturn = new PersistenceConfiguration<>();

		T targetTable = (T) new Table("Toto");
		PersistentFieldHarvester persistentFieldHarvester = new PersistentFieldHarvester();
		Map<PropertyAccessor<Toto, Object>, Column<T, Object>> mappedFileds = persistentFieldHarvester.mapFields(Toto.class, targetTable);
		PropertyAccessor<Toto, Integer> primaryKeyAccessor = Accessors.propertyAccessor(persistentFieldHarvester.getField("a"));
		persistentFieldHarvester.getColumn(primaryKeyAccessor).primaryKey();
		IdentifierInsertionManager<Toto, Integer> identifierGenerator = new BeforeInsertIdentifierManager<>(
			new SinglePropertyIdAccessor<>(primaryKeyAccessor), new InMemoryCounterIdentifierGenerator(), Integer.class);

		toReturn.classMappingStrategy = new ClassMapping<>(
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
	protected static <T extends Table<T>> PersistenceConfiguration<Toto, Toto, T> giveIdAsItselfPersistenceConfiguration() {
		T targetTable = (T) new Table("Toto");
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

		ComposedIdentifierAssembler<Toto, ?> composedIdentifierAssembler = new ComposedIdentifierAssembler<Toto, T>(targetTable.getPrimaryKey()) {
			@Override
			public Toto assemble(Function<Column<?, ?>, Object> columnValueProvider) {
				// No need to be implemented because we're on a delete test case, but it may be something 
				// like this :
				return new Toto((Integer) columnValueProvider.apply(colA), (Integer) columnValueProvider.apply(colB), null);
			}

			@Override
			public Map<Column<T, Object>, Object> getColumnValues(Toto id) {
				return Maps.forHashMap((Class<Column<Table, Object>>) (Class) Column.class, Object.class)
						.add(colA, id.a).add(colB, id.b);
			}
		};

		PersistenceConfiguration<Toto, Toto, T> toReturn = new PersistenceConfiguration<>();

		PersistentFieldHarvester persistentFieldHarvester = new PersistentFieldHarvester();
		Map<PropertyAccessor<Toto, Object>, Column<T, Object>> mappedFields = persistentFieldHarvester.mapFields(Toto.class, targetTable);
		ComposedIdMapping<Toto, Toto> idMappingStrategy = new ComposedIdMapping<>(idAccessor,
																				  new AlreadyAssignedIdentifierManager<>(Toto.class, c -> {}, c -> false),
																				  composedIdentifierAssembler);
		
		toReturn.classMappingStrategy = new ClassMapping<>(
				Toto.class,
				targetTable,
				mappedFields,
				idMappingStrategy);
		toReturn.targetTable = targetTable;

		return toReturn;
	}

	/**
	 * Gives a persistence configuration of {@link Tata} class which id is {@link ComposedId}
	 */
	protected static <T extends Table<T>> PersistenceConfiguration<Tata, ComposedId, T> giveComposedIdPersistenceConfiguration() {
		T targetTable = (T) new Table("Tata");
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
		
		ComposedIdentifierAssembler<ComposedId, ?> composedIdentifierAssembler = new ComposedIdentifierAssembler<ComposedId, T>(targetTable.getPrimaryKey()) {
			@Override
			public ComposedId assemble(Function<Column<?, ?>, Object> columnValueProvider) {
				// No need to be implemented because we're on a delete test case, but it may be something 
				// like this :
				return new ComposedId((Integer) columnValueProvider.apply(colA), (Integer) columnValueProvider.apply(colB));
			}

			@Override
			public Map<Column<T, Object>, Object> getColumnValues(ComposedId id) {
				return Maps.forHashMap((Class<Column<Table, Object>>) (Class) Column.class, Object.class)
						.add(colA, id.a).add(colB, id.b);
			}
		};

		PersistenceConfiguration<Tata, ComposedId, T> toReturn = new PersistenceConfiguration<>();

		ComposedIdMapping<Tata, ComposedId> idMappingStrategy = new ComposedIdMapping<>(idAccessor,
																						new AlreadyAssignedIdentifierManager<>(ComposedId.class, c -> {}, c -> false),
																						composedIdentifierAssembler);
		
		Map<AccessorByField<Tata, Object>, Column<T, Object>> mappedFields = Maps.asMap(Accessors.accessorByField(Tata.class, "c"), colC);
		toReturn.classMappingStrategy = new ClassMapping<>(
				Tata.class,
				targetTable,
				mappedFields,
				idMappingStrategy);
		toReturn.targetTable = targetTable;

		return toReturn;
	}


	protected static class PersistenceConfiguration<C, I, T extends Table<T>> {
		
		protected ClassMapping<C, I, T> classMappingStrategy;
		protected T targetTable;
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
		 * Constructor that doesn't set identifier. Created to test identifier auto-insertion.
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