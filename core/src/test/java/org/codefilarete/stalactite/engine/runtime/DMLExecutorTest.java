package org.codefilarete.stalactite.engine.runtime;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;

import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.InMemoryCounterIdentifierGenerator;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.BeforeInsertIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.mapping.ClassMappingStrategy;
import org.codefilarete.stalactite.mapping.ComposedIdMappingStrategy;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.mapping.PersistentFieldHarverster;
import org.codefilarete.stalactite.mapping.SinglePropertyIdAccessor;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Maps;

/**
 * @author Guillaume Mary
 */
class DMLExecutorTest {
	
	protected PersistenceConfiguration<Toto, Integer, Table> giveDefaultPersistenceConfiguration() {
		PersistenceConfiguration<Toto, Integer, Table> toReturn = new PersistenceConfiguration<>();

		Table targetTable = new Table("Toto");
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Map<PropertyAccessor<Toto, Object>, Column<Table, Object>> mappedFileds = persistentFieldHarverster.mapFields(Toto.class, targetTable);
		PropertyAccessor<Toto, Integer> primaryKeyAccessor = Accessors.propertyAccessor(persistentFieldHarverster.getField("a"));
		persistentFieldHarverster.getColumn(primaryKeyAccessor).primaryKey();
		IdentifierInsertionManager<Toto, Integer> identifierGenerator = new BeforeInsertIdentifierManager<>(
			new SinglePropertyIdAccessor<>(primaryKeyAccessor), new InMemoryCounterIdentifierGenerator(), Integer.class);

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


	protected static class PersistenceConfiguration<C, I, T extends Table> {
		
		protected ClassMappingStrategy<C, I, T> classMappingStrategy;
		protected Table targetTable;
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