package org.gama.stalactite.persistence.engine;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.gama.lang.Duo;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface PolymorphismPolicy<C, I> {
	
	static <C, I> TablePerClassPolymorphism<C, I> tablePerClass() {
		return new TablePerClassPolymorphism<>();
	}
	
	static <C, I> JoinedTablesPolymorphism<C, I> joinedTables() {
		return new JoinedTablesPolymorphism<>();
	}
	
	/**
	 * Creates a single-table polymorphism configuration with a default discriminating column names "DTYPE" of {@link String} type
	 * @param <C> entity type
	 * @param <I> identifier type
	 * @return a new {@link SingleTablePolymorphism} with "DTYPE" as String discriminator column
	 */
	static <C, I> SingleTablePolymorphism<C, I, String> singleTable() {
		return singleTable("DTYPE");
	}
	
	static <C, I> SingleTablePolymorphism<C, I, String> singleTable(String discriminatorColumnName) {
		return new SingleTablePolymorphism<>(discriminatorColumnName, String.class);
	}
	
	Set<SubEntityMappingConfiguration<? extends C, I>> getSubClasses();
	
	class TablePerClassPolymorphism<C, I> implements PolymorphismPolicy {
		
		private final Set<Duo<SubEntityMappingConfiguration<? extends C, I>, Table /* Nullable */>> subClasses = new HashSet<>();
		
		public TablePerClassPolymorphism<C, I> addSubClass(SubEntityMappingConfiguration<? extends C, Object> entityMappingConfigurationProvider) {
			addSubClass(entityMappingConfigurationProvider, null);
			return this;
		}
		
		public TablePerClassPolymorphism<C, I> addSubClass(SubEntityMappingConfiguration<? extends C, Object> entityMappingConfigurationProvider, @Nullable Table table) {
			subClasses.add(new Duo<>((SubEntityMappingConfiguration<? extends C, I>) entityMappingConfigurationProvider, table));
			return this;
		}
		
		public Set<SubEntityMappingConfiguration<? extends C, I>> getSubClasses() {
			return Iterables.collect(subClasses, Duo::getLeft, HashSet::new);
		}
		
		@Nullable
		public Table giveTable(SubEntityMappingConfiguration<? extends C, I> key) {
			return Iterables.find(subClasses, duo -> duo.getLeft().equals(key)).getRight();
		}
	}
	
	class JoinedTablesPolymorphism<C, I> implements PolymorphismPolicy {
		
		private final Set<Duo<SubEntityMappingConfiguration<? extends C, I>, Table /* Nullable */>> subClasses = new HashSet<>();
		
		public JoinedTablesPolymorphism<C, I> addSubClass(SubEntityMappingConfiguration<? extends C, Object> entityMappingConfigurationProvider) {
			addSubClass(entityMappingConfigurationProvider, null);
			return this;
		}
		
		public JoinedTablesPolymorphism<C, I> addSubClass(SubEntityMappingConfiguration<? extends C, Object> entityMappingConfigurationProvider, @Nullable Table table) {
			subClasses.add(new Duo<>((SubEntityMappingConfiguration<? extends C, I>) entityMappingConfigurationProvider, table));
			return this;
		}
		
		public Set<SubEntityMappingConfiguration<? extends C, I>> getSubClasses() {
			return Iterables.collect(subClasses, Duo::getLeft, HashSet::new);
		}
		
		@Nullable
		public Table giveTable(SubEntityMappingConfiguration key) {
			return Iterables.find(subClasses, duo -> duo.getLeft().equals(key)).getRight();
		}
	}
	
	class SingleTablePolymorphism<C, I, D> implements PolymorphismPolicy {
		
		private final String discriminatorColumn;
		
		private final Class<D> discriminatorType;
		
		private final Map<D, SubEntityMappingConfiguration<? extends C, I>> subClasses = new HashMap<>();
		
		public SingleTablePolymorphism(String discriminatorColumn, Class<D> discriminatorType) {
			this.discriminatorColumn = discriminatorColumn;
			this.discriminatorType = discriminatorType;
		}
		
		public String getDiscriminatorColumn() {
			return discriminatorColumn;
		}
		
		public Class<D> getDiscrimintorType() {
			return discriminatorType;
		}
		
		// Please note that we accept sub-entity mapping configuration with <Object> as identifier whereas we expect I, this is only done for
		// fluent write reason and compatibility with MappingEase.subentityBuilder(..) method result signature
		public <E extends C> SingleTablePolymorphism<C, I, D> addSubClass(SubEntityMappingConfiguration<E, Object> entityMappingConfiguration, D discriminatorValue) {
			subClasses.put(discriminatorValue, (SubEntityMappingConfiguration<? extends C, I>) entityMappingConfiguration);
			return this;
		}
		
		public Class<? extends C> getClass(D discriminatorValue) {
			return subClasses.get(discriminatorValue).getEntityType();
		}
		
		public D getDiscriminatorValue(Class<? extends C> instanceType) {
			return Iterables.find(subClasses.entrySet(), e -> e.getValue().getEntityType().equals(instanceType)).getKey();
		}
		
		public Set<SubEntityMappingConfiguration<? extends C, I>> getSubClasses() {
			return new HashSet<>(this.subClasses.values());
		}
	}
}
