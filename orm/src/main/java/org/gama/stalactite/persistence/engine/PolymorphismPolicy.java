package org.gama.stalactite.persistence.engine;

import javax.annotation.Nullable;
import java.util.Collection;
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
public abstract class PolymorphismPolicy {
	
	public static <C, I> TablePerClassPolymorphism<C, I> tablePerClass() {
		return new TablePerClassPolymorphism<>();
	}
	
	public static <C, I> JoinedTablesPolymorphism<C, I> joinedTables() {
		return new JoinedTablesPolymorphism<>();
	}
	
	/**
	 * Creates a single-table polymorphism configuration with a default discriminating column names "DTYPE" of {@link String} type
	 * @param <C> entity type
	 * @param <I> identifier type
	 * @return a new {@link SingleTablePolymorphism} with "DTYPE" as String discriminator column
	 */
	public static <C, I> SingleTablePolymorphism<C, I, String> singleTable() {
		return singleTable("DTYPE");
	}
	
	public static <C, I> SingleTablePolymorphism<C, I, String> singleTable(String discriminatorColumnName) {
		return new SingleTablePolymorphism<>(discriminatorColumnName, String.class);
	}
	
	public static class TablePerClassPolymorphism<C, I> extends PolymorphismPolicy {
		
		private final Set<Duo<EntityMappingConfigurationProvider<? extends C, I>, Table /* Nullable */>> subClasses = new HashSet<>();
		
		public TablePerClassPolymorphism<C, I> addSubClass(EntityMappingConfigurationProvider<? extends C, I> entityMappingConfigurationProvider) {
			addSubClass(entityMappingConfigurationProvider, null);
			return this;
		}
		
		public TablePerClassPolymorphism<C, I> addSubClass(EntityMappingConfigurationProvider<? extends C, I> entityMappingConfigurationProvider, @Nullable Table table) {
			subClasses.add(new Duo<>(entityMappingConfigurationProvider, table));
			return this;
		}
		
		public Set<EntityMappingConfigurationProvider<? extends C, I>> getSubClasses() {
			return Iterables.collect(subClasses, Duo::getLeft, HashSet::new);
		}
		
		@Nullable
		public Table giveTable(EntityMappingConfigurationProvider<? extends C, I> key) {
			return Iterables.find(subClasses, duo -> duo.getLeft().equals(key)).getRight();
		}
	}
	
	public static class JoinedTablesPolymorphism<C, I> extends PolymorphismPolicy {
		
		private final Set<Duo<EntityMappingConfigurationProvider<? extends C, I>, Table /* Nullable */>> subClasses = new HashSet<>();
		
		public JoinedTablesPolymorphism<C, I> addSubClass(EntityMappingConfigurationProvider<? extends C, I> entityMappingConfigurationProvider) {
			addSubClass(entityMappingConfigurationProvider, null);
			return this;
		}
		
		public JoinedTablesPolymorphism<C, I> addSubClass(EntityMappingConfigurationProvider<? extends C, I> entityMappingConfigurationProvider, @Nullable Table table) {
			subClasses.add(new Duo<>(entityMappingConfigurationProvider, table));
			return this;
		}
		
		public Set<EntityMappingConfigurationProvider<? extends C, I>> getSubClasses() {
			return Iterables.collect(subClasses, Duo::getLeft, HashSet::new);
		}
		
		@Nullable
		public Table giveTable(EntityMappingConfigurationProvider<? extends C, I> key) {
			return Iterables.find(subClasses, duo -> duo.getLeft().equals(key)).getRight();
		}
	}
	
	public static class SingleTablePolymorphism<C, I, D> extends PolymorphismPolicy {
		
		private final String discriminatorColumn;
		
		private final Class<D> discriminatorType;
		
		private final Map<D, EntityMappingConfigurationProvider<? extends C, I>> subClasses = new HashMap<>();
		
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
		
		public <E extends C> SingleTablePolymorphism<C, I, D> addSubClass(EntityMappingConfigurationProvider<E, I> entityMappingConfigurationProvider, D discriminatorValue) {
			subClasses.put(discriminatorValue, entityMappingConfigurationProvider);
			return this;
		}
		
		public Class<? extends C> getClass(D discriminatorValue) {
			return subClasses.get(discriminatorValue).getConfiguration().getPersistedClass();
		}
		
		public D getDiscriminatorValue(Class<? extends C> instanceType) {
			return Iterables.find(subClasses.entrySet(), e -> e.getValue().getConfiguration().getPersistedClass().equals(instanceType)).getKey();
		}
		
		public Collection<EntityMappingConfigurationProvider<? extends C, I>> getSubClasses() {
			return this.subClasses.values();
		}
	}
}
