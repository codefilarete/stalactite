package org.codefilarete.stalactite.dsl;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.dsl.subentity.SubEntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.subentity.SubEntityMappingConfigurationProvider;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * @author Guillaume Mary
 */
public interface PolymorphismPolicy<C> {
	
	/**
	 * Starts a persistence configuration of a table-per-class polymorphic type.
	 * As a difference with {@link #tablePerClass(Class)}, this method doesn't require the polymorphic type as argument,
	 * though you should prefix its call by the polymorphic type as a generic, for instance : 
	 * <code>
	 *     &lt;Vehicle&gt;tablePerClass()
	 * </code>
	 *
	 * @return the configuration
	 */
	static <C> TablePerClassPolymorphism<C> tablePerClass() {
		return new TablePerClassPolymorphism<>();
	}
	
	/**
	 * Starts a persistence configuration of a table-per-class polymorphic type 
	 * 
	 * @param polymorphicType type for which polymorphism is declared
	 * @return the configuration
	 * @param <C> polymorphic type
	 */
	static <C> TablePerClassPolymorphism<C> tablePerClass(Class<C> polymorphicType) {
		return new TablePerClassPolymorphism<>();
	}
	
	/**
	 * Starts a persistence configuration of a join-table polymorphic type.
	 * As a difference with {@link #joinTable(Class)}, this method doesn't require the polymorphic type as argument,
	 * though you should prefix its call by the polymorphic type as a generic, for instance : 
	 * <code>
	 *     &lt;Vehicle&gt;joinTable()
	 * </code>
	 *
	 * @return the configuration
	 */
	static <C> JoinTablePolymorphism<C> joinTable() {
		return new JoinTablePolymorphism<>();
	}
	
	/**
	 * Starts a persistence configuration of a join-table polymorphic type 
	 *
	 * @param polymorphicType type for which polymorphism is declared
	 * @return the configuration
	 * @param <C> polymorphic type
	 */
	static <C> JoinTablePolymorphism<C> joinTable(Class<C> polymorphicType) {
		return new JoinTablePolymorphism<>();
	}
	
	/**
	 * Starts a persistence configuration of a single-table polymorphic type with a default discriminating column name "DTYPE" of {@link String} type
	 * As a difference with {@link #singleTable(Class)}, this method doesn't require the polymorphic type as argument,
	 * though you should prefix its call by the polymorphic type as a generic, for instance : 
	 * <code>
	 *     &lt;Vehicle&gt;singleTable()
	 * </code>
	 * 
	 * @param <C> entity type
	 * @return a new {@link SingleTablePolymorphism} with "DTYPE" as String discriminator column
	 */
	static <C> SingleTablePolymorphism<C, String> singleTable() {
		return singleTable("DTYPE");
	}
	
	/**
	 * Starts a persistence configuration of a single-table polymorphic type with the give discriminating column name {@link String} type
	 * As a difference with {@link #singleTable(Class)}, this method doesn't require the polymorphic type as argument,
	 * though you should prefix its call by the polymorphic type as a generic, for instance : 
	 * <code>
	 *     &lt;Vehicle&gt;singleTable()
	 * </code>
	 *
	 * @param <C> entity type
	 * @return a new {@link SingleTablePolymorphism} with "DTYPE" as String discriminator column
	 */
	static <C> SingleTablePolymorphism<C, String> singleTable(String discriminatorColumnName) {
		return new SingleTablePolymorphism<>(discriminatorColumnName, String.class);
	}
	
	/**
	 * Starts a persistence configuration of a single-table polymorphic type with a default discriminating column name "DTYPE" of {@link String} type
	 *
	 * @param polymorphicType type for which polymorphism is declared
	 * @return the configuration
	 * @param <C> polymorphic type
	 */
	static <C> SingleTablePolymorphism<C, String> singleTable(Class<C> polymorphicType) {
		return singleTable("DTYPE");
	}
	
	/**
	 * Starts a persistence configuration of a single-table polymorphic type with the give discriminating column name {@link String} type
	 *
	 * @param polymorphicType type for which polymorphism is declared
	 * @return the configuration
	 * @param <C> polymorphic type
	 */
	static <C> SingleTablePolymorphism<C, String> singleTable(Class<C> polymorphicType, String discriminatorColumnName) {
		return new SingleTablePolymorphism<>(discriminatorColumnName, String.class);
	}
	
	Set<SubEntityMappingConfiguration<? extends C>> getSubClasses();
	
	class TablePerClassPolymorphism<C> implements PolymorphismPolicy<C> {
		
		// we use a KeepOrderSet for stability order (overall for test assertions), not a strong expectation
		private final Set<Duo<SubEntityMappingConfiguration<? extends C>, Table /* Nullable */>> subClasses = new KeepOrderSet<>();
		
		/**
		 * Registers a sub-entity type mapping to current polymorphic type
		 * 
		 * @param entityMappingConfigurationProvider the sub-entity type mapping to register
		 * @return this
		 * @see MappingEase#subentityBuilder(Class)
		 */
		public TablePerClassPolymorphism<C> addSubClass(SubEntityMappingConfigurationProvider<? extends C> entityMappingConfigurationProvider) {
			addSubClass(entityMappingConfigurationProvider, (Table) null);
			return this;
		}
		
		/**
		 * Registers a sub-entity type mapping to current polymorphic type
		 *
		 * @param entityMappingConfiguration the sub-entity type mapping to register
		 * @return this
		 * @see MappingEase#subentityBuilder(Class)
		 */
		public TablePerClassPolymorphism<C> addSubClass(SubEntityMappingConfiguration<? extends C> entityMappingConfiguration) {
			addSubClass(entityMappingConfiguration, (Table) null);
			return this;
		}
		
		/**
		 * Registers a sub-entity type mapping to current polymorphic type
		 *
		 * @param entityMappingConfigurationProvider the sub-entity type mapping to register
		 * @param tableName the table name to store the sub-entity type
		 * @return this
		 * @see MappingEase#subentityBuilder(Class)
		 */
		public TablePerClassPolymorphism<C> addSubClass(SubEntityMappingConfigurationProvider<? extends C> entityMappingConfigurationProvider, @Nullable String tableName) {
			addSubClass(entityMappingConfigurationProvider.getConfiguration(), tableName);
			return this;
		}
		
		/**
		 * Registers a sub-entity type mapping to current polymorphic type
		 *
		 * @param entityMappingConfiguration the sub-entity type mapping to register
		 * @param tableName the table name to store the sub-entity type
		 * @return this
		 * @see MappingEase#subentityBuilder(Class)
		 */
		public TablePerClassPolymorphism<C> addSubClass(SubEntityMappingConfiguration<? extends C> entityMappingConfiguration, @Nullable String tableName) {
			addSubClass(entityMappingConfiguration, nullable(tableName).map(Table::new).get());
			return this;
		}
		
		/**
		 * Registers a sub-entity type mapping to current polymorphic type
		 *
		 * @param entityMappingConfigurationProvider the sub-entity type mapping to register
		 * @param table the table to store the sub-entity type
		 * @return this
		 * @see MappingEase#subentityBuilder(Class)
		 */
		public TablePerClassPolymorphism<C> addSubClass(SubEntityMappingConfigurationProvider<? extends C> entityMappingConfigurationProvider, @Nullable Table table) {
			addSubClass(entityMappingConfigurationProvider.getConfiguration(), table);
			return this;
		}
		
		/**
		 * Registers a sub-entity type mapping to current polymorphic type
		 *
		 * @param entityMappingConfiguration the sub-entity type mapping to register
		 * @param table the table to store the sub-entity type
		 * @return this
		 * @see MappingEase#subentityBuilder(Class)
		 */
		public TablePerClassPolymorphism<C> addSubClass(SubEntityMappingConfiguration<? extends C> entityMappingConfiguration, @Nullable Table table) {
			subClasses.add(new Duo<>(entityMappingConfiguration, table));
			return this;
		}
		
		@Override
		public Set<SubEntityMappingConfiguration<? extends C>> getSubClasses() {
			// we use a KeepOrderSet for stability order (overall for test assertions), not a strong expectation
			return Iterables.collect(subClasses, Duo::getLeft, KeepOrderSet::new);
		}
		
		@Nullable
		public Table giveTable(SubEntityMappingConfiguration<? extends C> key) {
			return Iterables.find(subClasses, duo -> duo.getLeft().equals(key)).getRight();
		}
	}
	
	class JoinTablePolymorphism<C> implements PolymorphismPolicy<C> {
		
		// we use a KeepOrderSet for stability order (overall for test assertions), not a strong expectation
		private final Set<Duo<SubEntityMappingConfiguration<? extends C>, Table /* Nullable */>> subClasses = new KeepOrderSet<>();
		
		/**
		 * Registers a sub-entity type mapping to current polymorphic type
		 *
		 * @param entityMappingConfigurationProvider the sub-entity type mapping to register
		 * @return this
		 * @see MappingEase#subentityBuilder(Class)
		 */
		public JoinTablePolymorphism<C> addSubClass(SubEntityMappingConfigurationProvider<? extends C> entityMappingConfigurationProvider) {
			addSubClass(entityMappingConfigurationProvider, (Table) null);
			return this;
		}		
		
		/**
		 * Registers a sub-entity type mapping to current polymorphic type
		 *
		 * @param entityMappingConfiguration the sub-entity type mapping to register
		 * @return this
		 * @see MappingEase#subentityBuilder(Class)
		 */
		public JoinTablePolymorphism<C> addSubClass(SubEntityMappingConfiguration<? extends C> entityMappingConfiguration) {
			addSubClass(entityMappingConfiguration, (Table) null);
			return this;
		}
		
		/**
		 * Registers a sub-entity type mapping to current polymorphic type
		 *
		 * @param entityMappingConfigurationProvider the sub-entity type mapping to register
		 * @param tableName the table to store the sub-entity type
		 * @return this
		 * @see MappingEase#subentityBuilder(Class)
		 */
		public JoinTablePolymorphism<C> addSubClass(SubEntityMappingConfigurationProvider<? extends C> entityMappingConfigurationProvider, @Nullable String tableName) {
			return addSubClass(entityMappingConfigurationProvider.getConfiguration(), tableName);
		}
		
		/**
		 * Registers a sub-entity type mapping to current polymorphic type
		 *
		 * @param entityMappingConfiguration the sub-entity type mapping to register
		 * @param tableName the table to store the sub-entity type
		 * @return this
		 * @see MappingEase#subentityBuilder(Class)
		 */
		public JoinTablePolymorphism<C> addSubClass(SubEntityMappingConfiguration<? extends C> entityMappingConfiguration, @Nullable String tableName) {
			addSubClass(entityMappingConfiguration, nullable(tableName).map(Table::new).get());
			return this;
		}
		
		/**
		 * Registers a sub-entity type mapping to current polymorphic type
		 *
		 * @param entityMappingConfigurationProvider the sub-entity type mapping to register
		 * @param table the table to store the sub-entity type
		 * @return this
		 * @see MappingEase#subentityBuilder(Class)
		 */
		public JoinTablePolymorphism<C> addSubClass(SubEntityMappingConfigurationProvider<? extends C> entityMappingConfigurationProvider, @Nullable Table table) {
			return addSubClass(entityMappingConfigurationProvider.getConfiguration(), table);
		}
		
		/**
		 * Registers a sub-entity type mapping to current polymorphic type
		 *
		 * @param entityMappingConfiguration the sub-entity type mapping to register
		 * @param table the table to store the sub-entity type
		 * @return this
		 * @see MappingEase#subentityBuilder(Class)
		 */
		public JoinTablePolymorphism<C> addSubClass(SubEntityMappingConfiguration<? extends C> entityMappingConfiguration, @Nullable Table table) {
			subClasses.add(new Duo<>(entityMappingConfiguration, table));
			return this;
		}
		
		@Override
		public Set<SubEntityMappingConfiguration<? extends C>> getSubClasses() {
			// we use a KeepOrderSet for stability order (overall for test assertions), not a strong expectation
			return Iterables.collect(subClasses, Duo::getLeft, KeepOrderSet::new);
		}
		
		@Nullable
		public Table giveTable(SubEntityMappingConfiguration key) {
			return Iterables.find(subClasses, duo -> duo.getLeft().equals(key)).getRight();
		}
	}
	
	class SingleTablePolymorphism<C, D> implements PolymorphismPolicy<C> {
		
		private final String discriminatorColumn;
		
		private final Class<D> discriminatorType;
		
		// we use a KeepOrderMap for stability order (overall for test assertions), not a strong expectation
		private final Map<D, SubEntityMappingConfiguration<? extends C>> subClasses = new KeepOrderMap<>();
		
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
		
		/**
		 * Registers a sub-entity type mapping to current polymorphic type
		 *
		 * @param entityMappingConfiguration the sub-entity type mapping to register
		 * @param discriminatorValue sub-entity discriminator value (will be used to distinguish entity type in while loading them from database)
		 * @return this
		 * @see MappingEase#subentityBuilder(Class)
		 */
		public SingleTablePolymorphism<C, D> addSubClass(SubEntityMappingConfigurationProvider<? extends C> entityMappingConfiguration, D discriminatorValue) {
			return addSubClass(entityMappingConfiguration.getConfiguration(), discriminatorValue);
		}
		
		/**
		 * Registers a sub-entity type mapping to current polymorphic type
		 *
		 * @param entityMappingConfiguration the sub-entity type mapping to register
		 * @param discriminatorValue sub-entity discriminator value (will be used to distinguish entity type in while loading them from database)
		 * @return this
		 * @see MappingEase#subentityBuilder(Class)
		 */
		public SingleTablePolymorphism<C, D> addSubClass(SubEntityMappingConfiguration<? extends C> entityMappingConfiguration, D discriminatorValue) {
			subClasses.put(discriminatorValue, entityMappingConfiguration);
			return this;
		}
		
		public Class<? extends C> getClass(D discriminatorValue) {
			return subClasses.get(discriminatorValue).getEntityType();
		}
		
		public D getDiscriminatorValue(Class<? extends C> instanceType) {
			return Iterables.find(subClasses.entrySet(), e -> e.getValue().getEntityType().isAssignableFrom(instanceType)).getKey();
		}
		
		@Override
		public Set<SubEntityMappingConfiguration<? extends C>> getSubClasses() {
			// we use a KeepOrderSet for stability order (overall for test assertions), not a strong expectation
			return new KeepOrderSet<>(this.subClasses.values());
		}
	}
}
