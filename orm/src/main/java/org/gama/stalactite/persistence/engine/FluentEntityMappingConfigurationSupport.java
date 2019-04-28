package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Duo;
import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.exception.NotImplementedException;
import org.gama.lang.function.Serie;
import org.gama.lang.reflect.MethodDispatcher;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.Accessors;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MemberDefinition;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.PropertyAccessor;
import org.gama.reflection.ValueAccessPoint;
import org.gama.reflection.ValueAccessPointMap;
import org.gama.stalactite.persistence.engine.AbstractVersioningStrategy.VersioningStrategySupport;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfiguration.Linkage;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.AbstractLinkage;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.Inset;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.LinkageByColumnName;
import org.gama.stalactite.persistence.engine.IFluentEmbeddableMappingBuilder.IFluentEmbeddableMappingBuilderEmbedOptions;
import org.gama.stalactite.persistence.engine.IFluentEmbeddableMappingBuilder.IFluentEmbeddableMappingBuilderEnumOptions;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.builder.CascadeManyList;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.assembly.SimpleIdentifierAssembler;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdAccessor;
import org.gama.stalactite.persistence.mapping.SinglePropertyIdAccessor;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.collection.Iterables.collect;

/**
 * @author Guillaume Mary
 */
public class FluentEntityMappingConfigurationSupport<C, I> implements IFluentMappingBuilder<C, I>, EntityMappingConfiguration<C, I> {
	
	/**
	 * Will start a {@link FluentEntityMappingConfigurationSupport} for a given class which will target a table that as the class name.
	 *
	 * @param persistedClass the class to be persisted by the {@link Persister} that will be created by {@link #build(PersistenceContext)}
	 * @param identifierClass the class of the identifier
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link IFluentMappingBuilder}
	 */
	public static <T, I> IFluentMappingBuilder<T, I> from(Class<T> persistedClass, Class<I> identifierClass) {
		return new FluentEntityMappingConfigurationSupport<>(persistedClass);
	}
	
	private final Class<C> persistedClass;
	
	private IdentifierInsertionManager<C, I> identifierInsertionManager;
	
	PropertyAccessor<C, I> identifierAccessor;
	
	private final MethodReferenceCapturer methodSpy;
	
	private final List<CascadeOne<C, ?, ?>> cascadeOnes = new ArrayList<>();
	
	private final List<CascadeMany<C, ?, ?, ? extends Collection>> cascadeManys = new ArrayList<>();
	
	private final EntityDecoratedEmbeddableConfigurationSupport<C, I> propertiesMappingConfigurationSurrogate;
	
	private ForeignKeyNamingStrategy foreignKeyNamingStrategy = ForeignKeyNamingStrategy.DEFAULT;
	
	private ColumnNamingStrategy joinColumnNamingStrategy = ColumnNamingStrategy.JOIN_DEFAULT;
	
	private AssociationTableNamingStrategy associationTableNamingStrategy = AssociationTableNamingStrategy.DEFAULT;
	
	private OptimisticLockOption optimisticLockOption;
	
	private EntityMappingConfiguration<? super C, I> inheritanceConfiguration;
	
	private boolean joinTable = false;
	
	private Table targetParentTable;
	
	/**
	 * Creates a builder to map the given class for persistence
	 *
	 * @param persistedClass the class to create a mapping for
	 */
	public FluentEntityMappingConfigurationSupport(Class<C> persistedClass) {
		this.persistedClass = persistedClass;
		
		// Helper to capture Method behind method reference
		this.methodSpy = new MethodReferenceCapturer();
		
		this.propertiesMappingConfigurationSurrogate = new EntityDecoratedEmbeddableConfigurationSupport<>(this, persistedClass);
	}
	
	@Override
	public Class<C> getPersistedClass() {
		return persistedClass;
	}
	
	public ColumnNamingStrategy getJoinColumnNamingStrategy() {
		return joinColumnNamingStrategy;
	}
	
	private Method captureLambdaMethod(SerializableFunction getter) {
		return this.methodSpy.findMethod(getter);
	}
	
	private Method captureLambdaMethod(SerializableBiConsumer setter) {
		return this.methodSpy.findMethod(setter);
	}
	
	@Override
	public IReversibleAccessor getIdentifierAccessor() {
		return this.identifierAccessor;
	}
	
	@Override
	public IdentifierInsertionManager<C, I> getIdentifierInsertionManager() {
		return this.identifierInsertionManager;
	}
	
	@Override
	public EmbeddableMappingConfiguration<C> getPropertiesMapping() {
		return propertiesMappingConfigurationSurrogate;
	}
	
	@Override
	public VersioningStrategy getOptimisticLockOption() {
		return Nullable.nullable(this.optimisticLockOption).map(OptimisticLockOption::getVersioningStrategy).get();
	}
	
	@Override
	public List<CascadeOne<C, ?, ?>> getOneToOnes() {
		return cascadeOnes;
	}
	
	@Override
	public List<CascadeMany<C, ?, ?, ? extends Collection>> getOneToManys() {
		return cascadeManys;
	}
	
	@Override
	public EntityMappingConfiguration<? super C, I> getInheritanceConfiguration() {
		return inheritanceConfiguration;
	}
	
	@Override
	public boolean isJoinTable() {
		return this.joinTable;
	}
	
	@Override
	public Table getInheritanceTable() {
		return targetParentTable;
	}
	
	@Override
	public ForeignKeyNamingStrategy getForeignKeyNamingStrategy() {
		return this.foreignKeyNamingStrategy;
	}
	
	@Override
	public AssociationTableNamingStrategy getAssociationTableNamingStrategy() {
		return this.associationTableNamingStrategy;
	}
	
	@Override
	public EntityMappingConfiguration<C, I> getConfiguration() {
		return this;
	}
	
	@Override
	public <O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableBiConsumer<C, O> setter) {
		Method method = captureLambdaMethod(setter);
		return add(method, (String) null);
	}
	
	@Override
	public <O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableFunction<C, O> getter) {
		Method method = captureLambdaMethod(getter);
		return add(method, (String) null);
	}
	
	@Override
	public <O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableBiConsumer<C, O> setter, String columnName) {
		Method method = captureLambdaMethod(setter);
		return add(method, columnName);
	}
	
	@Override
	public <O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableFunction<C, O> getter, String columnName) {
		Method method = captureLambdaMethod(getter);
		return add(method, columnName);
	}
	
	@Override
	public <O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableFunction<C, O> getter, Column<Table, O> column) {
		Method method = captureLambdaMethod(getter);
		return add(method, column);
	}
	
	private <O> IFluentMappingBuilderColumnOptions<C, I> add(Method method, Column<Table, O> column) {
		Linkage<C> mapping = propertiesMappingConfigurationSurrogate.addMapping(method, column);
		return this.propertiesMappingConfigurationSurrogate.applyAdditionalOptions(method, mapping);
	}
	
	private IFluentMappingBuilderColumnOptions<C, I> add(Method method, @javax.annotation.Nullable String columnName) {
		Linkage<C> mapping = propertiesMappingConfigurationSurrogate.addMapping(method, columnName);
		return this.propertiesMappingConfigurationSurrogate.applyAdditionalOptions(method, mapping);
	}
	
	@Override
	public <E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter) {
		Method method = captureLambdaMethod(setter);
		return addEnum(method, null);
	}
	
	@Override
	public <E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter) {
		Method method = captureLambdaMethod(getter);
		return addEnum(method, null);
	}
	
	@Override
	public <E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter, String columnName) {
		Method method = captureLambdaMethod(setter);
		return addEnum(method, columnName);
	}
	
	@Override
	public <E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter, String columnName) {
		Method method = captureLambdaMethod(getter);
		return addEnum(method, columnName);
	}
	
	@Override
	public <E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter, Column<Table, E> column) {
		Method method = captureLambdaMethod(getter);
		AbstractLinkage<C> linkage = propertiesMappingConfigurationSurrogate.addMapping(method, column);
		IFluentEmbeddableMappingBuilderEnumOptions<C> enumOptionsHandler = propertiesMappingConfigurationSurrogate.addEnumOptions(linkage);
		return new MethodDispatcher()
				.redirect(EnumOptions.class, enumOptionsHandler, true)
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderEnumOptions<C, I>>) (Class) IFluentMappingBuilderEnumOptions.class);
	}
	
	private IFluentMappingBuilderEnumOptions<C, I> addEnum(Method method, @javax.annotation.Nullable String columnName) {
		AbstractLinkage<C> linkage = propertiesMappingConfigurationSurrogate.addMapping(method, columnName);
		IFluentEmbeddableMappingBuilderEnumOptions<C> enumOptionsHandler = propertiesMappingConfigurationSurrogate.addEnumOptions(linkage);
		// we redirect all of the EnumOptions method to the instance that can handle them, returning the dispatcher on this methods so one can chain
		// with some other methods, other methods are redirected to this instance because it can handle them.
		return new MethodDispatcher()
				.redirect(EnumOptions.class, enumOptionsHandler, true)
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderEnumOptions<C, I>>) (Class) IFluentMappingBuilderEnumOptions.class);
	}
	
	@Override
	public IFluentMappingBuilder<C, I> mapInheritance(EntityMappingConfiguration<? super C, I> mappingConfiguration) {
		inheritanceConfiguration = mappingConfiguration;
		return this;
	}
	
	@Override
	public IFluentMappingBuilder<C, I> mapSuperClass(EmbeddableMappingConfiguration<? super C> superMappingConfiguration) {
		this.propertiesMappingConfigurationSurrogate.mapSuperClass(superMappingConfiguration);
		return this;
	}
	
	@Override
	public <O, J> IFluentMappingBuilderOneToOneOptions<C, I> addOneToOne(SerializableFunction<C, O> getter, Persister<O, J, ? extends Table> persister) {
		// we declare the column on our side: we do it first because it checks some rules
		add(getter);
		// we keep it
		CascadeOne<C, O, J> cascadeOne = new CascadeOne<>(getter, persister, captureLambdaMethod(getter));
		this.cascadeOnes.add(cascadeOne);
		// then we return an object that allows fluent settings over our OneToOne cascade instance
		return new MethodDispatcher()
				.redirect(OneToOneOptions.class, new OneToOneOptions() {
					@Override
					public IFluentMappingBuilderOneToOneOptions cascading(RelationshipMode relationshipMode) {
						cascadeOne.setRelationshipMode(relationshipMode);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public IFluentMappingBuilderOneToOneOptions mandatory() {
						cascadeOne.setNullable(false);
						return null;	// we can return null because dispatcher will return proxy
					}
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderOneToOneOptions<C, I>>) (Class) IFluentMappingBuilderOneToOneOptions.class);
	}
	
	@Override
	public <O, J, S extends Set<O>> IFluentMappingBuilderOneToManyOptions<C, I, O> addOneToManySet(
			SerializableFunction<C, S> getter, Persister<O, J, ? extends Table> persister) {
		CascadeMany<C, O, J, S> cascadeMany = new CascadeMany<>(getter, persister, captureLambdaMethod(getter));
		this.cascadeManys.add(cascadeMany);
		return new MethodDispatcher()
				.redirect(OneToManyOptions.class, new OneToManyOptionsSupport<>(cascadeMany), true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderOneToManyOptions<C, I, O>>) (Class) IFluentMappingBuilderOneToManyOptions.class);
	}
	
	@Override
	public <O, J, S extends List<O>> IFluentMappingBuilderOneToManyListOptions<C, I, O> addOneToManyList(
			SerializableFunction<C, S> getter, Persister<O, J, ? extends Table> persister) {
		CascadeManyList<C, O, J, ? extends List<O>> cascadeMany = new CascadeManyList<>(getter, persister, captureLambdaMethod(getter));
		this.cascadeManys.add(cascadeMany);
		return new MethodDispatcher()
				.redirect(OneToManyOptions.class, new OneToManyOptionsSupport<>(cascadeMany), true)	// true to allow "return null" in implemented methods
				.redirect(IndexableCollectionOptions.class, new IndexableCollectionOptions<C, I, O>() {
					@Override
					public <T extends Table> IndexableCollectionOptions<C, I, O> indexedBy(Column<T, Integer> orderingColumn) {
						cascadeMany.setIndexingColumn(orderingColumn);
						return null;
					}
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderOneToManyListOptions<C, I, O>>) (Class) IFluentMappingBuilderOneToManyListOptions.class);
	}
	
	@Override
	public <O> IFluentMappingBuilderEmbedOptions<C, I, O> embed(SerializableBiConsumer<C, O> setter) {
		return embed(propertiesMappingConfigurationSurrogate.embed(setter));
	}
	
	@Override
	public <O> IFluentMappingBuilderEmbedOptions<C, I, O> embed(SerializableFunction<C, O> getter) {
		return embed(propertiesMappingConfigurationSurrogate.embed(getter));
	}
	
	@Override
	public <O> IFluentMappingBuilderEmbeddableOptions<C, I, O> embed(SerializableFunction<C, O> getter,
																				EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder) {
		return null;
	}
	
	@Override
	public <O> IFluentMappingBuilderEmbeddableOptions<C, I, O> embed(SerializableBiConsumer<C, O> getter,
																				EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder) {
		return null;
	}
	
	private <O> IFluentMappingBuilderEmbedOptions<C, I, O> embed(IFluentEmbeddableMappingBuilderEmbedOptions<C, O> embedSupport) {
		return new MethodDispatcher()
				.redirect(EmbedWithColumnOptions.class, new EmbedWithColumnOptions() {
					
					@Override
					public EmbedOptions innerEmbed(SerializableBiConsumer setter) {
						embedSupport.innerEmbed(setter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedOptions innerEmbed(SerializableFunction getter) {
						embedSupport.innerEmbed(getter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedOptions overrideName(SerializableFunction function, String columnName) {
						embedSupport.overrideName(function, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedWithColumnOptions exclude(SerializableBiConsumer setter) {
						embedSupport.exclude(setter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedWithColumnOptions exclude(SerializableFunction getter) {
						embedSupport.exclude(getter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedWithColumnOptions override(SerializableFunction function, Column targetColumn) {
						propertiesMappingConfigurationSurrogate.currentInset().override(function, targetColumn);
						return null;	// we can return null because dispatcher will return proxy
					}
				}, true)
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderEmbedOptions<C, I, O>>) (Class) IFluentMappingBuilderEmbedOptions.class);
	}
	
	@Override
	public IFluentMappingBuilder<C, I> foreignKeyNamingStrategy(ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		return this;
	}
	
	@Override
	public IFluentMappingBuilder<C, I> columnNamingStrategy(ColumnNamingStrategy columnNamingStrategy) {
		this.propertiesMappingConfigurationSurrogate.columnNamingStrategy(columnNamingStrategy);
		return this;
	}
	
	@Override
	public IFluentMappingBuilder<C, I> joinColumnNamingStrategy(ColumnNamingStrategy columnNamingStrategy) {
		this.joinColumnNamingStrategy = columnNamingStrategy;
		return this;
	}
	
	/**
	 * Defines the versioning property of beans. This implies that Optmistic Locking will be applied on those beans.
	 * Versioning policy is supported for following types:
	 * <ul>
	 * <li>{@link Integer} : a "+1" policy will be applied, see {@link Serie#INTEGER_SERIE}</li>
	 * <li>{@link Long} : a "+1" policy will be applied, see {@link Serie#LONG_SERIE}</li>
	 * <li>{@link Date} : a "now" policy will be applied, see {@link Serie#NOW_SERIE}</li>
	 * </ul>
	 * 
	 * @param getter the funciton that gives access to the versioning property
	 * @param <V> type of the versioning property, determines versioning policy
	 * @return this
	 * @see #versionedBy(SerializableFunction, Serie)
	 */
	@Override
	public <V> IFluentMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter) {
		Method method = captureLambdaMethod(getter);
		Serie<V> serie;
		if (Integer.class.isAssignableFrom(method.getReturnType())) {
			serie = (Serie<V>) Serie.INTEGER_SERIE;
		} else if (Long.class.isAssignableFrom(method.getReturnType())) {
			serie = (Serie<V>) Serie.LONG_SERIE;
		} else if (Date.class.isAssignableFrom(method.getReturnType())) {
			serie = (Serie<V>) Serie.NOW_SERIE;
		} else {
			throw new NotImplementedException("Type of versioned property is not implemented, please provide a "
					+ Serie.class.getSimpleName() + " for it : " + Reflections.toString(method.getReturnType()));
		}
		return versionedBy(getter, method, serie);
	}
	
	@Override
	public <V> IFluentMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter, Serie<V> serie) {
		return versionedBy(getter, captureLambdaMethod(getter), serie);
	}
	
	public <V> IFluentMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter, Method method, Serie<V> serie) {
		optimisticLockOption = new OptimisticLockOption<>(Accessors.of(method), serie);
		add(getter);
		return this;
	}
	
	@Override
	public IFluentMappingBuilder<C, I> withJoinTable() {
		this.joinTable = true;
		return this;
	}
	
	@Override
	public IFluentMappingBuilder<C, I> withJoinTable(Table parentTable) {
		withJoinTable();
		this.targetParentTable = parentTable;
		return this;
	}
	
	@Override
	public Persister<C, I, Table> build(PersistenceContext persistenceContext) {
		return build(persistenceContext, new Table(getPersistedClass().getSimpleName()));
	}
	
	@Override
	public <T extends Table> Persister<C, I, T> build(PersistenceContext persistenceContext, T targetTable) {
		if (inheritanceConfiguration != null && propertiesMappingConfigurationSurrogate.getMappedSuperClassConfiguration() != null) {
			throw new MappingConfigurationException("Mapped super class and inheritance are not supported when they are combined, please remove one of them");
		}
		
		if (inheritanceConfiguration != null && isJoinTable()) {
			return new JoinedTablesEntityMappingBuilder<>(this, methodSpy)
					.build(persistenceContext, targetTable);
		} else {
			return new EntityMappingBuilder<>(this, methodSpy)
					.build(persistenceContext, targetTable);
		}
	}
	
	/**
	 * Specialized version of {@link Linkage} for entity use case
	 * 
	 * @param <T>
	 */
	private interface EntityLinkage<T> extends Linkage<T> {
		
		boolean isPrimaryKey();
	}
	
	private static class EntityLinkageByColumnName<T>  extends LinkageByColumnName<T> implements EntityLinkage<T> {
		
		private boolean primaryKey;
		
		/**
		 * Constructor by {@link Method}. Only accessor by method is implemented (since input is from method reference).
		 * (Doing it for field accessor is simple work but not necessary)
		 * 
		 * @param method a {@link PropertyAccessor}
		 * @param columnName an override of the default name that will be generated
		 */
		private EntityLinkageByColumnName(Method method, String columnName) {
			super(method, columnName);
		}
		
		public boolean isPrimaryKey() {
			return primaryKey;
		}
		
		public void primaryKey() {
			this.primaryKey = true;
		}
	}
	
	private static class EntityLinkageByColumn<T> extends AbstractLinkage<T> implements EntityLinkage<T> {
		
		private final PropertyAccessor function;
		private final Column column;
		
		/**
		 * Constructor by {@link Method}. Only accessor by method is implemented (since input is from method reference).
		 * (Doing it for field accessor is simple work but not necessary)
		 * @param method a {@link PropertyAccessor}
		 * @param column an override of the default column that would have been generated
		 */
		private EntityLinkageByColumn(Method method, Column column) {
			this.function = Accessors.of(method);
			this.column = column;
		}
		
		public <I> PropertyAccessor<T, I> getAccessor() {
			return function;
		}
		
		public String getColumnName() {
			return column.getName();
		}
		
		public Class<?> getColumnType() {
			return column.getJavaType();
		}
		
		public boolean isPrimaryKey() {
			return column.isPrimaryKey();
		}
		
		public Column getColumn() {
			return column;
		}
	}
	
	/**
	 * Class very close to {@link FluentEmbeddableMappingConfigurationSupport}, but with dedicated methods to entity mapping such as
	 * identifier definition or configuration override by {@link Column}
	 */
	static class EntityDecoratedEmbeddableConfigurationSupport<C, I> extends FluentEmbeddableMappingConfigurationSupport<C> {
		
		private final FluentEntityMappingConfigurationSupport<C, I> entityConfigurationSupport;
		private OverridableColumnInset<C, ?> currentInset;
		
		/**
		 * Creates a builder to map the given class for persistence
		 *
		 * @param persistedClass the class to create a mapping for
		 */
		public EntityDecoratedEmbeddableConfigurationSupport(FluentEntityMappingConfigurationSupport<C, I> entityConfigurationSupport, Class<C> persistedClass) {
			super(persistedClass);
			this.entityConfigurationSupport = entityConfigurationSupport;
		}
		
		@Override
		protected <O> Inset<C, O> newInset(SerializableBiConsumer<C, O> setter) {
			this.currentInset = new OverridableColumnInset<>(setter, this);
			return (Inset<C, O>) currentInset;
		}
		
		@Override
		protected <O> Inset<C, O> newInset(SerializableFunction<C, O> getter) {
			this.currentInset = new OverridableColumnInset<>(getter, this);
			return (Inset<C, O>) currentInset;
		}
		
		/**
		 *  
		 * @return the last {@link Inset} built by {@link #newInset(SerializableFunction)} or {@link #newInset(SerializableBiConsumer)}
		 */
		@Override
		protected OverridableColumnInset<C, ?> currentInset() {
			return currentInset;
		}
		
		@Override
		protected LinkageByColumnName<C> newLinkage(Method method, String linkName) {
			return new EntityLinkageByColumnName<>(method, linkName);
		}
		
		@Override
		protected String giveLinkName(Method method) {
			if (Identified.class.isAssignableFrom(Reflections.javaBeanTargetType(method))) {
				return entityConfigurationSupport.joinColumnNamingStrategy.giveName(method);
			} else {
				return super.giveLinkName(method);
			}
		}
		
		/**
		 * Equivalent of {@link #addMapping(Method, String)} with a {@link Column}
		 * 
		 * @return a new Column added to the target table, throws an exception if already mapped
		 */
		<O> AbstractLinkage<C> addMapping(Method method, Column<Table, O> column) {
			PropertyAccessor<Object, Object> propertyAccessor = Accessors.of(method);
			assertMappingIsNotAlreadyDefined(column.getName(), propertyAccessor);
			EntityLinkageByColumn<C> newLinkage = new EntityLinkageByColumn<>(method, column);
			mapping.add(newLinkage);
			return newLinkage;
		}
		
		private IFluentMappingBuilderColumnOptions<C, I> applyAdditionalOptions(Method method, Linkage newMapping) {
			return new MethodDispatcher()
					.redirect(ColumnOptions.class, identifierPolicy -> {
						// Please note that we don't check for any id presence in inheritance since this will override parent one (see final build()) 
						if (entityConfigurationSupport.identifierAccessor != null) {
							throw new IllegalArgumentException("Identifier is already defined by " + entityConfigurationSupport.identifierAccessor.getAccessor());
						}
						if (identifierPolicy == IdentifierPolicy.ALREADY_ASSIGNED) {
							Class<I> identifierType = Reflections.propertyType(method);
							if (Identified.class.isAssignableFrom(method.getDeclaringClass()) && Identifier.class.isAssignableFrom(identifierType)) {
								entityConfigurationSupport.identifierInsertionManager = new IdentifiedIdentifierManager<>(identifierType);
							} else {
								throw new NotYetSupportedOperationException(
										IdentifierPolicy.ALREADY_ASSIGNED + " is only supported with entities that implement " + Reflections.toString(Identified.class));
							}
							if (newMapping instanceof EntityLinkageByColumnName) {
								// we force primary key so it's not necessary to be set by caller
								((EntityLinkageByColumnName) newMapping).primaryKey();
							} else if (newMapping instanceof EntityLinkageByColumn && !((EntityLinkageByColumn) newMapping).isPrimaryKey()) {
								// safeguard about misconfiguration, even if mapping would work it smells bad configuration
								throw new IllegalArgumentException("Identifier policy is assigned on a non primary key column");
							} else {
								// in case of evolution in the Linkage API
								throw new NotImplementedException(newMapping.getClass());
							}
						} else {
							throw new NotYetSupportedOperationException(identifierPolicy + " is not yet supported");
						}
						entityConfigurationSupport.identifierAccessor = (PropertyAccessor<C, I>) newMapping.getAccessor();
						// we return the fluent builder so user can chain with any other configuration
						return entityConfigurationSupport;
					})
					.fallbackOn(entityConfigurationSupport)
					.build((Class<IFluentMappingBuilderColumnOptions<C, I>>) (Class) IFluentMappingBuilderColumnOptions.class);
		}
	}
	
	/**
	 * {@link Inset} with override capability for mapping definition by {@link Column}
	 *
	 * @param <SRC> the owner type
	 * @param <TRGT> the target type
	 */
	private static class OverridableColumnInset<SRC, TRGT> extends Inset<SRC, TRGT> {
		
		private final ValueAccessPointMap<Column> overridenColumns = new ValueAccessPointMap<>();
		
		OverridableColumnInset(SerializableBiConsumer<SRC, TRGT> targetSetter, LambdaMethodUnsheller lambdaMethodUnsheller) {
			super(targetSetter, lambdaMethodUnsheller);
		}
		
		OverridableColumnInset(SerializableFunction<SRC, TRGT> targetGetter, LambdaMethodUnsheller lambdaMethodUnsheller) {
			super(targetGetter, lambdaMethodUnsheller);
		}
		
		public void override(SerializableFunction methodRef, Column column) {
			this.overridenColumns.put(new AccessorByMethodReference(methodRef), column);
		}
	}
	
	private static class OptimisticLockOption<C> {
		
		private final VersioningStrategy<Object, C> versioningStrategy;
		
		public OptimisticLockOption(PropertyAccessor<Object, C> propertyAccessor, Serie<C> serie) {
			this.versioningStrategy = new VersioningStrategySupport<>(propertyAccessor, serie);
		}
		
		public VersioningStrategy getVersioningStrategy() {
			return versioningStrategy;
		}
	}
	
	/**
	 * A small class for one-to-many options storage into a {@link CascadeMany}. Acts as a wrapper over it.
	 */
	private static class OneToManyOptionsSupport<T, I, O>
			implements OneToManyOptions<T, I, O> {
		
		private final CascadeMany<T, O, I, ? extends Collection> cascadeMany;
		
		public OneToManyOptionsSupport(CascadeMany<T, O, I, ? extends Collection> cascadeMany) {
			this.cascadeMany = cascadeMany;
		}
		
		@Override
		public IFluentMappingBuilderOneToManyOptions<T, I, O> mappedBy(SerializableBiConsumer<O, T> reverseLink) {
			cascadeMany.setReverseSetter(reverseLink);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public IFluentMappingBuilderOneToManyOptions<T, I, O> mappedBy(SerializableFunction<O, T> reverseLink) {
			cascadeMany.setReverseGetter(reverseLink);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public IFluentMappingBuilderOneToManyOptions<T, I, O> mappedBy(Column<Table, T> reverseLink) {
			cascadeMany.setReverseColumn(reverseLink);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public IFluentMappingBuilderOneToManyOptions<T, I, O> cascading(RelationshipMode relationshipMode) {
			cascadeMany.setRelationshipMode(relationshipMode);
			return null;	// we can return null because dispatcher will return proxy
		}
	}
	
	/**
	 * Identifier manager dedicated to {@link Identified} entities
	 * @param <C> entity type
	 * @param <I> identifier type
	 */
	private static class IdentifiedIdentifierManager<C, I> extends AlreadyAssignedIdentifierManager<C, I> {
		public IdentifiedIdentifierManager(Class<I> identifierType) {
			super(identifierType);
		}
		
		@Override
		public void setPersistedFlag(C e) {
			((Identified) e).getId().setPersisted();
		}
	}
	
	static class EntityMappingBuilder<C, I> {
		
		private final EntityMappingConfiguration<C, I> configurationSupport;
		
		private final EmbeddableMappingBuilder<C> embeddableMappingBuilder;
		/** Mapping of the super class, build early by {@link #build(PersistenceContext, ClassMappingStrategy)} */
		private ClassMappingStrategy<? super C, I, Table> inheritanceMappingStrategy;
		
		private final MethodReferenceCapturer methodSpy;
		
		EntityMappingBuilder(EntityMappingConfiguration<C, I> entityBuilderSupport, MethodReferenceCapturer methodSpy) {
			this.configurationSupport = entityBuilderSupport;
			this.methodSpy = methodSpy;
			this.embeddableMappingBuilder = new EmbeddableMappingBuilder<C>(entityBuilderSupport.getPropertiesMapping()) {
				/** Overriden to take property definition by column into account */
				@Override
				protected Column findColumn(ValueAccessPoint valueAccessPoint, String defaultColumnName, Map<String, Column<Table, Object>> tableColumnsPerName, Inset<C, ?> configuration) {
					Column overridenColumn = ((OverridableColumnInset<C, ?>) configuration).overridenColumns.get(valueAccessPoint);
					return Nullable.nullable(overridenColumn).orGet(() -> super.findColumn(valueAccessPoint, defaultColumnName, tableColumnsPerName, configuration));
				}
				
				/** Overriden to take primary key into account */
				@Override
				protected Column addLinkage(Linkage linkage) {
					Column column = super.addLinkage(linkage);
					// setting the primary key option as asked
					if (linkage instanceof FluentEntityMappingConfigurationSupport.EntityLinkage) {	// should always be true, see FluentEntityMappingConfigurationSupport.newLinkage(..)
						if (((EntityLinkage) linkage).isPrimaryKey()) {
							column.primaryKey();
						}
					} else {
						throw new NotImplementedException(linkage.getClass());
					}
					
					return column;
				}
				
				@Override
				protected Map<IReversibleAccessor, Column> buildMappingFromInheritance() {
					// adding mapped super class properties (if present)
					Map<IReversibleAccessor, Column> result = super.buildMappingFromInheritance();
					// adding inherited class properties (if present)
					if (inheritanceMappingStrategy != null) {
						result.putAll(collectMapping(inheritanceMappingStrategy, super.getTargetTable(), (a, c) -> c.getName()));
						// Dealing with identifier
						Duo<IReversibleAccessor, Column> idMapping = collectIdMapping();
						result.put(idMapping.getLeft(), idMapping.getRight());
					}
					return result;
				}
				
				private Duo<IReversibleAccessor, Column> collectIdMapping() {
					Duo<IReversibleAccessor, Column> result = new Duo<>();
					IdAccessor<? super C, ?> idAccessor = inheritanceMappingStrategy.getIdMappingStrategy().getIdAccessor();
					if (!(idAccessor instanceof SinglePropertyIdAccessor)) {
						throw new NotYetSupportedOperationException();
					}
					IReversibleAccessor<? super C, ?> entityIdentifierAccessor = ((SinglePropertyIdAccessor<? super C, ?>) idAccessor).getIdAccessor();
					// Because IdAccessor is a single column one (see assertion above) we can get the only column composing the primary key
					Column primaryKey = ((SimpleIdentifierAssembler) inheritanceMappingStrategy.getIdMappingStrategy().getIdentifierAssembler()).getColumn();
					Column projectedPrimarykey = super.getTargetTable().addColumn(primaryKey.getName(), primaryKey.getJavaType()).primaryKey();
					projectedPrimarykey.setAutoGenerated(primaryKey.isAutoGenerated());
					result.setLeft(entityIdentifierAccessor);
					result.setRight(projectedPrimarykey);
					return result;
				}
			};
		}
		
		private <T extends Table> ClassMappingStrategy<C, I, T> buildClassMappingStrategy(Dialect dialect, Table targetTable) {
			// dealing with identifier
			Duo<IReversibleAccessor<C, I>, IdentifierInsertionManager<C, I>> identification = determineIdentification();
			IReversibleAccessor<C, I> identifierAccessor = identification.getLeft();
			IdentifierInsertionManager<C, I> identifierInsertionManager = identification.getRight();
			
			Map<IReversibleAccessor, Column> columnMapping = this.embeddableMappingBuilder.build(dialect, targetTable);
			
			Column primaryKey = columnMapping.get(identifierAccessor);
			if (primaryKey == null) {
				// since this instance manage table_per_class or single_table inheritance, primary key is expected to be on target table 
				throw new UnsupportedOperationException("No matching primary key columns for identifier "
						+ MemberDefinition.toString(identifierAccessor) + " in table " + targetTable.getName());
			} else {
				List<IReversibleAccessor> identifierAccessors = collect(columnMapping.entrySet(), e -> e.getValue().isPrimaryKey(), Entry::getKey, ArrayList::new);
				if (identifierAccessors.size() > 1) {
					throw new NotYetSupportedOperationException("Composed primary key is not yet supported");
				}
			}
			
			return new ClassMappingStrategy<C, I, T>(configurationSupport.getPersistedClass(), (T) embeddableMappingBuilder.getTargetTable(),
					(Map) columnMapping, identifierAccessor, identifierInsertionManager);
		}
		
		/**
		 * Looks for identifier as well as its inserter, throwing exception if configuration is not coherent
		 * @return a couple that defines identification of the mapping
		 */
		private Duo<IReversibleAccessor<C, I>, IdentifierInsertionManager<C, I>> determineIdentification() {
			if (configurationSupport.getIdentifierAccessor() != null && configurationSupport.getInheritanceConfiguration() != null) {
				throw new MappingConfigurationException("Defining an identifier while inheritance is used is not supported");
			}
			IReversibleAccessor<C, I> identifierAccessor = null;
			IdentifierInsertionManager<C, I> identifierInsertionManager = null;
			if (configurationSupport.getInheritanceConfiguration() == null) {
				identifierAccessor = configurationSupport.getIdentifierAccessor();
				identifierInsertionManager = configurationSupport.getIdentifierInsertionManager();
			} else {
				// mapping with inheritance : identifier is expected to be defined on it
				// at least one parent ClassMappingStrategy exists, it necessarily defines an identifier : we stop at the very first one
				EntityMappingConfiguration<? super C, I> pawn = configurationSupport.getInheritanceConfiguration();
				while(identifierAccessor == null && pawn != null) {
					identifierAccessor = pawn.getIdentifierAccessor();
					identifierInsertionManager = (IdentifierInsertionManager<C, I>) pawn.getIdentifierInsertionManager();
					pawn = pawn.getInheritanceConfiguration();
				}
			}
			if (identifierAccessor == null) {
				// no ClassMappingStratey in hierarchy, so we can't get an identifier from it => impossible
				SerializableBiFunction<ColumnOptions, IdentifierPolicy, IFluentMappingBuilder> identifierMethodReference = ColumnOptions::identifier;
				Method identifierSetter = this.methodSpy.findMethod(identifierMethodReference);
				throw new UnsupportedOperationException("Identifier is not defined for " + Reflections.toString(configurationSupport.getPersistedClass())
						+ ", please add one throught " + Reflections.toString(identifierSetter));
			}
			return new Duo<>(identifierAccessor, identifierInsertionManager);
		}
		
		public Persister<C, I, Table> build(PersistenceContext persistenceContext) {
			return build(persistenceContext, new Table(configurationSupport.getPersistedClass().getSimpleName()));
		}
		
		public <T extends Table> Persister<C, I, T> build(PersistenceContext persistenceContext, T table) {
			if (configurationSupport.getInheritanceConfiguration() != null) {
				if (configurationSupport.isJoinTable()) {
					// Note that generics can't be used be cause "<? super C> can't be instantiated directly"
					inheritanceMappingStrategy = new JoinedTablesEntityMappingBuilder(configurationSupport.getInheritanceConfiguration(), methodSpy)
							.build(persistenceContext, table)
							.getMappingStrategy();
				} else {
					inheritanceMappingStrategy = new EntityMappingBuilder(configurationSupport.getInheritanceConfiguration(), methodSpy)
							.build(persistenceContext, table)
							.getMappingStrategy();
				}
			}
			
			return build(persistenceContext, buildClassMappingStrategy(persistenceContext.getDialect(), table));
		}
		
		<T extends Table> JoinedTablesPersister<C, I, T> build(PersistenceContext persistenceContext, ClassMappingStrategy<C, I, T> mainMappingStrategy) {
			// Please note that we could have created a simple Persister for cases of no one-to-one neither one-to-many relations
			// because in such cases no join is required, but it would have introduced inconsistent return type depending on configuration
			// which is hard to manage by JoinedTablesEntityMappingBuilder that uses this method
			JoinedTablesPersister<C, I, T> result = new JoinedTablesPersister<>(persistenceContext, mainMappingStrategy);
			// don't forget to register this new persister, its usefull for schema deployment
			persistenceContext.addPersister(result);
			
			CascadeOneConfigurer cascadeOneConfigurer = new CascadeOneConfigurer<>();
			for (CascadeOne<C, ?, ?> cascadeOne : this.configurationSupport.getOneToOnes()) {
				cascadeOneConfigurer.appendCascade(cascadeOne, result, this.configurationSupport.getForeignKeyNamingStrategy());
			}
			CascadeManyConfigurer cascadeManyConfigurer = new CascadeManyConfigurer();
			for (CascadeMany<C, ?, ?, ? extends Collection> cascadeMany : this.configurationSupport.getOneToManys()) {
				cascadeManyConfigurer.appendCascade(cascadeMany, result, this.configurationSupport.getForeignKeyNamingStrategy(),
						this.configurationSupport.getAssociationTableNamingStrategy(),
						persistenceContext.getDialect());
			}
			
			Nullable<VersioningStrategy> versionigStrategy = Nullable.nullable(this.configurationSupport.getOptimisticLockOption());
			if (versionigStrategy.isPresent()) {
				// we have to declare it to the mapping strategy. To do that we must find the versionning column
				Column column = result.getMappingStrategy().getMainMappingStrategy().getPropertyToColumn().get(this.configurationSupport.getOptimisticLockOption().getVersionAccessor());
				result.getMappingStrategy().addVersionedColumn(this.configurationSupport.getOptimisticLockOption().getVersionAccessor(), column);
				// and don't forget to give it to the workers !
				result.getUpdateExecutor().setVersioningStrategy(versionigStrategy.get());
				result.getInsertExecutor().setVersioningStrategy(versionigStrategy.get());
			}
			
			return result;
		}
	}
}
