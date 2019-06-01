package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.exception.NotImplementedException;
import org.gama.lang.function.Serie;
import org.gama.lang.reflect.MethodDispatcher;
import org.gama.reflection.AccessorByMethod;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.Accessors;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MemberDefinition;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.MutatorByMethodReference;
import org.gama.reflection.PropertyAccessor;
import org.gama.reflection.ValueAccessPointByMethodReference;
import org.gama.reflection.ValueAccessPointMap;
import org.gama.stalactite.persistence.engine.AbstractVersioningStrategy.VersioningStrategySupport;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfiguration.Linkage;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.AbstractLinkage;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.Inset;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.LinkageByColumnName;
import org.gama.stalactite.persistence.engine.IFluentEmbeddableMappingBuilder.IFluentEmbeddableMappingBuilderEmbedOptions;
import org.gama.stalactite.persistence.engine.IFluentEmbeddableMappingBuilder.IFluentEmbeddableMappingBuilderEnumOptions;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.builder.CascadeManyList;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

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
	
	private TableNamingStrategy tableNamingStrategy = TableNamingStrategy.DEFAULT;
	
	private PropertyAccessor<C, I> identifierAccessor;
	
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
	
	@Override
	public TableNamingStrategy getTableNamingStrategy() {
		return tableNamingStrategy;
	}
	
	public ColumnNamingStrategy getJoinColumnNamingStrategy() {
		return joinColumnNamingStrategy;
	}
	
	private Method captureMethod(SerializableFunction getter) {
		return this.methodSpy.findMethod(getter);
	}
	
	private Method captureMethod(SerializableBiConsumer setter) {
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
	public <O> IFluentMappingBuilderPropertyOptions<C, I> add(SerializableBiConsumer<C, O> setter) {
		return add(setter, (String) null);
	}
	
	@Override
	public <O> IFluentMappingBuilderPropertyOptions<C, I> add(SerializableFunction<C, O> getter) {
		return add(getter, (String) null);
	}
	
	@Override
	public <O> IFluentMappingBuilderPropertyOptions<C, I> add(SerializableBiConsumer<C, O> setter, String columnName) {
		AbstractLinkage<C> mapping = propertiesMappingConfigurationSurrogate.addMapping(setter, columnName);
		return this.propertiesMappingConfigurationSurrogate.applyAdditionalOptions(setter, mapping);
	}
	
	@Override
	public <O> IFluentMappingBuilderPropertyOptions<C, I> add(SerializableFunction<C, O> getter, String columnName) {
		AbstractLinkage<C> mapping = propertiesMappingConfigurationSurrogate.addMapping(getter, columnName);
		return this.propertiesMappingConfigurationSurrogate.applyAdditionalOptions(getter, mapping);
	}
	
	@Override
	public <O> IFluentMappingBuilderPropertyOptions<C, I> add(SerializableBiConsumer<C, O> setter, Column<Table, O> column) {
		AbstractLinkage<C> mapping = propertiesMappingConfigurationSurrogate.addMapping(setter, column);
		return this.propertiesMappingConfigurationSurrogate.applyAdditionalOptions(setter, mapping);
	}
	
	@Override
	public <O> IFluentMappingBuilderPropertyOptions<C, I> add(SerializableFunction<C, O> getter, Column<Table, O> column) {
		AbstractLinkage<C> mapping = propertiesMappingConfigurationSurrogate.addMapping(getter, column);
		return this.propertiesMappingConfigurationSurrogate.applyAdditionalOptions(getter, mapping);
	}
	
	@Override
	public <E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter) {
		return addEnum(setter, (String) null);
	}
	
	@Override
	public <E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter) {
		return addEnum(getter, (String) null);
	}
	
	@Override
	public <E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter, @javax.annotation.Nullable String columnName) {
		AbstractLinkage<C> linkage = propertiesMappingConfigurationSurrogate.addMapping(setter, columnName);
		return handleEnumOptions(propertiesMappingConfigurationSurrogate.addEnumOptions(linkage));
	}
	
	@Override
	public <E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter, @javax.annotation.Nullable String columnName) {
		AbstractLinkage<C> linkage = propertiesMappingConfigurationSurrogate.addMapping(getter, columnName);
		return handleEnumOptions(propertiesMappingConfigurationSurrogate.addEnumOptions(linkage));
	}
	
	@Override
	public <E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter, Column<Table, E> column) {
		AbstractLinkage<C> linkage = propertiesMappingConfigurationSurrogate.addMapping(setter, column);
		return handleEnumOptions(propertiesMappingConfigurationSurrogate.addEnumOptions(linkage));
	}
	
	@Override
	public <E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter, Column<Table, E> column) {
		AbstractLinkage<C> linkage = propertiesMappingConfigurationSurrogate.addMapping(getter, column);
		return handleEnumOptions(propertiesMappingConfigurationSurrogate.addEnumOptions(linkage));
	}
	
	private IFluentMappingBuilderEnumOptions<C, I> handleEnumOptions(IFluentEmbeddableMappingBuilderEnumOptions<C> enumOptionsHandler) {
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
	public <O, J, T extends Table> IFluentMappingBuilderOneToOneOptions<C, I, T> addOneToOne(
			SerializableFunction<C, O> getter,
			EntityMappingConfiguration<O, J> mappingConfiguration) {
		return addOneToOne(getter, mappingConfiguration, null);
	}
	
	@Override
	public <O, J, T extends Table> IFluentMappingBuilderOneToOneOptions<C, I, T> addOneToOne(
			SerializableFunction<C, O> getter,
			EntityMappingConfiguration<O, J> mappingConfiguration,
			T table) {
		// we declare the column on our side: we do it first because it checks some rules
		add(getter);
		// we keep it
		IReversibleAccessor<C, O> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				Accessors.accessorByMethodReference(getter),
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<C, O>(captureMethod(getter)).toMutator());
		CascadeOne<C, O, J> cascadeOne = new CascadeOne<>(propertyAccessor, mappingConfiguration, table);
		this.cascadeOnes.add(cascadeOne);
		// then we return an object that allows fluent settings over our OneToOne cascade instance
		return new MethodDispatcher()
				.redirect(OneToOneOptions.class, new OneToOneOptions<C, I, T>() {
					@Override
					public IFluentMappingBuilderOneToOneOptions<C, I, T> cascading(RelationshipMode relationshipMode) {
						cascadeOne.setRelationshipMode(relationshipMode);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public IFluentMappingBuilderOneToOneOptions<C, I, T> mandatory() {
						cascadeOne.setNullable(false);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public IFluentMappingBuilderOneToOneOptions<C, I, T> mappedBy(SerializableFunction reverseLink) {
						cascadeOne.setReverseGetter(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public IFluentMappingBuilderOneToOneOptions<C, I, T> mappedBy(SerializableBiConsumer reverseLink) {
						cascadeOne.setReverseSetter(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public IFluentMappingBuilderOneToOneOptions<C, I, T> mappedBy(Column<T, C> reverseLink) {
						cascadeOne.setReverseColumn(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderOneToOneOptions<C, I, T>>) (Class) IFluentMappingBuilderOneToOneOptions.class);
	}
	
	@Override
	public <O, J, S extends Set<O>> IFluentMappingBuilderOneToManyOptions<C, I, O> addOneToManySet(
			SerializableFunction<C, S> getter,
			EntityMappingConfiguration<O, J> mappingConfiguration) {
		return addOneToManySet(getter, mappingConfiguration, null);
	}
		
	@Override
	public <O, J, S extends Set<O>, T extends Table> IFluentMappingBuilderOneToManyOptions<C, I, O> addOneToManySet(
			SerializableFunction<C, S> getter,
			EntityMappingConfiguration<O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		
		AccessorByMethodReference<C, S> getterReference = Accessors.accessorByMethodReference(getter);
		IReversibleAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				getterReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<C, S>(captureMethod(getter)).toMutator());
		return addOneToManySet(propertyAccessor, getterReference, mappingConfiguration, table);
	}
	
	@Override
	public <O, J, S extends Set<O>, T extends Table> IFluentMappingBuilderOneToManyOptions<C, I, O> addOneToManySet(
			SerializableBiConsumer<C, S> setter,
			EntityMappingConfiguration<O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		
		MutatorByMethodReference<C, S> setterReference = Accessors.mutatorByMethodReference(setter);
		PropertyAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				Accessors.accessor(setterReference.getDeclaringClass(), Reflections.propertyName(setterReference.getMethodName())),
				setterReference
		);
		return addOneToManySet(propertyAccessor, setterReference, mappingConfiguration, table);
	}
	
	private <O, J, S extends Set<O>, T extends Table> IFluentMappingBuilderOneToManyOptions<C, I, O> addOneToManySet(
			IReversibleAccessor<C, S> propertyAccessor,
			ValueAccessPointByMethodReference methodReference,
			EntityMappingConfiguration<O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		CascadeMany<C, O, J, S> cascadeMany = new CascadeMany<>(propertyAccessor, methodReference, mappingConfiguration, table);
		this.cascadeManys.add(cascadeMany);
		return new MethodDispatcher()
				.redirect(OneToManyOptions.class, new OneToManyOptionsSupport<>(cascadeMany), true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderOneToManyOptions<C, I, O>>) (Class) IFluentMappingBuilderOneToManyOptions.class);
	}
	
	@Override
	public <O, J, S extends List<O>> IFluentMappingBuilderOneToManyListOptions<C, I, O> addOneToManyList(
			SerializableFunction<C, S> getter,
			EntityMappingConfiguration<O, J> mappingConfiguration) {
		return addOneToManyList(getter, mappingConfiguration, null);
	}
		
	@Override
	public <O, J, S extends List<O>, T extends Table> IFluentMappingBuilderOneToManyListOptions<C, I, O> addOneToManyList(
			SerializableFunction<C, S> getter,
			EntityMappingConfiguration<O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		
		AccessorByMethodReference<C, S> getterReference = Accessors.accessorByMethodReference(getter);
		IReversibleAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				getterReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<C, S>(captureMethod(getter)).toMutator());
		return addOneToManyList(propertyAccessor, getterReference, mappingConfiguration, table);
	}
	
	@Override
	public <O, J, S extends List<O>, T extends Table> IFluentMappingBuilderOneToManyListOptions<C, I, O> addOneToManyList(
			SerializableBiConsumer<C, S> setter,
			EntityMappingConfiguration<O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		
		MutatorByMethodReference<C, S> setterReference = Accessors.mutatorByMethodReference(setter);
		PropertyAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				Accessors.accessor(setterReference.getDeclaringClass(), Reflections.propertyName(setterReference.getMethodName())),
				setterReference
		);
		return addOneToManyList(propertyAccessor, setterReference, mappingConfiguration, table);
	}
	
	private <O, J, S extends List<O>, T extends Table> IFluentMappingBuilderOneToManyListOptions<C, I, O> addOneToManyList(
			IReversibleAccessor<C, S> propertyAccessor,
			ValueAccessPointByMethodReference methodReference,
			EntityMappingConfiguration<O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		CascadeManyList<C, O, J, ? extends List<O>> cascadeMany = new CascadeManyList<>(propertyAccessor, methodReference, mappingConfiguration, table);
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
		AccessorByMethodReference methodReference = Accessors.accessorByMethodReference(getter);
		Serie<V> serie;
		if (Integer.class.isAssignableFrom(methodReference.getPropertyType())) {
			serie = (Serie<V>) Serie.INTEGER_SERIE;
		} else if (Long.class.isAssignableFrom(methodReference.getPropertyType())) {
			serie = (Serie<V>) Serie.LONG_SERIE;
		} else if (Date.class.isAssignableFrom(methodReference.getPropertyType())) {
			serie = (Serie<V>) Serie.NOW_SERIE;
		} else {
			throw new NotImplementedException("Type of versioned property is not implemented, please provide a "
					+ Serie.class.getSimpleName() + " for it : " + Reflections.toString(methodReference.getPropertyType()));
		}
		return versionedBy(getter, methodReference, serie);
	}
	
	@Override
	public <V> IFluentMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter, Serie<V> serie) {
		return versionedBy(getter, new AccessorByMethodReference<>(getter), serie);
	}
	
	private <V> IFluentMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter, AccessorByMethodReference methodReference, Serie<V> serie) {
		optimisticLockOption = new OptimisticLockOption<>(methodReference, serie);
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
		return build(persistenceContext, null);
	}
	
	@Override
	public <T extends Table> Persister<C, I, T> build(PersistenceContext persistenceContext, @javax.annotation.Nullable T targetTable) {
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
	interface EntityLinkage<T> extends Linkage<T> {
		
		boolean isPrimaryKey();
	}
	
	static class EntityLinkageByColumnName<T>  extends LinkageByColumnName<T> implements EntityLinkage<T> {
		
		private boolean primaryKey;
		
		/**
		 * Constructor
		 * 
		 * @param accessor a {@link IReversibleAccessor}
		 * @param columnType the Java type of the column, will be converted to sql type thanks to {@link org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping}
		 * @param columnName an override of the default name that will be generated
		 */
		<O> EntityLinkageByColumnName(IReversibleAccessor<T, O> accessor, Class<O> columnType, String columnName) {
			super(accessor, columnType, columnName);
		}
		
		public boolean isPrimaryKey() {
			return primaryKey;
		}
		
		public void primaryKey() {
			this.primaryKey = true;
		}
	}
	
	static class EntityLinkageByColumn<T> extends AbstractLinkage<T> implements EntityLinkage<T> {
		
		private final PropertyAccessor<T, ?> function;
		private final Column column;
		
		/**
		 * Constructor with mandatory objects
		 * 
		 * @param propertyAccessor a {@link PropertyAccessor}
		 * @param column an override of the default column that would have been generated
		 */
		<I> EntityLinkageByColumn(PropertyAccessor<T, I> propertyAccessor, Column column) {
			this.function = propertyAccessor;
			this.column = column;
		}
		
		@Override
		public <I> PropertyAccessor<T, I> getAccessor() {
			return (PropertyAccessor<T, I>) function;
		}
		
		@Override
		public String getColumnName() {
			return column.getName();
		}
		
		@Override
		public Class<?> getColumnType() {
			return column.getJavaType();
		}
		
		@Override
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
		protected <O> EntityLinkageByColumnName<C> newLinkage(IReversibleAccessor<C, O> accessor, Class<O> returnType, String linkName) {
			return new EntityLinkageByColumnName<>(accessor, returnType, linkName);
		}
		
		@Override
		protected String giveLinkName(MemberDefinition memberDefinition) {
			if (Identified.class.isAssignableFrom(memberDefinition.getMemberType())) {
				return entityConfigurationSupport.joinColumnNamingStrategy.giveName(memberDefinition);
			} else {
				return super.giveLinkName(memberDefinition);
			}
		}
		
		
		<E> AbstractLinkage<C> addMapping(SerializableBiConsumer<C, E> setter, Column column) {
			MutatorByMethodReference<C, E> mutatorByMethodReference = Accessors.mutatorByMethodReference(setter);
			PropertyAccessor<C, E> propertyAccessor = new PropertyAccessor<>(
					Accessors.accessor(mutatorByMethodReference.getDeclaringClass(), Reflections.propertyName(mutatorByMethodReference.getMethodName())),
					mutatorByMethodReference
			);
			return addMapping(propertyAccessor, column);
		}
		
		<E> AbstractLinkage<C> addMapping(SerializableFunction<C, E> getter, Column column) {
			AccessorByMethodReference<C, E> accessorByMethodReference = Accessors.accessorByMethodReference(getter);
			PropertyAccessor<C, E> propertyAccessor = new PropertyAccessor<>(
					accessorByMethodReference,
					Accessors.mutator(accessorByMethodReference.getDeclaringClass(), Reflections.propertyName(accessorByMethodReference.getMethodName()), accessorByMethodReference.getPropertyType())
			);
			return addMapping(propertyAccessor, column);
		}
		
		/**
		 * Equivalent of {@link #addMapping(PropertyAccessor, ValueAccessPointByMethodReference, String)} with a {@link Column}
		 * 
		 * @return a new Column added to the target table, throws an exception if already mapped
		 */
		AbstractLinkage<C> addMapping(PropertyAccessor<C, ?> propertyAccessor, Column column) {
			assertMappingIsNotAlreadyDefined(column.getName(), propertyAccessor);
			EntityLinkageByColumn<C> newLinkage = new EntityLinkageByColumn<>(propertyAccessor, column);
			mapping.add(newLinkage);
			return newLinkage;
		}
		
		private IFluentMappingBuilderPropertyOptions<C, I> applyAdditionalOptions(SerializableBiConsumer<C, ?> setter, AbstractLinkage newMapping) {
			MutatorByMethodReference<C, ?> mutatorByMethodReference = Accessors.mutatorByMethodReference(setter);
			return applyAdditionalOptions(mutatorByMethodReference, newMapping);
		}
		
		private IFluentMappingBuilderPropertyOptions<C, I> applyAdditionalOptions(SerializableFunction<C, ?> getter, AbstractLinkage newMapping) {
			AccessorByMethodReference<C, ?> accessorByMethodReference = Accessors.accessorByMethodReference(getter);
			return applyAdditionalOptions(accessorByMethodReference, newMapping);
		}
		
		private IFluentMappingBuilderPropertyOptions<C, I> applyAdditionalOptions(ValueAccessPointByMethodReference methodReference, AbstractLinkage newMapping) {
			return new MethodDispatcher()
					.redirect(ColumnOptions.class, new ColumnOptions() {
						@Override
						public ColumnOptions identifier(IdentifierPolicy identifierPolicy) {
							// Please note that we don't check for any id presence in inheritance since this will override parent one (see final build()) 
							if (entityConfigurationSupport.identifierAccessor != null) {
								throw new IllegalArgumentException("Identifier is already defined by " + MemberDefinition.toString(entityConfigurationSupport.identifierAccessor.getAccessor()));
							}
							if (identifierPolicy == IdentifierPolicy.ALREADY_ASSIGNED) {
								Class<I> identifierType = methodReference.getPropertyType();
								if (Identified.class.isAssignableFrom(methodReference.getDeclaringClass()) && Identifier.class.isAssignableFrom(identifierType)) {
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
									throw new IllegalArgumentException("Identifier policy is assigned to a non primary key column");
								} else {
									// in case of evolution in the Linkage API
									throw new NotImplementedException(newMapping.getClass());
								}
							} else {
								throw new NotYetSupportedOperationException(identifierPolicy + " is not yet supported");
							}
							entityConfigurationSupport.identifierAccessor = (PropertyAccessor<C, I>) newMapping.getAccessor();
							return null;
						}
						
						@Override
						public ColumnOptions mandatory() {
							newMapping.setNullable(false);
							return null;
						}
					}, true)
					.fallbackOn(entityConfigurationSupport)
					.build((Class<IFluentMappingBuilderPropertyOptions<C, I>>) (Class) IFluentMappingBuilderPropertyOptions.class);
		}
	}
	
	/**
	 * {@link Inset} with override capability for mapping definition by {@link Column}
	 *
	 * @param <SRC> the owner type
	 * @param <TRGT> the target type
	 */
	static class OverridableColumnInset<SRC, TRGT> extends Inset<SRC, TRGT> {
		
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
		
		ValueAccessPointMap<Column> getOverridenColumns() {
			return overridenColumns;
		}
	}
	
	private static class OptimisticLockOption<C> {
		
		private final VersioningStrategy<Object, C> versioningStrategy;
		
		public OptimisticLockOption(AccessorByMethodReference<Object, C> versionAccessor, Serie<C> serie) {
			this.versioningStrategy = new VersioningStrategySupport<>(new PropertyAccessor<>(
					versionAccessor,
					Accessors.mutator(versionAccessor.getDeclaringClass(), Reflections.propertyName(versionAccessor.getMethodName()), versionAccessor.getPropertyType())
			), serie);
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
	
}