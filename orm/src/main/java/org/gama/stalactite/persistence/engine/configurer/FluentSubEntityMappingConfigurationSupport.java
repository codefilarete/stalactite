package org.gama.stalactite.persistence.engine.configurer;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Reflections;
import org.gama.lang.function.SerializableTriFunction;
import org.gama.lang.reflect.MethodDispatcher;
import org.gama.reflection.AccessorByMethod;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.AccessorDefinition;
import org.gama.reflection.Accessors;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.reflection.MutatorByMethod;
import org.gama.reflection.MutatorByMethodReference;
import org.gama.reflection.PropertyAccessor;
import org.gama.reflection.ValueAccessPointByMethodReference;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.ElementCollectionOptions;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfiguration;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfigurationProvider;
import org.gama.stalactite.persistence.engine.EntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.EntityMappingConfigurationProvider;
import org.gama.stalactite.persistence.engine.EnumOptions;
import org.gama.stalactite.persistence.engine.IFluentEmbeddableMappingBuilder.IFluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions;
import org.gama.stalactite.persistence.engine.IFluentEmbeddableMappingBuilder.IFluentEmbeddableMappingBuilderEnumOptions;
import org.gama.stalactite.persistence.engine.IFluentSubEntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.ImportedEmbedWithColumnOptions;
import org.gama.stalactite.persistence.engine.IndexableCollectionOptions;
import org.gama.stalactite.persistence.engine.OneToManyOptions;
import org.gama.stalactite.persistence.engine.OneToOneOptions;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy;
import org.gama.stalactite.persistence.engine.PropertyOptions;
import org.gama.stalactite.persistence.engine.configurer.FluentEmbeddableMappingConfigurationSupport.AbstractLinkage;
import org.gama.stalactite.persistence.engine.configurer.FluentEntityMappingConfigurationSupport.OneToManyOptionsSupport;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.Reflections.propertyName;

/**
 * A class that stores configuration made throught a {@link IFluentSubEntityMappingConfiguration}
 * 
 * @author Guillaume Mary
 */
public class FluentSubEntityMappingConfigurationSupport<C, I> implements IFluentSubEntityMappingConfiguration<C, I> {
	
	private final Class<C> persistedClass;
	
	private final MethodReferenceCapturer methodSpy;
	
	private final List<CascadeOne<C, ?, ?>> cascadeOnes = new ArrayList<>();
	
	private final List<CascadeMany<C, ?, ?, ? extends Collection>> cascadeManys = new ArrayList<>();
	
	private final List<ElementCollectionLinkage<C, ?, ? extends Collection>> elementCollections = new ArrayList<>();
	
	private final SubEntityDecoratedEmbeddableConfigurationSupport<C, I> propertiesMappingConfigurationSurrogate;
	
	private PolymorphismPolicy<C> polymorphismPolicy;
	
	/**
	 * Creates a builder to map the given class for persistence
	 *
	 * @param persistedClass the class to create a mapping for
	 */
	public FluentSubEntityMappingConfigurationSupport(Class<C> persistedClass) {
		this.persistedClass = persistedClass;
		
		// Helper to capture Method behind method reference
		this.methodSpy = new MethodReferenceCapturer();
		
		this.propertiesMappingConfigurationSurrogate = new SubEntityDecoratedEmbeddableConfigurationSupport<>(this, persistedClass);
	}
	
	@Override
	public Class<C> getEntityType() {
		return persistedClass;
	}
	
	@Override
	public Function<Function<Column, Object>, C> getEntityFactory() {
		// for now (until reason to expose this to user) instanciation type is the same as entity one
		return row -> Reflections.newInstance(getEntityType());
	}
	
	private Method captureMethod(SerializableFunction getter) {
		return this.methodSpy.findMethod(getter);
	}
	
	private Method captureMethod(SerializableBiConsumer setter) {
		return this.methodSpy.findMethod(setter);
	}
	
	@Override
	public EmbeddableMappingConfiguration<C> getPropertiesMapping() {
		return propertiesMappingConfigurationSurrogate;
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
	public List<ElementCollectionLinkage<C, ?, ? extends Collection>> getElementCollections() {
		return elementCollections;
	}
	
	@Override
	public PolymorphismPolicy<C> getPolymorphismPolicy() {
		return polymorphismPolicy;
	}
	
	@Override
	public <O> IFluentSubEntityMappingBuilderPropertyOptions<C, I> add(SerializableBiConsumer<C, O> setter) {
		return add(setter, (String) null);
	}
	
	@Override
	public <O> IFluentSubEntityMappingBuilderPropertyOptions<C, I> add(SerializableFunction<C, O> getter) {
		return add(getter, (String) null);
	}
	
	@Override
	public <O> IFluentSubEntityMappingBuilderPropertyOptions<C, I> add(SerializableBiConsumer<C, O> setter, String columnName) {
		AbstractLinkage<C> mapping = propertiesMappingConfigurationSurrogate.addMapping(setter, columnName);
		return this.propertiesMappingConfigurationSurrogate.wrapForAdditionalOptions(mapping);
	}
	
	@Override
	public <O> IFluentSubEntityMappingBuilderPropertyOptions<C, I> add(SerializableFunction<C, O> getter, String columnName) {
		AbstractLinkage<C> mapping = propertiesMappingConfigurationSurrogate.addMapping(getter, columnName);
		return this.propertiesMappingConfigurationSurrogate.wrapForAdditionalOptions(mapping);
	}
	
	@Override
	public <O> IFluentSubEntityMappingBuilderPropertyOptions<C, I> add(SerializableBiConsumer<C, O> setter, Column<? extends Table, O> column) {
		AbstractLinkage<C> mapping = propertiesMappingConfigurationSurrogate.addMapping(setter, column);
		return this.propertiesMappingConfigurationSurrogate.wrapForAdditionalOptions(mapping);
	}
	
	@Override
	public <O> IFluentSubEntityMappingBuilderPropertyOptions<C, I> add(SerializableFunction<C, O> getter, Column<? extends Table, O> column) {
		AbstractLinkage<C> mapping = propertiesMappingConfigurationSurrogate.addMapping(getter, column);
		return this.propertiesMappingConfigurationSurrogate.wrapForAdditionalOptions(mapping);
	}
	
	@Override
	public <E extends Enum<E>> IFluentSubEntityMappingConfigurationEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter) {
		return addEnum(setter, (String) null);
	}
	
	@Override
	public <E extends Enum<E>> IFluentSubEntityMappingConfigurationEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter) {
		return addEnum(getter, (String) null);
	}
	
	@Override
	public <E extends Enum<E>> IFluentSubEntityMappingConfigurationEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter, @javax.annotation.Nullable String columnName) {
		AbstractLinkage<C> linkage = propertiesMappingConfigurationSurrogate.addMapping(setter, columnName);
		return handleEnumOptions(propertiesMappingConfigurationSurrogate.addEnumOptions(linkage));
	}
	
	@Override
	public <E extends Enum<E>> IFluentSubEntityMappingConfigurationEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter, @javax.annotation.Nullable String columnName) {
		AbstractLinkage<C> linkage = propertiesMappingConfigurationSurrogate.addMapping(getter, columnName);
		return handleEnumOptions(propertiesMappingConfigurationSurrogate.addEnumOptions(linkage));
	}
	
	@Override
	public <E extends Enum<E>> IFluentSubEntityMappingConfigurationEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter, Column<? extends Table, E> column) {
		AbstractLinkage<C> linkage = propertiesMappingConfigurationSurrogate.addMapping(setter, column);
		return handleEnumOptions(propertiesMappingConfigurationSurrogate.addEnumOptions(linkage));
	}
	
	@Override
	public <E extends Enum<E>> IFluentSubEntityMappingConfigurationEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter, Column<? extends Table, E> column) {
		AbstractLinkage<C> linkage = propertiesMappingConfigurationSurrogate.addMapping(getter, column);
		return handleEnumOptions(propertiesMappingConfigurationSurrogate.addEnumOptions(linkage));
	}
	
	private IFluentSubEntityMappingConfigurationEnumOptions<C, I> handleEnumOptions(IFluentEmbeddableMappingBuilderEnumOptions<C> enumOptionsHandler) {
		// we redirect all of the EnumOptions method to the instance that can handle them, returning the dispatcher on this methods so one can chain
		// with some other methods, other methods are redirected to this instance because it can handle them.
		return new MethodDispatcher()
				.redirect(EnumOptions.class, enumOptionsHandler, true)
				.fallbackOn(this)
				.build((Class<IFluentSubEntityMappingConfigurationEnumOptions<C, I>>) (Class) IFluentSubEntityMappingConfigurationEnumOptions.class);
	}
	
	@Override
	public <O, S extends Collection<O>> IFluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> addCollection(SerializableFunction<C, S> getter,
																														 Class<O> componentType) {
		ElementCollectionLinkage<C, O, S> elementCollectionLinkage = new ElementCollectionLinkage<>(getter, componentType,
				propertiesMappingConfigurationSurrogate, null);
		elementCollections.add(elementCollectionLinkage);
		return new MethodReferenceDispatcher()
				.redirect((SerializableBiFunction<IFluentSubEntityMappingBuilderElementCollectionOptions, String, IFluentSubEntityMappingBuilderElementCollectionOptions>)
								IFluentSubEntityMappingBuilderElementCollectionOptions::override,
						elementCollectionLinkage::overrideColumnName)
				.redirect(ElementCollectionOptions.class, wrapAsOptions(elementCollectionLinkage), true)
				.fallbackOn(this)
				.build((Class<IFluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S>>) (Class) IFluentSubEntityMappingBuilderElementCollectionOptions.class);
	}
	
	@Override
	public <O, S extends Collection<O>> IFluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> addCollection(SerializableBiConsumer<C, S> setter,
																														 Class<O> componentType) {
		ElementCollectionLinkage<C, O, S> elementCollectionLinkage = new ElementCollectionLinkage<>(setter, componentType, null);
		elementCollections.add(elementCollectionLinkage);
		return new MethodReferenceDispatcher()
				.redirect((SerializableBiFunction<IFluentSubEntityMappingBuilderElementCollectionOptions, String, IFluentSubEntityMappingBuilderElementCollectionOptions>)
								IFluentSubEntityMappingBuilderElementCollectionOptions::override,
						elementCollectionLinkage::overrideColumnName)
				.redirect(ElementCollectionOptions.class, wrapAsOptions(elementCollectionLinkage), true)
				.fallbackOn(this)
				.build((Class<IFluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S>>) (Class) IFluentSubEntityMappingBuilderElementCollectionOptions.class);
	}
	
	@Override
	public <O, S extends Collection<O>> IFluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> addCollection(SerializableFunction<C, S> getter,
																																	Class<O> componentType,
																																	EmbeddableMappingConfigurationProvider<O> embeddableConfiguration) {
		ElementCollectionLinkage<C, O, S> elementCollectionLinkage = new ElementCollectionLinkage<>(getter, componentType,
				propertiesMappingConfigurationSurrogate,
				embeddableConfiguration);
		elementCollections.add(elementCollectionLinkage);
		return new MethodReferenceDispatcher()
				.redirect((SerializableTriFunction<IFluentSubEntityMappingBuilderElementCollectionImportEmbedOptions, SerializableFunction, String, IFluentSubEntityMappingBuilderElementCollectionImportEmbedOptions>)
								IFluentSubEntityMappingBuilderElementCollectionImportEmbedOptions::overrideName,
						(BiConsumer<SerializableFunction, String>) elementCollectionLinkage::overrideName)
				.redirect(ElementCollectionOptions.class, wrapAsOptions(elementCollectionLinkage), true)
				.fallbackOn(this)
				.build((Class<IFluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S>>) (Class) IFluentSubEntityMappingBuilderElementCollectionImportEmbedOptions.class);
	}
	
	@Override
	public <O, S extends Collection<O>> IFluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> addCollection(SerializableBiConsumer<C, S> setter,
																																	Class<O> componentType,
																																	EmbeddableMappingConfigurationProvider<O> embeddableConfiguration) {
		ElementCollectionLinkage<C, O, S> elementCollectionLinkage = new ElementCollectionLinkage<>(setter, componentType, embeddableConfiguration);
		elementCollections.add(elementCollectionLinkage);
		return new MethodReferenceDispatcher()
				.redirect((SerializableTriFunction<IFluentSubEntityMappingBuilderElementCollectionImportEmbedOptions, SerializableFunction, String, IFluentSubEntityMappingBuilderElementCollectionImportEmbedOptions>)
								IFluentSubEntityMappingBuilderElementCollectionImportEmbedOptions::overrideName,
						(BiConsumer<SerializableFunction, String>) elementCollectionLinkage::overrideName)
				.redirect(ElementCollectionOptions.class, wrapAsOptions(elementCollectionLinkage), true)
				.fallbackOn(this)
				.build((Class<IFluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S>>) (Class) IFluentSubEntityMappingBuilderElementCollectionImportEmbedOptions.class);
	}
	
	private <O, S extends Collection<O>> ElementCollectionOptions<C, O, S> wrapAsOptions(ElementCollectionLinkage<C, O, S> elementCollectionLinkage) {
		return new ElementCollectionOptions<C, O, S>() {
			
			@Override
			public ElementCollectionOptions<C, O, S> withCollectionFactory(Supplier<? extends S> collectionFactory) {
				elementCollectionLinkage.setCollectionFactory(collectionFactory);
				return null;
			}
			
			@Override
			public IFluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> mappedBy(String name) {
				elementCollectionLinkage.setReverseColumnName(name);
				return null;
			}
			
			@Override
			public ElementCollectionOptions<C, O, S> withTable(Table table) {
				elementCollectionLinkage.setTargetTable(table);
				return null;
			}
			
			@Override
			public ElementCollectionOptions<C, O, S> withTable(String tableName) {
				elementCollectionLinkage.setTargetTableName(tableName);
				return null;
			}
		};
	}
	
	@Override
	public <O, J, T extends Table> IFluentMappingBuilderOneToOneOptions<C, I, T> addOneToOne(
			SerializableFunction<C, O> getter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration) {
		return addOneToOne(getter, mappingConfiguration, null);
	}
	
	@Override
	public <O, J, T extends Table> IFluentMappingBuilderOneToOneOptions<C, I, T> addOneToOne(
			SerializableBiConsumer<C, O> setter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration) {
		return addOneToOne(setter, mappingConfiguration, null);
	}
	
	@Override
	public <O, J, T extends Table> IFluentMappingBuilderOneToOneOptions<C, I, T> addOneToOne(
			SerializableBiConsumer<C, O> setter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			T table) {
		MutatorByMethodReference<C, O> mutatorByMethodReference = Accessors.mutatorByMethodReference(setter);
		PropertyAccessor<C, O> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				new MutatorByMethod<C, O>(captureMethod(setter)).toAccessor(),
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				mutatorByMethodReference);
		CascadeOne<C, O, J> cascadeOne = new CascadeOne<>(propertyAccessor, mappingConfiguration.getConfiguration(), table);
		this.cascadeOnes.add(cascadeOne);
		return wrapForAdditionalOptions(cascadeOne);
	}
	
	@Override
	public <O, J, T extends Table> IFluentMappingBuilderOneToOneOptions<C, I, T> addOneToOne(
			SerializableFunction<C, O> getter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			T table) {
		AccessorByMethodReference<C, O> accessorByMethodReference = Accessors.accessorByMethodReference(getter);
		PropertyAccessor<C, O> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				accessorByMethodReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<C, O>(captureMethod(getter)).toMutator());
		CascadeOne<C, O, J> cascadeOne = new CascadeOne<>(propertyAccessor, mappingConfiguration.getConfiguration(), table);
		this.cascadeOnes.add(cascadeOne);
		return wrapForAdditionalOptions(cascadeOne);
	}
	
	private <O, J, T extends Table> IFluentMappingBuilderOneToOneOptions<C, I, T> wrapForAdditionalOptions(final CascadeOne<C, O, J> cascadeOne) {
		// then we return an object that allows fluent settings over our OneToOne cascade instance
		return new MethodDispatcher()
				.redirect(OneToOneOptions.class, new OneToOneOptions() {
					@Override
					public OneToOneOptions cascading(RelationMode relationMode) {
						cascadeOne.setRelationMode(relationMode);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions mandatory() {
						cascadeOne.setNullable(false);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions mappedBy(SerializableFunction reverseLink) {
						cascadeOne.setReverseGetter(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions mappedBy(SerializableBiConsumer reverseLink) {
						cascadeOne.setReverseSetter(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions mappedBy(Column reverseLink) {
						cascadeOne.setReverseColumn(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderOneToOneOptions<C, I, T>>) (Class) IFluentMappingBuilderOneToOneOptions.class);
	}
	
	@Override
	public <O, J, S extends Set<O>> IFluentMappingBuilderOneToManyOptions<C, I, O, S> addOneToManySet(
			SerializableFunction<C, S> getter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration) {
		return addOneToManySet(getter, mappingConfiguration, null);
	}
		
	@Override
	public <O, J, S extends Set<O>, T extends Table> IFluentMappingBuilderOneToManyOptions<C, I, O, S> addOneToManySet(
			SerializableFunction<C, S> getter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
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
	public <O, J, S extends Set<O>, T extends Table> IFluentMappingBuilderOneToManyOptions<C, I, O, S> addOneToManySet(
			SerializableBiConsumer<C, S> setter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		
		MutatorByMethodReference<C, S> setterReference = Accessors.mutatorByMethodReference(setter);
		PropertyAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				Accessors.accessor(setterReference.getDeclaringClass(), propertyName(setterReference.getMethodName())),
				setterReference
		);
		return addOneToManySet(propertyAccessor, setterReference, mappingConfiguration, table);
	}
	
	private <O, J, S extends Set<O>, T extends Table> IFluentMappingBuilderOneToManyOptions<C, I, O, S> addOneToManySet(
			IReversibleAccessor<C, S> propertyAccessor,
			ValueAccessPointByMethodReference methodReference,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		CascadeMany<C, O, J, S> cascadeMany = new CascadeMany<>(propertyAccessor, methodReference, mappingConfiguration.getConfiguration(), table);
		this.cascadeManys.add(cascadeMany);
		return new MethodDispatcher()
				.redirect(OneToManyOptions.class, new OneToManyOptionsSupport<>(cascadeMany), true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderOneToManyOptions<C, I, O, S>>) (Class) IFluentMappingBuilderOneToManyOptions.class);
	}
	
	@Override
	public <O, J, S extends List<O>> IFluentMappingBuilderOneToManyListOptions<C, I, O, S> addOneToManyList(
			SerializableFunction<C, S> getter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration) {
		return addOneToManyList(getter, mappingConfiguration, null);
	}
		
	@Override
	public <O, J, S extends List<O>, T extends Table> IFluentMappingBuilderOneToManyListOptions<C, I, O, S> addOneToManyList(
			SerializableFunction<C, S> getter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
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
	public <O, J, S extends List<O>, T extends Table> IFluentMappingBuilderOneToManyListOptions<C, I, O, S> addOneToManyList(
			SerializableBiConsumer<C, S> setter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		
		MutatorByMethodReference<C, S> setterReference = Accessors.mutatorByMethodReference(setter);
		PropertyAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				Accessors.accessor(setterReference.getDeclaringClass(), propertyName(setterReference.getMethodName())),
				setterReference
		);
		return addOneToManyList(propertyAccessor, setterReference, mappingConfiguration, table);
	}
	
	private <O, J, S extends List<O>, T extends Table> IFluentMappingBuilderOneToManyListOptions<C, I, O, S> addOneToManyList(
			IReversibleAccessor<C, S> propertyAccessor,
			ValueAccessPointByMethodReference methodReference,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		CascadeManyList<C, O, J, ? extends List<O>> cascadeMany = new CascadeManyList<>(propertyAccessor, methodReference, mappingConfiguration.getConfiguration(), table);
		this.cascadeManys.add(cascadeMany);
		return new MethodDispatcher()
				.redirect(OneToManyOptions.class, new OneToManyOptionsSupport<>(cascadeMany), true)	// true to allow "return null" in implemented methods
				.redirect(IndexableCollectionOptions.class, orderingColumn -> {
					cascadeMany.setIndexingColumn(orderingColumn);
					return null;
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderOneToManyListOptions<C, I, O, S>>) (Class) IFluentMappingBuilderOneToManyListOptions.class);
	}
	
	@Override
	public <O> IFluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableFunction<C, O> getter,
																									  EmbeddableMappingConfigurationProvider<O> embeddableMappingBuilder) {
		return embed(propertiesMappingConfigurationSurrogate.embed(getter, embeddableMappingBuilder));
	}
	
	@Override
	public <O> IFluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableBiConsumer<C, O> setter,
																									  EmbeddableMappingConfigurationProvider<O> embeddableMappingBuilder) {
		return embed(propertiesMappingConfigurationSurrogate.embed(setter, embeddableMappingBuilder));
	}
	
	@Override
	public IFluentSubEntityMappingConfiguration<C, I> mapPolymorphism(PolymorphismPolicy<C> polymorphismPolicy) {
		this.polymorphismPolicy = polymorphismPolicy;
		return this;
	}
	
	private <O> IFluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(IFluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embedSupport) {
		return new MethodDispatcher()
				.redirect(ImportedEmbedWithColumnOptions.class, new ImportedEmbedWithColumnOptions<C>() {
					
					@Override
					public ImportedEmbedWithColumnOptions<C> overrideName(SerializableFunction getter, String columnName) {
						embedSupport.overrideName(getter, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}

					@Override
					public ImportedEmbedWithColumnOptions<C> overrideName(SerializableBiConsumer setter, String columnName) {
						embedSupport.overrideName(setter, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ImportedEmbedWithColumnOptions override(SerializableBiConsumer setter, Column targetColumn) {
						propertiesMappingConfigurationSurrogate.currentInset().override(setter, targetColumn);
						return null;
					}
					
					@Override
					public ImportedEmbedWithColumnOptions override(SerializableFunction getter, Column targetColumn) {
						propertiesMappingConfigurationSurrogate.currentInset().override(getter, targetColumn);
						return null;
					}

					@Override
					public ImportedEmbedWithColumnOptions<C> exclude(SerializableBiConsumer setter) {
						embedSupport.exclude(setter);
						return null;	// we can return null because dispatcher will return proxy
					}

					@Override
					public ImportedEmbedWithColumnOptions<C> exclude(SerializableFunction getter) {
						embedSupport.exclude(getter);
						return null;	// we can return null because dispatcher will return proxy
					}
				}, true)
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O>>) (Class) IFluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions.class);
	}
	
	@Override
	public IFluentSubEntityMappingConfiguration<C, I> withColumnNaming(ColumnNamingStrategy columnNamingStrategy) {
		this.propertiesMappingConfigurationSurrogate.withColumnNaming(columnNamingStrategy);
		return this;
	}
	
	/**
	 * Class very close to {@link FluentEmbeddableMappingConfigurationSupport}, but with dedicated methods to sub-entity mapping
	 */
	static class SubEntityDecoratedEmbeddableConfigurationSupport<C, I> extends FluentEmbeddableMappingConfigurationSupport<C> {
		
		private final FluentSubEntityMappingConfigurationSupport<C, I> entityConfigurationSupport;
		
		/**
		 * Creates a builder to map the given class for persistence
		 *
		 * @param persistedClass the class to create a mapping for
		 */
		public SubEntityDecoratedEmbeddableConfigurationSupport(FluentSubEntityMappingConfigurationSupport<C, I> entityConfigurationSupport, Class<C> persistedClass) {
			super(persistedClass);
			this.entityConfigurationSupport = entityConfigurationSupport;
		}
		
		@Override
		protected <O> EntityLinkageByColumnName<C> newLinkage(IReversibleAccessor<C, O> accessor, Class<O> returnType, String linkName) {
			return new EntityLinkageByColumnName<>(accessor, returnType, linkName);
		}
		
		<E> AbstractLinkage<C> addMapping(SerializableBiConsumer<C, E> setter, Column column) {
			return addMapping(Accessors.mutator(setter), column);
		}
		
		<E> AbstractLinkage<C> addMapping(SerializableFunction<C, E> getter, Column column) {
			return addMapping(Accessors.accessor(getter), column);
		}
		
		/**
		 * Equivalent of {@link #addMapping(IReversibleAccessor, AccessorDefinition, String)} with a {@link Column}
		 * 
		 * @return a new Column added to the target table, throws an exception if already mapped
		 */
		AbstractLinkage<C> addMapping(IReversibleAccessor<C, ?> propertyAccessor, Column column) {
			EntityLinkageByColumn<C> newLinkage = new EntityLinkageByColumn<>(propertyAccessor, column);
			mapping.add(newLinkage);
			return newLinkage;
		}
		
		private IFluentSubEntityMappingBuilderPropertyOptions<C, I> wrapForAdditionalOptions(AbstractLinkage<C> newMapping) {
			return new MethodDispatcher()
					.redirect(PropertyOptions.class, new PropertyOptions() {
						@Override
						public PropertyOptions mandatory() {
							newMapping.setNullable(false);
							return null;
						}
						
						@Override
						public PropertyOptions setByConstructor() {
							newMapping.setByConstructor();
							return null;
						}
					}, true)
					.fallbackOn(entityConfigurationSupport)
					.build((Class<IFluentSubEntityMappingBuilderPropertyOptions<C, I>>) (Class) IFluentSubEntityMappingBuilderPropertyOptions.class);
		}
	}
	
}
