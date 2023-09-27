package org.codefilarete.stalactite.engine.configurer;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.reflection.MutatorByMethod;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointByMethodReference;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ElementCollectionOptions;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.EnumOptions;
import org.codefilarete.stalactite.engine.FluentEmbeddableMappingBuilder.FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions;
import org.codefilarete.stalactite.engine.FluentEmbeddableMappingBuilder.FluentEmbeddableMappingBuilderEnumOptions;
import org.codefilarete.stalactite.engine.FluentSubEntityMappingConfiguration;
import org.codefilarete.stalactite.engine.ImportedEmbedWithColumnOptions;
import org.codefilarete.stalactite.engine.IndexableCollectionOptions;
import org.codefilarete.stalactite.engine.MappingConfigurationException;
import org.codefilarete.stalactite.engine.OneToManyOptions;
import org.codefilarete.stalactite.engine.OneToOneOptions;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.PropertyOptions;
import org.codefilarete.stalactite.engine.configurer.FluentEmbeddableMappingConfigurationSupport.LinkageSupport;
import org.codefilarete.stalactite.engine.configurer.FluentEntityMappingConfigurationSupport.OneToManyOptionsSupport;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyListRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelation;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.codefilarete.tool.reflect.MethodDispatcher;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static org.codefilarete.tool.Reflections.propertyName;

/**
 * A class that stores configuration made through a {@link FluentSubEntityMappingConfiguration}
 * 
 * @author Guillaume Mary
 */
public class FluentSubEntityMappingConfigurationSupport<C, I> implements FluentSubEntityMappingConfiguration<C, I> {
	
	private final Class<C> classToPersist;
	
	private final MethodReferenceCapturer methodSpy;
	
	private final List<OneToOneRelation<C, ?, ?>> oneToOneRelations = new ArrayList<>();
	
	private final List<OneToManyRelation<C, ?, ?, ? extends Collection>> oneToManyRelations = new ArrayList<>();
	
	private final List<ElementCollectionRelation<C, ?, ? extends Collection>> elementCollections = new ArrayList<>();
	
	private final SubEntityDecoratedEmbeddableConfigurationSupport<C, I> propertiesMappingConfigurationSurrogate;
	
	private PolymorphismPolicy<C> polymorphismPolicy;
	
	/**
	 * Creates a builder to map the given class for persistence
	 *
	 * @param classToPersist the class to create a mapping for
	 */
	public FluentSubEntityMappingConfigurationSupport(Class<C> classToPersist) {
		this.classToPersist = classToPersist;
		
		// Helper to capture Method behind method reference
		this.methodSpy = new MethodReferenceCapturer();
		
		this.propertiesMappingConfigurationSurrogate = new SubEntityDecoratedEmbeddableConfigurationSupport<>(this, classToPersist);
	}
	
	@Override
	public Class<C> getEntityType() {
		return classToPersist;
	}
	
	@Override
	public Function<Function<Column, Object>, C> getEntityFactory() {
		// for now (until reason to expose this to user) instantiation type is the same as entity one
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
	public <TRGT, TRGTID> List<OneToOneRelation<C, TRGT, TRGTID>> getOneToOnes() {
		return (List) oneToOneRelations;
	}
	
	@Override
	public <TRGT, TRGTID> List<OneToManyRelation<C, TRGT, TRGTID, ? extends Collection<TRGT>>> getOneToManys() {
		return (List) oneToManyRelations;
	}
	
	@Override
	public List<ElementCollectionRelation<C, ?, ? extends Collection>> getElementCollections() {
		return elementCollections;
	}
	
	@Override
	public PolymorphismPolicy<C> getPolymorphismPolicy() {
		return polymorphismPolicy;
	}
	
	@Override
	public <O> FluentSubEntityMappingBuilderPropertyOptions<C, I> map(SerializableBiConsumer<C, O> setter) {
		LinkageSupport<C, O> mapping = propertiesMappingConfigurationSurrogate.addMapping(setter);
		return this.propertiesMappingConfigurationSurrogate.wrapForAdditionalOptions(mapping);
	}
	
	@Override
	public <O> FluentSubEntityMappingBuilderPropertyOptions<C, I> map(SerializableFunction<C, O> getter) {
		LinkageSupport<C, O> mapping = propertiesMappingConfigurationSurrogate.addMapping(getter);
		return this.propertiesMappingConfigurationSurrogate.wrapForAdditionalOptions(mapping);
	}
	
	@Override
	public <E extends Enum<E>> FluentSubEntityMappingConfigurationEnumOptions<C, I> mapEnum(SerializableBiConsumer<C, E> setter) {
		LinkageSupport<C, E> linkage = propertiesMappingConfigurationSurrogate.addMapping(setter);
		return handleEnumOptions(propertiesMappingConfigurationSurrogate.wrapWithEnumOptions(linkage));
	}
	
	@Override
	public <E extends Enum<E>> FluentSubEntityMappingConfigurationEnumOptions<C, I> mapEnum(SerializableFunction<C, E> getter) {
		LinkageSupport<C, E> linkage = propertiesMappingConfigurationSurrogate.addMapping(getter);
		return handleEnumOptions(propertiesMappingConfigurationSurrogate.wrapWithEnumOptions(linkage));
	}
	
	private FluentSubEntityMappingConfigurationEnumOptions<C, I> handleEnumOptions(FluentEmbeddableMappingBuilderEnumOptions<C> enumOptionsHandler) {
		// we redirect all of the EnumOptions method to the instance that can handle them, returning the dispatcher on this methods so one can chain
		// with some other methods, other methods are redirected to this instance because it can handle them.
		return new MethodDispatcher()
				.redirect(EnumOptions.class, enumOptionsHandler, true)
				.fallbackOn(this)
				.build((Class<FluentSubEntityMappingConfigurationEnumOptions<C, I>>) (Class) FluentSubEntityMappingConfigurationEnumOptions.class);
	}
	
	@Override
	public <O, S extends Collection<O>> FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> mapCollection(SerializableFunction<C, S> getter,
																														Class<O> componentType) {
		ElementCollectionRelation<C, O, S> elementCollectionRelation = new ElementCollectionRelation<>(getter, componentType,
				propertiesMappingConfigurationSurrogate, null);
		elementCollections.add(elementCollectionRelation);
		return new MethodReferenceDispatcher()
				.redirect((SerializableBiFunction<FluentSubEntityMappingBuilderElementCollectionOptions, String, FluentSubEntityMappingBuilderElementCollectionOptions>)
								FluentSubEntityMappingBuilderElementCollectionOptions::override,
						elementCollectionRelation::overrideColumnName)
				.redirect(ElementCollectionOptions.class, wrapAsOptions(elementCollectionRelation), true)
				.fallbackOn(this)
				.build((Class<FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S>>) (Class) FluentSubEntityMappingBuilderElementCollectionOptions.class);
	}
	
	@Override
	public <O, S extends Collection<O>> FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> mapCollection(SerializableBiConsumer<C, S> setter,
																														Class<O> componentType) {
		ElementCollectionRelation<C, O, S> elementCollectionRelation = new ElementCollectionRelation<>(setter, componentType, null);
		elementCollections.add(elementCollectionRelation);
		return new MethodReferenceDispatcher()
				.redirect((SerializableBiFunction<FluentSubEntityMappingBuilderElementCollectionOptions, String, FluentSubEntityMappingBuilderElementCollectionOptions>)
								FluentSubEntityMappingBuilderElementCollectionOptions::override,
						elementCollectionRelation::overrideColumnName)
				.redirect(ElementCollectionOptions.class, wrapAsOptions(elementCollectionRelation), true)
				.fallbackOn(this)
				.build((Class<FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S>>) (Class) FluentSubEntityMappingBuilderElementCollectionOptions.class);
	}
	
	@Override
	public <O, S extends Collection<O>> FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> mapCollection(SerializableFunction<C, S> getter,
																																   Class<O> componentType,
																																   EmbeddableMappingConfigurationProvider<O> embeddableConfiguration) {
		ElementCollectionRelation<C, O, S> elementCollectionRelation = new ElementCollectionRelation<>(getter, componentType,
				propertiesMappingConfigurationSurrogate,
				embeddableConfiguration);
		elementCollections.add(elementCollectionRelation);
		return new MethodReferenceDispatcher()
				.redirect((SerializableTriFunction<FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions, SerializableFunction, String, FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions>)
								FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions::overrideName,
						(BiConsumer<SerializableFunction, String>) elementCollectionRelation::overrideName)
				.redirect(ElementCollectionOptions.class, wrapAsOptions(elementCollectionRelation), true)
				.fallbackOn(this)
				.build((Class<FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S>>) (Class) FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions.class);
	}
	
	@Override
	public <O, S extends Collection<O>> FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> mapCollection(SerializableBiConsumer<C, S> setter,
																																   Class<O> componentType,
																																   EmbeddableMappingConfigurationProvider<O> embeddableConfiguration) {
		ElementCollectionRelation<C, O, S> elementCollectionRelation = new ElementCollectionRelation<>(setter, componentType, embeddableConfiguration);
		elementCollections.add(elementCollectionRelation);
		return new MethodReferenceDispatcher()
				.redirect((SerializableTriFunction<FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions, SerializableFunction, String, FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions>)
								FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions::overrideName,
						(BiConsumer<SerializableFunction, String>) elementCollectionRelation::overrideName)
				.redirect(ElementCollectionOptions.class, wrapAsOptions(elementCollectionRelation), true)
				.fallbackOn(this)
				.build((Class<FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S>>) (Class) FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions.class);
	}
	
	private <O, S extends Collection<O>> ElementCollectionOptions<C, O, S> wrapAsOptions(ElementCollectionRelation<C, O, S> elementCollectionRelation) {
		return new ElementCollectionOptions<C, O, S>() {
			
			@Override
			public ElementCollectionOptions<C, O, S> withCollectionFactory(Supplier<? extends S> collectionFactory) {
				elementCollectionRelation.setCollectionFactory(collectionFactory);
				return null;
			}
			
			@Override
			public FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> mappedBy(String name) {
				elementCollectionRelation.setReverseColumnName(name);
				return null;
			}
			
			@Override
			public ElementCollectionOptions<C, O, S> withTable(Table table) {
				elementCollectionRelation.setTargetTable(table);
				return null;
			}
			
			@Override
			public ElementCollectionOptions<C, O, S> withTable(String tableName) {
				elementCollectionRelation.setTargetTableName(tableName);
				return null;
			}
		};
	}
	
	@Override
	public <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T> mapOneToOne(
			SerializableFunction<C, O> getter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration) {
		return mapOneToOne(getter, mappingConfiguration, null);
	}
	
	@Override
	public <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T> mapOneToOne(
			SerializableBiConsumer<C, O> setter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration) {
		return mapOneToOne(setter, mappingConfiguration, null);
	}
	
	@Override
	public <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T> mapOneToOne(
			SerializableBiConsumer<C, O> setter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			T table) {
		MutatorByMethodReference<C, O> mutatorByMethodReference = Accessors.mutatorByMethodReference(setter);
		PropertyAccessor<C, O> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				new MutatorByMethod<C, O>(captureMethod(setter)).toAccessor(),
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				mutatorByMethodReference);
		OneToOneRelation<C, O, J> oneToOneRelation = new OneToOneRelation<>(propertyAccessor, mappingConfiguration.getConfiguration(), table);
		this.oneToOneRelations.add(oneToOneRelation);
		return wrapForAdditionalOptions(oneToOneRelation);
	}
	
	@Override
	public <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T> mapOneToOne(
			SerializableFunction<C, O> getter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			T table) {
		AccessorByMethodReference<C, O> accessorByMethodReference = Accessors.accessorByMethodReference(getter);
		PropertyAccessor<C, O> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				accessorByMethodReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<C, O>(captureMethod(getter)).toMutator());
		OneToOneRelation<C, O, J> oneToOneRelation = new OneToOneRelation<>(propertyAccessor, mappingConfiguration.getConfiguration(), table);
		this.oneToOneRelations.add(oneToOneRelation);
		return wrapForAdditionalOptions(oneToOneRelation);
	}
	
	private <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T> wrapForAdditionalOptions(final OneToOneRelation<C, O, J> oneToOneRelation) {
		// then we return an object that allows fluent settings over our OneToOne cascade instance
		return new MethodDispatcher()
				.redirect(OneToOneOptions.class, new OneToOneOptions() {
					@Override
					public OneToOneOptions cascading(RelationMode relationMode) {
						oneToOneRelation.setRelationMode(relationMode);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions mandatory() {
						oneToOneRelation.setNullable(false);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions mappedBy(SerializableFunction reverseLink) {
						oneToOneRelation.setReverseGetter(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions mappedBy(SerializableBiConsumer reverseLink) {
						oneToOneRelation.setReverseSetter(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions mappedBy(Column reverseLink) {
						oneToOneRelation.setReverseColumn(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions fetchSeparately() {
						oneToOneRelation.fetchSeparately();
						return null;	// we can return null because dispatcher will return proxy
					}
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderOneToOneOptions<C, I, T>>) (Class) FluentMappingBuilderOneToOneOptions.class);
	}
	
	@Override
	public <O, J, S extends Set<O>> FluentMappingBuilderOneToManyOptions<C, I, O, S> mapOneToManySet(
			SerializableFunction<C, S> getter,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration) {
		return mapOneToManySet(getter, mappingConfiguration, null);
	}
		
	@Override
	public <O, J, S extends Set<O>, T extends Table> FluentMappingBuilderOneToManyOptions<C, I, O, S> mapOneToManySet(
			SerializableFunction<C, S> getter,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		
		AccessorByMethodReference<C, S> getterReference = Accessors.accessorByMethodReference(getter);
		ReversibleAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				getterReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<C, S>(captureMethod(getter)).toMutator());
		return mapOneToManySet(propertyAccessor, getterReference, mappingConfiguration, table);
	}
	
	@Override
	public <O, J, S extends Set<O>, T extends Table> FluentMappingBuilderOneToManyOptions<C, I, O, S> mapOneToManySet(
			SerializableBiConsumer<C, S> setter,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		
		MutatorByMethodReference<C, S> setterReference = Accessors.mutatorByMethodReference(setter);
		PropertyAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				Accessors.accessor(setterReference.getDeclaringClass(), propertyName(setterReference.getMethodName())),
				setterReference
		);
		return mapOneToManySet(propertyAccessor, setterReference, mappingConfiguration, table);
	}
	
	private <O, J, S extends Set<O>, T extends Table> FluentMappingBuilderOneToManyOptions<C, I, O, S> mapOneToManySet(
			ReversibleAccessor<C, S> propertyAccessor,
			ValueAccessPointByMethodReference methodReference,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		OneToManyRelation<C, O, J, S> oneToManyRelation = new OneToManyRelation<>(propertyAccessor, methodReference,
				(EntityMappingConfigurationProvider<? extends O, J>) mappingConfiguration, table);
		this.oneToManyRelations.add(oneToManyRelation);
		return new MethodDispatcher()
				.redirect(OneToManyOptions.class, new OneToManyOptionsSupport<>(oneToManyRelation), true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderOneToManyOptions<C, I, O, S>>) (Class) FluentMappingBuilderOneToManyOptions.class);
	}
	
	@Override
	public <O, J, S extends List<O>> FluentMappingBuilderOneToManyListOptions<C, I, O, S> mapOneToManyList(
			SerializableFunction<C, S> getter,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration) {
		return mapOneToManyList(getter, mappingConfiguration, null);
	}
		
	@Override
	public <O, J, S extends List<O>, T extends Table> FluentMappingBuilderOneToManyListOptions<C, I, O, S> mapOneToManyList(
			SerializableFunction<C, S> getter,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		
		AccessorByMethodReference<C, S> getterReference = Accessors.accessorByMethodReference(getter);
		ReversibleAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				getterReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<C, S>(captureMethod(getter)).toMutator());
		return mapOneToManyList(propertyAccessor, getterReference, mappingConfiguration, table);
	}
	
	@Override
	public <O, J, S extends List<O>, T extends Table> FluentMappingBuilderOneToManyListOptions<C, I, O, S> mapOneToManyList(
			SerializableBiConsumer<C, S> setter,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		
		MutatorByMethodReference<C, S> setterReference = Accessors.mutatorByMethodReference(setter);
		PropertyAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				Accessors.accessor(setterReference.getDeclaringClass(), propertyName(setterReference.getMethodName())),
				setterReference
		);
		return mapOneToManyList(propertyAccessor, setterReference, mappingConfiguration, table);
	}
	
	private <O, J, S extends List<O>, T extends Table> FluentMappingBuilderOneToManyListOptions<C, I, O, S> mapOneToManyList(
			ReversibleAccessor<C, S> propertyAccessor,
			ValueAccessPointByMethodReference methodReference,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		OneToManyListRelation<C, O, J, ? extends List<O>> oneToManyListRelation = new OneToManyListRelation<>(propertyAccessor, methodReference,
																						(EntityMappingConfiguration<? extends O, J>) mappingConfiguration.getConfiguration(), table);
		this.oneToManyRelations.add(oneToManyListRelation);
		return new MethodDispatcher()
				.redirect(OneToManyOptions.class, new OneToManyOptionsSupport<>(oneToManyListRelation), true)	// true to allow "return null" in implemented methods
				.redirect(IndexableCollectionOptions.class, orderingColumn -> {
					oneToManyListRelation.setIndexingColumn(orderingColumn);
					return null;
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderOneToManyListOptions<C, I, O, S>>) (Class) FluentMappingBuilderOneToManyListOptions.class);
	}
	
	@Override
	public <O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableFunction<C, O> getter,
																									 EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder) {
		return embed(propertiesMappingConfigurationSurrogate.embed(getter, embeddableMappingBuilder));
	}
	
	@Override
	public <O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableBiConsumer<C, O> setter,
																									 EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder) {
		return embed(propertiesMappingConfigurationSurrogate.embed(setter, embeddableMappingBuilder));
	}
	
	@Override
	public FluentSubEntityMappingConfiguration<C, I> mapPolymorphism(PolymorphismPolicy<C> polymorphismPolicy) {
		this.polymorphismPolicy = polymorphismPolicy;
		return this;
	}
	
	private <O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embedSupport) {
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
				.build((Class<FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O>>) (Class) FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions.class);
	}
	
	@Override
	public FluentSubEntityMappingConfiguration<C, I> withColumnNaming(ColumnNamingStrategy columnNamingStrategy) {
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
		
		<E> LinkageSupport<C, E> addMapping(SerializableBiConsumer<C, E> setter) {
			LinkageSupport<C, E> newLinkage = new LinkageSupport<>(setter);
			mapping.add(newLinkage);
			return newLinkage;
		}
		
		<E> LinkageSupport<C, E> addMapping(SerializableFunction<C, E> getter) {
			LinkageSupport<C, E> newLinkage = new LinkageSupport<>(getter);
			mapping.add(newLinkage);
			return newLinkage;
		}
		
		private <O> FluentSubEntityMappingBuilderPropertyOptions<C, I> wrapForAdditionalOptions(LinkageSupport<C, O> newMapping) {
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
						
						@Override
						public PropertyOptions readonly() {
							newMapping.readonly();
							return null;
						}
						
						@Override
						public PropertyOptions columnName(String name) {
							newMapping.setColumnOptions(new ColumnLinkageOptionsByName(name));
							return null;
						}
						
						@Override
						public PropertyOptions column(Column column) {
							newMapping.setColumnOptions(new ColumnLinkageOptionsByColumn(column));
							return null;
						}
						
						@Override
						public PropertyOptions fieldName(String name) {
							Field field = Reflections.findField(SubEntityDecoratedEmbeddableConfigurationSupport.this.entityConfigurationSupport.classToPersist, name);
							if (field == null) {
								throw new MappingConfigurationException(("Field " + name
										+ " was not found in " + Reflections.toString(SubEntityDecoratedEmbeddableConfigurationSupport.this.entityConfigurationSupport.classToPersist)));
							}
							newMapping.setField(field);
							return null;
						}
					}, true)
					.fallbackOn(entityConfigurationSupport)
					.build((Class<FluentSubEntityMappingBuilderPropertyOptions<C, I>>) (Class) FluentSubEntityMappingBuilderPropertyOptions.class);
		}
	}
	
}
