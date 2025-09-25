package org.codefilarete.stalactite.engine.configurer;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.MutatorByMethod;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointByMethodReference;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ElementCollectionOptions;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.EnumOptions;
import org.codefilarete.stalactite.engine.FluentEmbeddableMappingBuilder.FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions;
import org.codefilarete.stalactite.engine.FluentEmbeddableMappingBuilder.FluentEmbeddableMappingBuilderEnumOptions;
import org.codefilarete.stalactite.engine.FluentSubEntityMappingBuilder;
import org.codefilarete.stalactite.engine.ImportedEmbedWithColumnOptions;
import org.codefilarete.stalactite.engine.MappingConfigurationException;
import org.codefilarete.stalactite.engine.OneToManyOptions;
import org.codefilarete.stalactite.engine.OneToOneOptions;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.PropertyOptions;
import org.codefilarete.stalactite.engine.SubEntityMappingConfiguration;
import org.codefilarete.stalactite.engine.configurer.FluentEmbeddableMappingConfigurationSupport.LinkageSupport;
import org.codefilarete.stalactite.engine.configurer.FluentEntityMappingConfigurationSupport.OneToManyOptionsSupport;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.manyToOne.ManyToOneRelation;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.map.MapRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelation;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.codefilarete.tool.reflect.MethodDispatcher;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static org.codefilarete.tool.Reflections.propertyName;

/**
 * A class that stores configuration made through a {@link FluentSubEntityMappingBuilder}
 * 
 * @author Guillaume Mary
 */
public class FluentSubEntityMappingConfigurationSupport<C, I> implements FluentSubEntityMappingBuilder<C, I> {
	
	private final Class<C> classToPersist;
	
	private final MethodReferenceCapturer methodSpy;
	
	private final List<OneToOneRelation<C, ?, ?>> oneToOneRelations = new ArrayList<>();
	
	private final List<OneToManyRelation<C, ?, ?, ? extends Collection<?>>> oneToManyRelations = new ArrayList<>();
	
	private final List<OneToManyRelation<C, ?, ?, ? extends Collection<?>>> manyToOneRelations = new ArrayList<>();
	
	private final List<ElementCollectionRelation<C, ?, ? extends Collection<?>>> elementCollections = new ArrayList<>();
	
	private final SubEntityDecoratedEmbeddableConfigurationSupport<C, I> propertiesMappingConfigurationDelegate;
	
	@Nullable
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
		
		this.propertiesMappingConfigurationDelegate = new SubEntityDecoratedEmbeddableConfigurationSupport<>(this, classToPersist);
	}
	
	@Override
	public SubEntityMappingConfiguration<C> getConfiguration() {
		return new SubEntityMappingConfiguration<C>() {
			@Override
			public Class<C> getEntityType() {
				return classToPersist;
			}
			
			@Override
			public EmbeddableMappingConfiguration<C> getPropertiesMapping() {
				return propertiesMappingConfigurationDelegate;
			}
			
			@Override
			public <TRGT, TRGTID> List<OneToOneRelation<C, TRGT, TRGTID>> getOneToOnes() {
				return (List) oneToOneRelations;
			}
			
			@Override
			public <TRGT, TRGTID> List<OneToManyRelation<C, TRGT, TRGTID, Collection<TRGT>>> getOneToManys() {
				return (List) oneToManyRelations;
			}
			
			@Override
			public <TRGT, TRGTID> List<ManyToManyRelation<C, TRGT, TRGTID, Collection<TRGT>, Collection<C>>> getManyToManys() {
				return Collections.emptyList();
			}
			
			@Override
			public <TRGT, TRGTID> List<ManyToOneRelation<C, TRGT, TRGTID, Collection<C>>> getManyToOnes() {
				return (List) manyToOneRelations;
			}
			
			@Override
			public <TRGT> List<ElementCollectionRelation<C, TRGT, ? extends Collection<TRGT>>> getElementCollections() {
				return (List) elementCollections;
			}
			
			@Override
			public List<MapRelation<C, ?, ?, ? extends Map>> getMaps() {
				return Collections.emptyList();
			}
			
			@Override
			public PolymorphismPolicy<C> getPolymorphismPolicy() {
				return polymorphismPolicy;
			}
		};
	}
	
	private Method captureMethod(SerializableFunction getter) {
		return this.methodSpy.findMethod(getter);
	}
	
	private Method captureMethod(SerializableBiConsumer setter) {
		return this.methodSpy.findMethod(setter);
	}
	
	@Override
	public <O> FluentSubEntityMappingBuilderPropertyOptions<C, I, O> map(SerializableBiConsumer<C, O> setter) {
		LinkageSupport<C, O> mapping = propertiesMappingConfigurationDelegate.addMapping(setter);
		return this.propertiesMappingConfigurationDelegate.wrapForAdditionalOptions(mapping);
	}
	
	@Override
	public <O> FluentSubEntityMappingBuilderPropertyOptions<C, I, O> map(SerializableFunction<C, O> getter) {
		LinkageSupport<C, O> mapping = propertiesMappingConfigurationDelegate.addMapping(getter);
		return this.propertiesMappingConfigurationDelegate.wrapForAdditionalOptions(mapping);
	}
	
	@Override
	public <E extends Enum<E>> FluentSubEntityMappingConfigurationEnumOptions<C, I, E> mapEnum(SerializableBiConsumer<C, E> setter) {
		LinkageSupport<C, E> linkage = propertiesMappingConfigurationDelegate.addMapping(setter);
		return handleEnumOptions(propertiesMappingConfigurationDelegate.wrapWithEnumOptions(linkage));
	}
	
	@Override
	public <E extends Enum<E>> FluentSubEntityMappingConfigurationEnumOptions<C, I, E> mapEnum(SerializableFunction<C, E> getter) {
		LinkageSupport<C, E> linkage = propertiesMappingConfigurationDelegate.addMapping(getter);
		return handleEnumOptions(propertiesMappingConfigurationDelegate.wrapWithEnumOptions(linkage));
	}
	
	private <E extends Enum<E>> FluentSubEntityMappingConfigurationEnumOptions<C, I, E> handleEnumOptions(FluentEmbeddableMappingBuilderEnumOptions<C, E> enumOptionsHandler) {
		// we redirect all of the EnumOptions method to the instance that can handle them, returning the dispatcher on this methods so one can chain
		// with some other methods, other methods are redirected to this instance because it can handle them.
		return new MethodDispatcher()
				.redirect(EnumOptions.class, enumOptionsHandler, true)
				.fallbackOn(this)
				.build((Class<FluentSubEntityMappingConfigurationEnumOptions<C, I, E>>) (Class) FluentSubEntityMappingConfigurationEnumOptions.class);
	}
	
	@Override
	public <O, S extends Collection<O>> FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> mapCollection(SerializableFunction<C, S> getter,
																														Class<O> componentType) {
		ElementCollectionRelation<C, O, S> elementCollectionRelation = new ElementCollectionRelation<>(getter, componentType,
				propertiesMappingConfigurationDelegate, null);
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
				propertiesMappingConfigurationDelegate,
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
			public FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> withReverseJoinColumn(String name) {
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
	public <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, O> mapOneToOne(
			SerializableFunction<C, O> getter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration) {
		return mapOneToOne(getter, mappingConfiguration, null);
	}
	
	@Override
	public <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, O> mapOneToOne(
			SerializableBiConsumer<C, O> setter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration) {
		return mapOneToOne(setter, mappingConfiguration, null);
	}
	
	@Override
	public <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, O> mapOneToOne(
			SerializableBiConsumer<C, O> setter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			T table) {
		// we keep close to user demand: we keep its method reference ...
		Mutator<C, O> mutatorByMethodReference = Accessors.mutatorByMethodReference(setter);
		// ... but we can't do it for accessor, so we use the most equivalent manner: an accessor based on setter method (fallback to property if not present)
		Accessor<C, O> accessor = new MutatorByMethod<C, O>(captureMethod(setter)).toAccessor();
		return mapOneToOne(accessor, mutatorByMethodReference, mappingConfiguration, table);
	}
	
	@Override
	public <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, O> mapOneToOne(
			SerializableFunction<C, O> getter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			T table) {
		// we keep close to user demand: we keep its method reference ...
		AccessorByMethodReference<C, O> accessorByMethodReference = Accessors.accessorByMethodReference(getter);
		// ... but we can't do it for mutator, so we use the most equivalent manner: a mutator based on getter method (fallback to property if not present)
		Mutator<C, O> mutator = new AccessorByMethod<C, O>(captureMethod(getter)).toMutator();
		return mapOneToOne(accessorByMethodReference, mutator, mappingConfiguration, table);
	}
	
	private <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, O> mapOneToOne(Accessor<C, O> accessor,
																							 Mutator<C, O> mutator,
																							 EntityMappingConfigurationProvider<O, J> mappingConfiguration,
																							 T table) {
		OneToOneRelation<C, O, J> oneToOneRelation = new OneToOneRelation<>(
				new PropertyAccessor<>(accessor, mutator),
				() -> this.polymorphismPolicy instanceof PolymorphismPolicy.TablePerClassPolymorphism,
				mappingConfiguration,
				table);
		this.oneToOneRelations.add(oneToOneRelation);
		return wrapForAdditionalOptions(oneToOneRelation);
	}
	
	private <O, J> FluentMappingBuilderOneToOneOptions<C, I, O> wrapForAdditionalOptions(OneToOneRelation<C, O, J> oneToOneRelation) {
		// then we return an object that allows fluent settings over our OneToOne cascade instance
		return new MethodDispatcher()
				.redirect(OneToOneOptions.class, new OneToOneOptions<C, J, O>() {
					@Override
					public OneToOneOptions<C, J, O> cascading(RelationMode relationMode) {
						oneToOneRelation.setRelationMode(relationMode);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions<C, J, O> mandatory() {
						oneToOneRelation.setNullable(false);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions<C, J, O> mappedBy(SerializableFunction<? super O, C> reverseLink) {
						oneToOneRelation.setReverseGetter(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions<C, J, O> mappedBy(SerializableBiConsumer<? super O, C> reverseLink) {
						oneToOneRelation.setReverseSetter(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions<C, J, O> mappedBy(Column<?, J> reverseLink) {
						oneToOneRelation.setReverseColumn(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions<C, J, O> mappedBy(String reverseColumnName) {
						oneToOneRelation.setReverseColumn(reverseColumnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions<C, J, O> fetchSeparately() {
						oneToOneRelation.fetchSeparately();
						return null;	// we can return null because dispatcher will return proxy
					}
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderOneToOneOptions<C, I, O>>) (Class) FluentMappingBuilderOneToOneOptions.class);
	}
	
	@Override
	public <O, J, S extends Collection<O>> FluentMappingBuilderOneToManyOptions<C, I, O, S> mapOneToMany(
			SerializableFunction<C, S> getter,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration) {
		return mapOneToMany(getter, mappingConfiguration, null);
	}
		
	@Override
	public <O, J, S extends Collection<O>, T extends Table> FluentMappingBuilderOneToManyOptions<C, I, O, S> mapOneToMany(
			SerializableFunction<C, S> getter,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		
		AccessorByMethodReference<C, S> getterReference = Accessors.accessorByMethodReference(getter);
		ReversibleAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				getterReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<C, S>(captureMethod(getter)).toMutator());
		return mapOneToMany(propertyAccessor, getterReference, mappingConfiguration, table);
	}
	
	@Override
	public <O, J, S extends Collection<O>, T extends Table> FluentMappingBuilderOneToManyOptions<C, I, O, S> mapOneToMany(
			SerializableBiConsumer<C, S> setter,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration) {
		return mapOneToMany(setter, mappingConfiguration, null);
	}
	
	@Override
	public <O, J, S extends Collection<O>, T extends Table> FluentMappingBuilderOneToManyOptions<C, I, O, S> mapOneToMany(
			SerializableBiConsumer<C, S> setter,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		
		MutatorByMethodReference<C, S> setterReference = Accessors.mutatorByMethodReference(setter);
		PropertyAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				Accessors.accessor(setterReference.getDeclaringClass(), propertyName(setterReference.getMethodName())),
				setterReference
		);
		return mapOneToMany(propertyAccessor, setterReference, mappingConfiguration, table);
	}
	
	private <O, J, S extends Collection<O>> FluentMappingBuilderOneToManyOptions<C, I, O, S> mapOneToMany(
			ReversibleAccessor<C, S> propertyAccessor,
			ValueAccessPointByMethodReference<C> methodReference,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration,
			@javax.annotation.Nullable Table table) {
		OneToManyRelation<C, O, J, S> oneToManyRelation = new OneToManyRelation<>(
				propertyAccessor,
				methodReference,
				() -> polymorphismPolicy instanceof PolymorphismPolicy.TablePerClassPolymorphism,
				mappingConfiguration,
				table);
		this.oneToManyRelations.add(oneToManyRelation);
		return new MethodDispatcher()
				.redirect(OneToManyOptions.class, new OneToManyOptionsSupport<>(oneToManyRelation), true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderOneToManyOptions<C, I, O, S>>) (Class) FluentMappingBuilderOneToManyOptions.class);
	}
	
	@Override
	public <O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableFunction<C, O> getter,
																									 EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder) {
		return embed(propertiesMappingConfigurationDelegate.embed(getter, embeddableMappingBuilder));
	}
	
	@Override
	public <O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableBiConsumer<C, O> setter,
																									 EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder) {
		return embed(propertiesMappingConfigurationDelegate.embed(setter, embeddableMappingBuilder));
	}
	
	@Override
	public FluentSubEntityMappingBuilder<C, I> mapPolymorphism(PolymorphismPolicy<C> polymorphismPolicy) {
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
						propertiesMappingConfigurationDelegate.currentInset().override(setter, targetColumn);
						return null;
					}
					
					@Override
					public ImportedEmbedWithColumnOptions override(SerializableFunction getter, Column targetColumn) {
						propertiesMappingConfigurationDelegate.currentInset().override(getter, targetColumn);
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
	public FluentSubEntityMappingBuilder<C, I> withColumnNaming(ColumnNamingStrategy columnNamingStrategy) {
		this.propertiesMappingConfigurationDelegate.withColumnNaming(columnNamingStrategy);
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
		
		private <O> FluentSubEntityMappingBuilderPropertyOptions<C, I, O> wrapForAdditionalOptions(LinkageSupport<C, O> newMapping) {
			return new MethodDispatcher()
					.redirect(PropertyOptions.class, new PropertyOptions<O>() {
						@Override
						public PropertyOptions<O> mandatory() {
							newMapping.setNullable(false);
							return null;
						}
						
						@Override
						public PropertyOptions<O> setByConstructor() {
							newMapping.setByConstructor();
							return null;
						}
						
						@Override
						public PropertyOptions<O> readonly() {
							newMapping.readonly();
							return null;
						}
						
						@Override
						public PropertyOptions<O> columnName(String name) {
							newMapping.setColumnOptions(new ColumnLinkageOptionsByName(name));
							return null;
						}
						
						@Override
						public PropertyOptions<O> column(Column column) {
							newMapping.setColumnOptions(new ColumnLinkageOptionsByColumn(column));
							return null;
						}
						
						@Override
						public PropertyOptions<O> fieldName(String name) {
							Field field = Reflections.findField(SubEntityDecoratedEmbeddableConfigurationSupport.this.entityConfigurationSupport.classToPersist, name);
							if (field == null) {
								throw new MappingConfigurationException(("Field " + name
										+ " was not found in " + Reflections.toString(SubEntityDecoratedEmbeddableConfigurationSupport.this.entityConfigurationSupport.classToPersist)));
							}
							newMapping.setField(field);
							return null;
						}
						
						@Override
						public PropertyOptions<O> readConverter(Converter<O, O> converter) {
							newMapping.setReadConverter(converter);
							return null;
						}
						
						@Override
						public PropertyOptions<O> writeConverter(Converter<O, O> converter) {
							newMapping.setWriteConverter(converter);
							return null;
						}
						
						@Override
						public <V> PropertyOptions<O> sqlBinder(ParameterBinder<V> parameterBinder) {
							newMapping.setParameterBinder(parameterBinder);
							return null;
						}
					}, true)
					.fallbackOn(entityConfigurationSupport)
					.build((Class<FluentSubEntityMappingBuilderPropertyOptions<C, I, O>>) (Class) FluentSubEntityMappingBuilderPropertyOptions.class);
		}
	}
	
}
