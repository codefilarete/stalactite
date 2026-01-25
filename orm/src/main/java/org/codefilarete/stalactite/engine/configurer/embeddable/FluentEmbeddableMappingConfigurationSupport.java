package org.codefilarete.stalactite.engine.configurer.embeddable;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nullable;

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
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilder;
import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilderManyToManyOptions;
import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilderManyToOneOptions;
import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilderOneToManyOptions;
import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilderOneToOneOptions;
import org.codefilarete.stalactite.dsl.embeddable.ImportedEmbedOptions;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.UniqueConstraintNamingStrategy;
import org.codefilarete.stalactite.dsl.property.ElementCollectionOptions;
import org.codefilarete.stalactite.dsl.property.EmbeddableCollectionOptions;
import org.codefilarete.stalactite.dsl.property.EnumOptions;
import org.codefilarete.stalactite.dsl.property.PropertyOptions;
import org.codefilarete.stalactite.dsl.relation.ManyToManyOptions;
import org.codefilarete.stalactite.dsl.relation.ManyToOneOptions;
import org.codefilarete.stalactite.dsl.relation.OneToManyOptions;
import org.codefilarete.stalactite.dsl.relation.OneToOneOptions;
import org.codefilarete.stalactite.engine.configurer.LambdaMethodUnsheller;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.entity.ManyToManyOptionsSupport;
import org.codefilarete.stalactite.engine.configurer.manyToOne.ManyToOneRelation;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.map.MapRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.property.ColumnLinkageOptionsByColumn;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry.EnumBindType;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.reflect.MethodDispatcher;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static org.codefilarete.tool.Reflections.propertyName;

/**
 * @author Guillaume Mary
 */
public class FluentEmbeddableMappingConfigurationSupport<C> implements FluentEmbeddableMappingBuilder<C>, LambdaMethodUnsheller,
		EmbeddableMappingConfiguration<C> {
	
	@Nullable
	private EmbeddableMappingConfigurationProvider<? super C> superMappingBuilder;
	
	/** Owning class of mapped properties */
	private final Class<C> classToPersist;
	
	private final List<OneToOneRelation<C, ?, ?>> oneToOneRelations = new ArrayList<>();
	
	private final List<OneToManyRelation<C, ?, ?, ?>> oneToManyRelations = new ArrayList<>();
	
	private final List<ManyToManyRelation<C, ?, ?, ?, ?>> manyToManyRelations = new ArrayList<>();
	
	private final List<ManyToOneRelation<C, ?, ?, ?>> manyToOneRelations = new ArrayList<>();
	
	private final List<ElementCollectionRelation<C, ?, ? extends Collection>> elementCollections = new ArrayList<>();
	
	@Nullable
	private ColumnNamingStrategy columnNamingStrategy;
	
	@Nullable
	private UniqueConstraintNamingStrategy uniqueConstraintNamingStrategy;

	/** Mapping definitions */
	protected final List<Linkage> mapping = new ArrayList<>();
	
	/** Collection of embedded elements, even inner ones to help final build process */
	private final Collection<Inset<C, Object>> insets = new ArrayList<>();
	
	/** Last embedded element, introduced to help inner embedding registration (kind of algorithm help). Has no purpose in whole mapping configuration. */
	private Inset<C, ?> currentInset;
	
	/** Helper to unshell method references */
	private final MethodReferenceCapturer methodSpy;
	
	/**
	 * Creates a builder to map the given class for persistence
	 *
	 * @param classToPersist the class to create a mapping for
	 */
	public FluentEmbeddableMappingConfigurationSupport(Class<C> classToPersist) {
		this.classToPersist = classToPersist;
		
		// Helper to capture Method behind method reference
		this.methodSpy = new MethodReferenceCapturer();
	}
	
	@Override
	public Class<C> getBeanType() {
		return classToPersist;
	}
	
	@Override
	public Collection<Inset<C, Object>> getInsets() {
		return insets;
	}
	
	@Override
	@Nullable
	public EmbeddableMappingConfiguration<? super C> getMappedSuperClassConfiguration() {
		return superMappingBuilder == null ? null : superMappingBuilder.getConfiguration();
	}
	
	@Override
	@Nullable
	public ColumnNamingStrategy getColumnNamingStrategy() {
		return columnNamingStrategy;
	}
	
	@Nullable
	@Override
	public UniqueConstraintNamingStrategy getUniqueConstraintNamingStrategy() {
		return uniqueConstraintNamingStrategy;
	}

	@Override
	public List<Linkage> getPropertiesMapping() {
		return mapping;
	}
	
	@Override
	public EmbeddableMappingConfiguration<C> getConfiguration() {
		return this;
	}
	
	@Override
	public Method captureLambdaMethod(SerializableFunction getter) {
		return this.methodSpy.findMethod(getter);
	}
	
	@Override
	public Method captureLambdaMethod(SerializableBiConsumer setter) {
		return this.methodSpy.findMethod(setter);
	}
	
	@Override
	public <O, J> FluentEmbeddableMappingBuilderOneToOneOptions<C, O> mapOneToOne(SerializableFunction<C, O> getter,
																				  EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration) {
		// we keep close to user demand: we keep its method reference ...
		AccessorByMethodReference<C, O> accessorByMethodReference = Accessors.accessorByMethodReference(getter);
		// ... but we can't do it for mutator, so we use the most equivalent manner: a mutator based on getter method (fallback to property if not present)
		Mutator<C, O> mutator = new AccessorByMethod<C, O>(captureLambdaMethod(getter)).toMutator();
		return mapOneToOne(accessorByMethodReference, mutator, mappingConfiguration);
	}
	
	@Override
	public <O, J> FluentEmbeddableMappingBuilderOneToOneOptions<C, O> mapOneToOne(SerializableBiConsumer<C, O> setter,
																				  EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration) {
		// we keep close to user demand: we keep its method reference ...
		Mutator<C, O> mutatorByMethodReference = Accessors.mutatorByMethodReference(setter);
		// ... but we can't do it for accessor, so we use the most equivalent manner: an accessor based on setter method (fallback to property if not present)
		Accessor<C, O> accessor = new MutatorByMethod<C, O>(captureLambdaMethod(setter)).toAccessor();
		return mapOneToOne(accessor, mutatorByMethodReference, mappingConfiguration);
	}
	
	private <O, J> FluentEmbeddableMappingBuilderOneToOneOptions<C, O> mapOneToOne(
			Accessor<C, O> accessor,
			Mutator<C, O> mutator,
			EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration) {
		OneToOneRelation<C, O, J> oneToOneRelation = new OneToOneRelation<>(
				new PropertyAccessor<>(accessor, mutator),
				() -> false,
				mappingConfiguration);
		this.oneToOneRelations.add((OneToOneRelation<C, Object, Object>) oneToOneRelation);
		return wrapForAdditionalOptions(oneToOneRelation);
	}
	
	private <O, J> FluentEmbeddableMappingBuilderOneToOneOptions<C, O> wrapForAdditionalOptions(OneToOneRelation<C, O, J> oneToOneRelation) {
		// then we return an object that allows fluent settings over our OneToOne cascade instance
		return new MethodDispatcher()
				.redirect(OneToOneOptions.class, new OneToOneOptions<C, O>() {
					@Override
					public OneToOneOptions<C, O> cascading(RelationMode relationMode) {
						oneToOneRelation.setRelationMode(relationMode);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions<C, O> mandatory() {
						oneToOneRelation.setNullable(false);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions<C, O> mappedBy(SerializableFunction<? super O, C> reverseLink) {
						oneToOneRelation.setReverseGetter(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions<C, O> mappedBy(SerializableBiConsumer<? super O, C> reverseLink) {
						oneToOneRelation.setReverseSetter(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions<C, O> reverseJoinColumn(String reverseColumnName) {
						oneToOneRelation.setReverseColumn(reverseColumnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions<C, O> fetchSeparately() {
						oneToOneRelation.fetchSeparately();
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions<C, O> columnName(String columnName) {
						oneToOneRelation.setColumnName(columnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions<C, O> unique() {
						oneToOneRelation.setUnique(true);
						return null;	// we can return null because dispatcher will return proxy
					}
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<FluentEmbeddableMappingBuilderOneToOneOptions<C, O>>) (Class) FluentEmbeddableMappingBuilderOneToOneOptions.class);
	}
	
	@Override
	public <O, J, S extends Collection<O>> FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> mapOneToMany(
			SerializableFunction<C, S> getter,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration) {

		AccessorByMethodReference<C, S> getterReference = Accessors.accessorByMethodReference(getter);
		ReversibleAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				getterReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<C, S>(captureLambdaMethod(getter)).toMutator());
		return mapOneToMany(propertyAccessor, mappingConfiguration);
	}
	
	@Override
	public <O, J, S extends Collection<O>> FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> mapOneToMany(
			SerializableBiConsumer<C, S> setter,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration) {
		
		MutatorByMethodReference<C, S> setterReference = Accessors.mutatorByMethodReference(setter);
		PropertyAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				Accessors.accessor(setterReference.getDeclaringClass(), propertyName(setterReference.getMethodName())),
				setterReference
		);
		return mapOneToMany(propertyAccessor, mappingConfiguration);
	}
	
	private <O, J, S extends Collection<O>> FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> mapOneToMany(
			ReversibleAccessor<C, S> propertyAccessor,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration) {
		OneToManyRelation<C, O, J, S> oneToManyRelation = new OneToManyRelation<>(
				propertyAccessor,
				() -> false,
				mappingConfiguration);
		this.oneToManyRelations.add(oneToManyRelation);
		return new MethodDispatcher()
				.redirect(OneToManyOptions.class, new OneToManyOptionsSupport<>(oneToManyRelation), true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S>>) (Class) FluentEmbeddableMappingBuilderOneToManyOptions.class);
	}
	
	@Override
	public <O, J, S extends Collection<C>>
	FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S>
	mapManyToOne(SerializableBiConsumer<C, O> setter,
				 EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration) {
		// we keep close to user demand: we keep its method reference ...
		Mutator<C, O> mutatorByMethodReference = Accessors.mutatorByMethodReference(setter);
		// ... but we can't do it for accessor, so we use the most equivalent manner: an accessor based on setter method (fallback to property if not present)
		Accessor<C, O> accessor = new MutatorByMethod<C, O>(captureLambdaMethod(setter)).toAccessor();
		return mapManyToOne(accessor, mutatorByMethodReference, mappingConfiguration);
	}
	
	@Override
	public <O, J, S extends Collection<C>>
	FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S>
	mapManyToOne(SerializableFunction<C, O> getter,
				 EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration) {
		// we keep close to user demand: we keep its method reference ...
		AccessorByMethodReference<C, O> accessorByMethodReference = Accessors.accessorByMethodReference(getter);
		// ... but we can't do it for mutator, so we use the most equivalent manner: a mutator based on getter method (fallback to property if not present)
		Mutator<C, O> mutator = new AccessorByMethod<C, O>(captureLambdaMethod(getter)).toMutator();
		return mapManyToOne(accessorByMethodReference, mutator, mappingConfiguration);
	}
	
	private <O, J, S extends Collection<C>>
	FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S> mapManyToOne(
			Accessor<C, O> accessor,
			Mutator<C, O> mutator,
			EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration) {
		ManyToOneRelation<C, O, J, S> manyToOneRelation = new ManyToOneRelation<>(
				new PropertyAccessor<>(accessor, mutator),
				() -> false,
				mappingConfiguration);
		this.manyToOneRelations.add(manyToOneRelation);
		return wrapForAdditionalOptions(manyToOneRelation);
	}
	
	private <O, J, S extends Collection<C>>
	FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S> wrapForAdditionalOptions(ManyToOneRelation<C, O, J, S> manyToOneRelation) {
		// then we return an object that allows fluent settings over our OneToOne cascade instance
		return new MethodDispatcher()
				.redirect(ManyToOneOptions.class, new ManyToOneOptions<C, O, S>() {
					@Override
					public ManyToOneOptions<C, O, S> cascading(RelationMode relationMode) {
						manyToOneRelation.setRelationMode(relationMode);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ManyToOneOptions<C, O, S> mandatory() {
						manyToOneRelation.setNullable(false);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ManyToOneOptions<C, O, S> reverselySetBy(SerializableBiConsumer<O, C> reverseLink) {
						manyToOneRelation.getMappedByConfiguration().setCombiner(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ManyToOneOptions<C, O, S> reverseCollection(SerializableFunction<O, S> collectionAccessor) {
						manyToOneRelation.getMappedByConfiguration().setAccessor(collectionAccessor);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ManyToOneOptions<C, O, S> reverseCollection(SerializableBiConsumer<O, S> collectionMutator) {
						manyToOneRelation.getMappedByConfiguration().setMutator(collectionMutator);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ManyToOneOptions<C, O, S> reverselyInitializeWith(Supplier<S> collectionFactory) {
						manyToOneRelation.getMappedByConfiguration().setFactory(collectionFactory);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ManyToOneOptions<C, O, S> fetchSeparately() {
						manyToOneRelation.fetchSeparately();
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ManyToOneOptions<C, O, S> columnName(String columnName) {
						manyToOneRelation.setColumnName(columnName);
						return null;
					}
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S>>) (Class) FluentEmbeddableMappingBuilderManyToOneOptions.class);
	}
	
	@Override
	public <O, J, S1 extends Collection<O>, S2 extends Collection<C>> FluentEmbeddableMappingBuilderManyToManyOptions<C, O, S1, S2> mapManyToMany(SerializableFunction<C, S1> getter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration) {
		AccessorByMethodReference<C, S1> getterReference = Accessors.accessorByMethodReference(getter);
		ReversibleAccessor<C, S1> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				getterReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<C, S1>(captureLambdaMethod(getter)).toMutator());
		return mapManyToMany(propertyAccessor, mappingConfiguration);
	}
	
	@Override
	public <O, J, S1 extends Collection<O>, S2 extends Collection<C>> FluentEmbeddableMappingBuilderManyToManyOptions<C, O, S1, S2> mapManyToMany(SerializableBiConsumer<C, S1> setter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration) {
		MutatorByMethodReference<C, S1> setterReference = Accessors.mutatorByMethodReference(setter);
		PropertyAccessor<C, S1> propertyAccessor = new PropertyAccessor<>(
				Accessors.accessor(setterReference.getDeclaringClass(), propertyName(setterReference.getMethodName())),
				setterReference
		);
		return mapManyToMany(propertyAccessor, mappingConfiguration);
	}
	
	private <O, J, S1 extends Collection<O>, S2 extends Collection<C>> FluentEmbeddableMappingBuilderManyToManyOptions<C, O, S1, S2> mapManyToMany(
			ReversibleAccessor<C, S1> propertyAccessor,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration) {
		ManyToManyRelation<C, O, J, S1, S2> manyToManyRelation = new ManyToManyRelation<>(
				propertyAccessor,
				() -> false,
				mappingConfiguration);
		this.manyToManyRelations.add(manyToManyRelation);
		return new MethodDispatcher()
				.redirect(ManyToManyOptions.class, new ManyToManyOptionsSupport<>(manyToManyRelation), true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<FluentEmbeddableMappingBuilderManyToManyOptions<C, O, S1, S2>>) (Class) FluentEmbeddableMappingBuilderManyToManyOptions.class);
	}
	
	@Override
	public FluentEmbeddableMappingConfigurationSupport<C> withColumnNaming(ColumnNamingStrategy columnNamingStrategy) {
		this.columnNamingStrategy = columnNamingStrategy;
		return this;
	}
	
	@Override
	public FluentEmbeddableMappingBuilder<C> withUniqueConstraintNaming(UniqueConstraintNamingStrategy uniqueConstraintNamingStrategy) {
		this.uniqueConstraintNamingStrategy = uniqueConstraintNamingStrategy;
		return this;
	}

	/**
	 * Gives access to currently configured {@link Inset}. Made so one can access features of {@link Inset} which are wider than
	 * the one available through {@link FluentEmbeddableMappingBuilder}.
	 * 
	 * @return the last {@link Inset} built by {@link #newInset(SerializableFunction, EmbeddableMappingConfigurationProvider)}
	 * or {@link #newInset(SerializableBiConsumer, EmbeddableMappingConfigurationProvider)}
	 */
	public Inset<C, ?> currentInset() {
		return currentInset;
	}
	
	protected <O> Inset<C, O> newInset(SerializableFunction<C, O> getter, EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder) {
		currentInset = Inset.fromGetter(getter, embeddableMappingBuilder, this);
		return (Inset<C, O>) currentInset;
	}
	
	protected <O> Inset<C, O> newInset(SerializableBiConsumer<C, O> setter, EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder) {
		currentInset = Inset.fromSetter(setter, embeddableMappingBuilder, this);
		return (Inset<C, O>) currentInset;
	}
	
	@Override
	public <O> FluentEmbeddableMappingBuilderPropertyOptions<C, O> map(SerializableBiConsumer<C, O> setter) {
		LinkageSupport<C, O> linkage = addLinkage(new LinkageSupport<>(setter));
		return wrapWithPropertyOptions(linkage);
	}
	
	@Override
	public <O> FluentEmbeddableMappingBuilderPropertyOptions<C, O> map(SerializableFunction<C, O> getter) {
		LinkageSupport<C, O> linkage = addLinkage(new LinkageSupport<>(getter));
		return wrapWithPropertyOptions(linkage);
	}
	
	public <O> LinkageSupport<C, O> addLinkage(LinkageSupport<C, O> linkage) {
		this.mapping.add(linkage);
		return linkage;
	}
	
	<O> FluentEmbeddableMappingBuilderPropertyOptions<C, O> wrapWithPropertyOptions(LinkageSupport<C, O> linkage) {
		return new MethodReferenceDispatcher()
				.redirect(PropertyOptions.class, new PropertyOptions<O>() {
					@Override
					public PropertyOptions<O> mandatory() {
						linkage.setNullable(false);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public PropertyOptions<O> unique() {
						linkage.setUnique(true);
						return null;
					}

					@Override
					public PropertyOptions<O> setByConstructor() {
						linkage.setByConstructor();
						return null;
					}
					
					@Override
					public PropertyOptions<O> readonly() {
						linkage.readonly();
						return null;
					}
					
					@Override
					public PropertyOptions<O> columnName(String name) {
						linkage.getColumnOptions().setColumnName(name);
						return null;
					}
					
					@Override
					public PropertyOptions<O> columnSize(Size size) {
						linkage.getColumnOptions().setColumnSize(size);
						return null;
					}
					
					@Override
					public PropertyOptions<O> column(Column<? extends Table, ? extends O> column) {
						linkage.setColumnOptions(new ColumnLinkageOptionsByColumn(column));
						return null;
					}
					
					@Override
					public PropertyOptions<O> fieldName(String name) {
						linkage.setField(FluentEmbeddableMappingConfigurationSupport.this.classToPersist, name);
						return null;
					}
					
					@Override
					public <X> PropertyOptions<O> readConverter(Converter<X, O> converter) {
						linkage.setReadConverter(converter);
						return null;
					}
					
					@Override
					public <X> PropertyOptions<O> writeConverter(Converter<O, X> converter) {
						linkage.setWriteConverter(converter);
						return null;
					}
					
					@Override
					public <V> PropertyOptions<O> sqlBinder(ParameterBinder<V> parameterBinder) {
						linkage.setParameterBinder(parameterBinder);
						return null;
					}
				}, true)
				.redirect((SerializableFunction<FluentEmbeddableMappingConfigurationPropertyOptions<C, O>, FluentEmbeddableMappingConfigurationPropertyOptions<C, O>>)
						FluentEmbeddableMappingConfigurationPropertyOptions::nullable, () -> linkage.setNullable(true))
				.fallbackOn(this)
				.build((Class<FluentEmbeddableMappingBuilderPropertyOptions<C, O>>) (Class) FluentEmbeddableMappingBuilderPropertyOptions.class);
	}
	
	@Override
	public <E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C, E> mapEnum(SerializableBiConsumer<C, E> setter) {
		LinkageSupport<C, E> linkage = new LinkageSupport<>(setter);
		this.mapping.add(linkage);
		return wrapWithEnumOptions(linkage);
	}
	
	@Override
	public <E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C, E> mapEnum(SerializableFunction<C, E> getter) {
		LinkageSupport<C, E> linkage = new LinkageSupport<>(getter);
		this.mapping.add(linkage);
		return wrapWithEnumOptions(linkage);
	}
	
	public <E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C, E> wrapWithEnumOptions(LinkageSupport<C, E> linkage) {
		return new MethodReferenceDispatcher()
				.redirect(EnumOptions.class, new EnumOptions<E>() {
					
					@Override
					public EnumOptions<E> byName() {
						linkage.setEnumBindType(EnumBindType.NAME);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EnumOptions<E> byOrdinal() {
						linkage.setEnumBindType(EnumBindType.ORDINAL);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EnumOptions<E> mandatory() {
						linkage.setNullable(false);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EnumOptions<E> unique() {
						linkage.setUnique(true);
						return null;
					}
					
					@Override
					public EnumOptions<E> setByConstructor() {
						linkage.setByConstructor();
						return null;
					}
					
					@Override
					public EnumOptions<E> readonly() {
						linkage.readonly();
						return null;
					}
					
					@Override
					public EnumOptions<E> columnName(String name) {
						linkage.getColumnOptions().setColumnName(name);
						return null;
					}
					
					@Override
					public EnumOptions<E> columnSize(Size size) {
						linkage.getColumnOptions().setColumnSize(size);
						return null;
					}
					
					@Override
					public EnumOptions<E> column(Column<? extends Table, ? extends E> column) {
						linkage.setColumnOptions(new ColumnLinkageOptionsByColumn(column));
						return null;
					}
					
					@Override
					public EnumOptions<E> fieldName(String name) {
						linkage.setField(FluentEmbeddableMappingConfigurationSupport.this.classToPersist, name);
						return null;
					}
					
					@Override
					public <X> EnumOptions<E> readConverter(Converter<X, E> converter) {
						linkage.setReadConverter(converter);
						return null;
					}
					
					@Override
					public <X> EnumOptions<E> writeConverter(Converter<E, X> converter) {
						linkage.setWriteConverter(converter);
						return null;
					}
					
					@Override
					public <V> PropertyOptions<E> sqlBinder(ParameterBinder<V> parameterBinder) {
						linkage.setParameterBinder(parameterBinder);
						return null;
					}
				}, true)
				.fallbackOn(this)
				.build((Class<FluentEmbeddableMappingBuilderEnumOptions<C, E>>) (Class) FluentEmbeddableMappingBuilderEnumOptions.class);
	}
	
	@Override
	public <O, S extends Collection<O>> FluentEmbeddableMappingConfigurationElementCollectionOptions<C, O, S> mapCollection(SerializableFunction<C, S> getter,
																											   Class<O> componentType) {
		ElementCollectionRelation<C, O, S> elementCollectionRelation = new ElementCollectionRelation<>(getter, componentType, this, null);
		elementCollections.add(elementCollectionRelation);
		return wrapWithElementCollectionOptions(elementCollectionRelation);
	}
	
	@Override
	public <O, S extends Collection<O>> FluentEmbeddableMappingConfigurationElementCollectionOptions<C, O, S> mapCollection(SerializableBiConsumer<C, S> setter,
																											   Class<O> componentType) {
		ElementCollectionRelation<C, O, S> elementCollectionRelation = new ElementCollectionRelation<>(setter, componentType, null);
		elementCollections.add(elementCollectionRelation);
		return wrapWithElementCollectionOptions(elementCollectionRelation);
	}
	
	private <O, S extends Collection<O>> FluentEmbeddableMappingConfigurationElementCollectionOptions<C, O, S> wrapWithElementCollectionOptions(
			ElementCollectionRelation<C, O, S> elementCollectionRelation) {
		return new MethodReferenceDispatcher()
				.redirect(ElementCollectionOptions.class, wrapAsOptions(elementCollectionRelation), true)
				.fallbackOn(this)
				.build((Class<FluentEmbeddableMappingConfigurationElementCollectionOptions<C, O, S>>) (Class) FluentEmbeddableMappingConfigurationElementCollectionOptions.class);
	}
	
	@Override
	public <O, S extends Collection<O>> FluentEmbeddableMappingConfigurationElementCollectionImportEmbedOptions<C, O, S> mapCollection(SerializableFunction<C, S> getter,
																														  Class<O> componentType,
																														  EmbeddableMappingConfigurationProvider<O> embeddableConfiguration) {
		ElementCollectionRelation<C, O, S> elementCollectionRelation = new ElementCollectionRelation<>(getter, componentType, this, embeddableConfiguration);
		elementCollections.add(elementCollectionRelation);
		return wrapWithElementCollectionImportOptions(elementCollectionRelation);
	}
	
	@Override
	public <O, S extends Collection<O>> FluentEmbeddableMappingConfigurationElementCollectionImportEmbedOptions<C, O, S> mapCollection(SerializableBiConsumer<C, S> setter,
																														  Class<O> componentType,
																														  EmbeddableMappingConfigurationProvider<O> embeddableConfiguration) {
		ElementCollectionRelation<C, O, S> elementCollectionRelation = new ElementCollectionRelation<>(setter, componentType, embeddableConfiguration);
		elementCollections.add(elementCollectionRelation);
		return wrapWithElementCollectionImportOptions(elementCollectionRelation);
	}
	
	private <O, S extends Collection<O>> FluentEmbeddableMappingConfigurationElementCollectionImportEmbedOptions<C, O, S> wrapWithElementCollectionImportOptions(
			ElementCollectionRelation<C, O, S> elementCollectionRelation) {
		return new MethodReferenceDispatcher()
				.redirect(EmbeddableCollectionOptions.class, new EmbeddableCollectionOptions<C, O, S>() {
					
					@Override
					public <IN> EmbeddableCollectionOptions<C, O, S> overrideName(SerializableFunction<O, IN> getter, String columnName) {
						elementCollectionRelation.overrideName(getter, columnName);
						return null;
					}
					
					@Override
					public <IN> EmbeddableCollectionOptions<C, O, S> overrideName(SerializableBiConsumer<O, IN> setter, String columnName) {
						elementCollectionRelation.overrideName(setter, columnName);
						return null;
					}
					
					@Override
					public <IN> EmbeddableCollectionOptions<C, O, S> overrideSize(SerializableFunction<O, IN> getter, Size columnSize) {
						elementCollectionRelation.overrideSize(getter, columnSize);
						return null;
					}
					
					@Override
					public <IN> EmbeddableCollectionOptions<C, O, S> overrideSize(SerializableBiConsumer<O, IN> setter, Size columnSize) {
						elementCollectionRelation.overrideSize(setter, columnSize);
						return null;
					}
					
					@Override
					public EmbeddableCollectionOptions<C, O, S> initializeWith(Supplier<? extends S> collectionFactory) {
						elementCollectionRelation.setCollectionFactory(collectionFactory);
						return null;
					}
					
					@Override
					public EmbeddableCollectionOptions<C, O, S> reverseJoinColumn(String name) {
						elementCollectionRelation.setReverseColumnName(name);
						return null;
					}
					
					@Override
					public EmbeddableCollectionOptions<C, O, S> indexed() {
						elementCollectionRelation.ordered();
						return null;
					}
					
					@Override
					public EmbeddableCollectionOptions<C, O, S> indexedBy(String columnName) {
						elementCollectionRelation.setIndexingColumnName(columnName);
						return null;
					}
					
					@Override
					public EmbeddableCollectionOptions<C, O, S> onTable(Table table) {
						elementCollectionRelation.setTargetTable(table);
						return null;
					}
					
					@Override
					public EmbeddableCollectionOptions<C, O, S> onTable(String tableName) {
						elementCollectionRelation.setTargetTableName(tableName);
						return null;
					}
				}, true)
				.fallbackOn(this)
				.build((Class<FluentEmbeddableMappingConfigurationElementCollectionImportEmbedOptions<C, O, S>>) (Class) FluentEmbeddableMappingConfigurationElementCollectionImportEmbedOptions.class);
	}
	
	private <O, S extends Collection<O>> ElementCollectionOptions<C, O, S> wrapAsOptions(ElementCollectionRelation<C, O, S> elementCollectionRelation) {
		return new ElementCollectionOptions<C, O, S>() {
			
			@Override
			public ElementCollectionOptions<C, O, S> initializeWith(Supplier<? extends S> collectionFactory) {
				elementCollectionRelation.setCollectionFactory(collectionFactory);
				return null;
			}
			
			@Override
			public ElementCollectionOptions<C, O, S> elementColumnName(String columnName) {
				elementCollectionRelation.setElementColumnName(columnName);
				return null;
			}
			
			@Override
			public ElementCollectionOptions<C, O, S> elementColumnSize(Size columnSize) {
				elementCollectionRelation.setElementColumnSize(columnSize);
				return null;
			}
			
			@Override
			public FluentEmbeddableMappingConfigurationElementCollectionOptions<C, O, S> reverseJoinColumn(String name) {
				elementCollectionRelation.setReverseColumnName(name);
				return null;
			}
			
			@Override
			public ElementCollectionOptions<C, O, S> indexed() {
				elementCollectionRelation.ordered();
				return null;
			}
			
			@Override
			public ElementCollectionOptions<C, O, S> indexedBy(String columnName) {
				elementCollectionRelation.setIndexingColumnName(columnName);
				return null;
			}
			
			@Override
			public ElementCollectionOptions<C, O, S> onTable(Table table) {
				elementCollectionRelation.setTargetTable(table);
				return null;
			}
			
			@Override
			public ElementCollectionOptions<C, O, S> onTable(String tableName) {
				elementCollectionRelation.setTargetTableName(tableName);
				return null;
			}
		};
	}
	
	@Override
	public FluentEmbeddableMappingBuilder<C> mapSuperClass(EmbeddableMappingConfigurationProvider<? super C> superMappingConfiguration) {
		this.superMappingBuilder = superMappingConfiguration;
		return this;
	}
	
	@Override
	public <O> FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embed(SerializableFunction<C, O> getter,
																											EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder) {
		return addImportedInset(newInset(getter, embeddableMappingBuilder));
	}
	
	@Override
	public <O> FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embed(SerializableBiConsumer<C, O> setter,
																											EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder) {
		return addImportedInset(newInset(setter, embeddableMappingBuilder));
	}
	
	private <O> FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> addImportedInset(Inset<C, O> inset) {
		insets.add((Inset<C, Object>) inset);
		return new MethodReferenceDispatcher()
				.redirect(ImportedEmbedOptions.class, new ImportedEmbedOptions<C>() {

					@Override
					public <IN> ImportedEmbedOptions<C> overrideName(SerializableFunction<C, IN> getter, String columnName) {
						inset.overrideName(getter, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}

					@Override
					public <IN> ImportedEmbedOptions<C> overrideName(SerializableBiConsumer<C, IN> setter, String columnName) {
						inset.overrideName(setter, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public <IN> ImportedEmbedOptions<C> overrideSize(SerializableFunction<C, IN> getter, Size columnSize) {
						inset.overrideSize(getter, columnSize);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public <IN> ImportedEmbedOptions<C> overrideSize(SerializableBiConsumer<C, IN> setter, Size columnSize) {
						inset.overrideSize(setter, columnSize);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ImportedEmbedOptions exclude(SerializableBiConsumer setter) {
						inset.exclude(setter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ImportedEmbedOptions exclude(SerializableFunction getter) {
						inset.exclude(getter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
				}, true)
				.fallbackOn(this)
				.build((Class<FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O>>) (Class) FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions.class);
	}
	
	@Override
	public Class<C> getEntityType() {
		return classToPersist;
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
		return (List) manyToManyRelations;
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
}
