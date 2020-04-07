package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Reflections;
import org.gama.lang.reflect.MethodDispatcher;
import org.gama.reflection.AccessorByMethod;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.AccessorDefinition;
import org.gama.reflection.Accessors;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.MutatorByMethod;
import org.gama.reflection.MutatorByMethodReference;
import org.gama.reflection.PropertyAccessor;
import org.gama.reflection.ValueAccessPointByMethodReference;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.AbstractLinkage;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupport.OneToManyOptionsSupport;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupport.OverridableColumnInset;
import org.gama.stalactite.persistence.engine.IFluentEmbeddableMappingBuilder.IFluentEmbeddableMappingBuilderEmbedOptions;
import org.gama.stalactite.persistence.engine.IFluentEmbeddableMappingBuilder.IFluentEmbeddableMappingBuilderEnumOptions;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.builder.CascadeManyList;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.Reflections.propertyName;
import static org.gama.reflection.AccessorDefinition.giveDefinition;

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
	
	private final SubEntityDecoratedEmbeddableConfigurationSupport<C, I> propertiesMappingConfigurationSurrogate;
	
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
		return this.propertiesMappingConfigurationSurrogate.wrapForAdditionalOptions(mapping);
	}
	
	@Override
	public <O> IFluentMappingBuilderPropertyOptions<C, I> add(SerializableFunction<C, O> getter, String columnName) {
		AbstractLinkage<C> mapping = propertiesMappingConfigurationSurrogate.addMapping(getter, columnName);
		return this.propertiesMappingConfigurationSurrogate.wrapForAdditionalOptions(mapping);
	}
	
	@Override
	public <O> IFluentMappingBuilderPropertyOptions<C, I> add(SerializableBiConsumer<C, O> setter, Column<? extends Table, O> column) {
		AbstractLinkage<C> mapping = propertiesMappingConfigurationSurrogate.addMapping(setter, column);
		return this.propertiesMappingConfigurationSurrogate.wrapForAdditionalOptions(mapping);
	}
	
	@Override
	public <O> IFluentMappingBuilderPropertyOptions<C, I> add(SerializableFunction<C, O> getter, Column<? extends Table, O> column) {
		AbstractLinkage<C> mapping = propertiesMappingConfigurationSurrogate.addMapping(getter, column);
		return this.propertiesMappingConfigurationSurrogate.wrapForAdditionalOptions(mapping);
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
	public <E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter, Column<? extends Table, E> column) {
		AbstractLinkage<C> linkage = propertiesMappingConfigurationSurrogate.addMapping(setter, column);
		return handleEnumOptions(propertiesMappingConfigurationSurrogate.addEnumOptions(linkage));
	}
	
	@Override
	public <E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter, Column<? extends Table, E> column) {
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
		// we declare the column on our side
		propertiesMappingConfigurationSurrogate.addMapping(propertyAccessor, giveDefinition(mutatorByMethodReference), null);
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
		// we declare the column on our side
		propertiesMappingConfigurationSurrogate.addMapping(propertyAccessor, giveDefinition(accessorByMethodReference), null);
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
	public <O> IFluentMappingBuilderEmbedOptions<C, I, O> embed(SerializableBiConsumer<C, O> setter) {
		return embed(propertiesMappingConfigurationSurrogate.embed(setter));
	}
	
	@Override
	public <O> IFluentMappingBuilderEmbedOptions<C, I, O> embed(SerializableFunction<C, O> getter) {
		return embed(propertiesMappingConfigurationSurrogate.embed(getter));
	}
	
	@Override
	public <O> IFluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableFunction<C, O> getter,
																									  EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder) {
		return null;
	}
	
	@Override
	public <O> IFluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableBiConsumer<C, O> setter,
																									  EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder) {
		return null;
	}
	
	private <O> IFluentMappingBuilderEmbedOptions<C, I, O> embed(IFluentEmbeddableMappingBuilderEmbedOptions<C, O> embedSupport) {
		return new MethodDispatcher()
				.redirect(EmbedWithColumnOptions.class, new EmbedWithColumnOptions() {
					
					@Override
					public EmbedWithColumnOptions innerEmbed(SerializableBiConsumer setter) {
						embedSupport.innerEmbed(setter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedWithColumnOptions innerEmbed(SerializableFunction getter) {
						embedSupport.innerEmbed(getter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedWithColumnOptions overrideName(SerializableFunction getter, String columnName) {
						embedSupport.overrideName(getter, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedWithColumnOptions overrideName(SerializableBiConsumer setter, String columnName) {
						embedSupport.overrideName(setter, columnName);
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
						((OverridableColumnInset) propertiesMappingConfigurationSurrogate.currentInset()).override(function, targetColumn);
						return null;	// we can return null because dispatcher will return proxy
					}
				}, true)
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderEmbedOptions<C, I, O>>) (Class) IFluentMappingBuilderEmbedOptions.class);
	}
	
	@Override
	public IFluentSubEntityMappingConfiguration<C, I> columnNamingStrategy(ColumnNamingStrategy columnNamingStrategy) {
		this.propertiesMappingConfigurationSurrogate.columnNamingStrategy(columnNamingStrategy);
		return this;
	}
	
	/**
	 * Class very close to {@link FluentEmbeddableMappingConfigurationSupport}, but with dedicated methods to sub-entity mapping
	 */
	static class SubEntityDecoratedEmbeddableConfigurationSupport<C, I> extends FluentEmbeddableMappingConfigurationSupport<C> {
		
		private final FluentSubEntityMappingConfigurationSupport<C, I> entityConfigurationSupport;
		private OverridableColumnInset<C, ?> currentInset;
		
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
		protected <O> Inset<C, O> newInset(SerializableBiConsumer<C, O> setter) {
			this.currentInset = new OverridableColumnInset<>(setter, this);
			return (Inset<C, O>) currentInset;
		}
		
		@Override
		protected <O> Inset<C, O> newInset(SerializableFunction<C, O> getter) {
			this.currentInset = new OverridableColumnInset<>(getter, this);
			return (Inset<C, O>) currentInset;
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
		
		private IFluentMappingBuilderPropertyOptions<C, I> wrapForAdditionalOptions(AbstractLinkage<C> newMapping) {
			return new MethodDispatcher()
					.redirect(PropertyOptions.class, () -> {
						newMapping.setNullable(false);
						return null;
					}, true)
					.fallbackOn(entityConfigurationSupport)
					.build((Class<IFluentMappingBuilderPropertyOptions<C, I>>) (Class) IFluentMappingBuilderPropertyOptions.class);
		}
	}
	
}
