package org.gama.stalactite.persistence.engine;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Reflections;
import org.gama.lang.function.SerializableTriFunction;
import org.gama.lang.reflect.MethodDispatcher;
import org.gama.reflection.AccessorByMethod;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.AccessorChain;
import org.gama.reflection.Accessors;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.reflection.MutatorByMethod;
import org.gama.reflection.MutatorByMethodReference;
import org.gama.reflection.PropertyAccessor;
import org.gama.reflection.ValueAccessPointMap;
import org.gama.reflection.ValueAccessPointSet;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.ParameterBinderRegistry.EnumBindType;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public class FluentEmbeddableMappingConfigurationSupport<C> implements IFluentEmbeddableMappingBuilder<C>, LambdaMethodUnsheller {
	
	/**
	 * Starts a {@link IFluentEmbeddableMappingBuilder} for a given class which will target a table that as the class name.
	 *
	 * @param persistedClass the class to be persisted by the {@link EmbeddedBeanMappingStrategy} that will be created by {@link #buildMapping(Dialect, Table)}}
	 * @param <T> any type to be persisted
	 * @return a new {@link FluentEmbeddableMappingConfigurationSupport}
	 */
	public static <T extends Identified> IFluentEmbeddableMappingBuilder<T> from(Class<T> persistedClass) {
		return new FluentEmbeddableMappingConfigurationSupport<>(persistedClass);
	}
	
	/** Owning class of mapped properties */
	private final Class<C> persistedClass;
	
	private ColumnNamingStrategy columnNamingStrategy = ColumnNamingStrategy.DEFAULT;
	
	/** Mapiing definitions */
	final List<Linkage> mapping = new ArrayList<>();
	
	/** Collection of embedded elements, even inner ones to help final build process */
	private final Collection<AbstractInset<C, ?>> insets = new ArrayList<>();
	
	private EmbeddedBeanMappingStrategy<? super C, ?> mappedSuperClass;
	
	/** Last embedded element, introduced to help inner embedding registration (kind of algorithm help). Has no purpose in whole mapping configuration. */
	private Inset<C, ?> currentInset;
	
	/** Helper to unshell method references */
	private final MethodReferenceCapturer methodSpy;
	
	/**
	 * Creates a builder to map the given class for persistence
	 *
	 * @param persistedClass the class to create a mapping for
	 */
	public FluentEmbeddableMappingConfigurationSupport(Class<C> persistedClass) {
		this.persistedClass = persistedClass;
		
		// Helper to capture Method behind method reference
		this.methodSpy = new MethodReferenceCapturer();
	}
	
	Collection<AbstractInset<C, ?>> getInsets() {
		return insets;
	}
	
	EmbeddedBeanMappingStrategy<? super C, ?> getMappedSuperClass() {
		return mappedSuperClass;
	}
	
	ColumnNamingStrategy getColumnNamingStrategy() {
		return columnNamingStrategy;
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
	public FluentEmbeddableMappingConfigurationSupport<C> columnNamingStrategy(ColumnNamingStrategy columnNamingStrategy) {
		this.columnNamingStrategy = columnNamingStrategy;
		return this;
	}
	
	/**
	 *
	 * @return the last {@link Inset} built by {@link #newInset(SerializableFunction)} or {@link #newInset(SerializableBiConsumer)}
	 */
	protected Inset<C, ?> currentInset() {
		return currentInset;
	}
	
	protected <O> Inset<C, O> newInset(SerializableFunction<C, O> getter) {
		currentInset = new Inset<>(getter, this);
		return (Inset<C, O>) currentInset;
	}
	
	protected <O> Inset<C, O> newInset(SerializableBiConsumer<C, O> setter) {
		currentInset = new Inset<>(setter, this);
		return (Inset<C, O>) currentInset;
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilder<C> add(SerializableBiConsumer<C, O> setter) {
		Method method = captureLambdaMethod(setter);
		return add(method, null);
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilder<C> add(SerializableFunction<C, O> getter) {
		Method method = captureLambdaMethod(getter);
		return add(method, null);
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilder<C> add(SerializableBiConsumer<C, O> setter, String columnName) {
		Method method = captureLambdaMethod(setter);
		return add(method, columnName);
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilder<C> add(SerializableFunction<C, O> getter, String columnName) {
		Method method = captureLambdaMethod(getter);
		return add(method, columnName);
	}
	
	private IFluentEmbeddableMappingBuilder<C> add(Method method, @Nullable String columnName) {
		addMapping(method, columnName);
		return this;
	}
	
	Linkage<C> addMapping(Method method, @Nullable String columnName) {
		PropertyAccessor<Object, Object> propertyAccessor = Accessors.of(method);
		assertMappingIsNotAlreadyDefined(columnName, propertyAccessor);
		String linkName = columnName;
		if (columnName == null) {
			linkName = giveLinkName(method);
		}
		Linkage<C> linkage = newLinkage(method, linkName);
		this.mapping.add(linkage);
		return linkage;
	}
	
	protected String giveLinkName(Method method) {
		return columnNamingStrategy.giveName(method);
	}
	
	protected LinkageByColumnName<C> newLinkage(Method method, String linkName) {
		return new LinkageByColumnName<>(method, linkName);
	}
	
	protected void assertMappingIsNotAlreadyDefined(String columnName, PropertyAccessor propertyAccessor) {
		Predicate<Linkage> checker = ((Predicate<Linkage>) linkage -> {
			PropertyAccessor<C, ?> accessor = linkage.getAccessor();
			if (accessor.equals(propertyAccessor)) {
				throw new MappingConfigurationException("Mapping is already defined by method " + accessor.getAccessor());
			}
			return true;
		}).and(linkage -> {
			if (columnName != null && columnName.equals(linkage.getColumnName())) {
				throw new MappingConfigurationException("Mapping is already defined for column " + columnName);
			}
			return true;
		});
		mapping.forEach(checker::test);
	}
	
	@Override
	public <E extends Enum<E>> IFluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableBiConsumer<C, E> setter) {
		Method method = captureLambdaMethod(setter);
		return addEnum(method, null);
	}
	
	@Override
	public <E extends Enum<E>> IFluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableFunction<C, E> getter) {
		Method method = captureLambdaMethod(getter);
		return addEnum(method, null);
	}
	
	@Override
	public <E extends Enum<E>> IFluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableBiConsumer<C, E> setter, String columnName) {
		Method method = captureLambdaMethod(setter);
		return addEnum(method, columnName);
	}
	
	@Override
	public <E extends Enum<E>> IFluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableFunction<C, E> getter, String columnName) {
		Method method = captureLambdaMethod(getter);
		return addEnum(method, columnName);
	}
	
	IFluentEmbeddableMappingBuilderEnumOptions<C> addEnum(Method method, @Nullable String columnName) {
		Linkage<C> linkage = addMapping(method, columnName);
		return addEnumOptions(linkage);
	}
	
	IFluentEmbeddableMappingBuilderEnumOptions<C> addEnumOptions(Linkage<C> linkage) {
		linkage.setParameterBinder(EnumBindType.NAME.newParameterBinder((Class<Enum>) linkage.getColumnType()));
		return new MethodReferenceDispatcher()
				.redirect(EnumOptions.class, new EnumOptions() {
					
					@Override
					public EnumOptions byName() {
						setLinkageParameterBinder(EnumBindType.NAME);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EnumOptions byOrdinal() {
						setLinkageParameterBinder(EnumBindType.ORDINAL);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					private void setLinkageParameterBinder(EnumBindType ordinal) {
						linkage.setParameterBinder(ordinal.newParameterBinder((Class<Enum>) linkage.getColumnType()));
					}
				}, true)
				.fallbackOn(this)
				.build((Class<IFluentEmbeddableMappingBuilderEnumOptions<C>>) (Class) IFluentEmbeddableMappingBuilderEnumOptions.class);
	}
	
	@Override
	public IFluentEmbeddableMappingBuilder<C> mapSuperClass(EmbeddedBeanMappingStrategy<? super C, ?> mappingStrategy) {
		mappedSuperClass = mappingStrategy;
		return this;
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilderEmbedOptions<C, O> embed(SerializableBiConsumer<C, O> setter) {
		return embed(newInset(setter));
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilderEmbedOptions<C, O> embed(SerializableFunction<C, O> getter) {
		return embed(newInset(getter));
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilderEmbeddableOptions<C, O> embed(SerializableFunction<C, O> getter, EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder) {
		ImportedInset<C, O> importedInset = new ImportedInset<>(getter, this, embeddableMappingBuilder);
		return addImportedInset(importedInset);
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilderEmbeddableOptions<C, O> embed(SerializableBiConsumer<C, O> getter, EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder) {
		ImportedInset<C, O> importedInset = new ImportedInset<>(getter, this, embeddableMappingBuilder);
		return addImportedInset(importedInset);
	}
	
	private <O> IFluentEmbeddableMappingBuilderEmbeddableOptions<C, O> addImportedInset(ImportedInset<C, O> importedInset) {
		insets.add(importedInset);
		return new MethodReferenceDispatcher()
				// Why capturing overrideName(AccessorChain, String) this way ? (I mean with the "one method" capture instead of the usual "interface methods capture")
				// Because of ... lazyness ;) : "interface method capture" (such as done with EmbedingEmbeddableOptions) would have required a dedicated
				// interface (inheriting from EmbedingEmbeddableOptions) to define overrideName(AccessorChain, String)
				.redirect((SerializableTriFunction<IFluentEmbeddableMappingConfigurationEmbeddableOptions, AccessorChain, String, IFluentEmbeddableMappingConfigurationEmbeddableOptions>)
						IFluentEmbeddableMappingConfigurationEmbeddableOptions::overrideName,
						(BiConsumer<AccessorChain, String>) importedInset::overrideName)
				.redirect(EmbeddingOptions.class, new EmbeddingOptions<C>() {

					@Override
					public <IN> EmbeddingOptions<C> overrideName(SerializableFunction<C, IN> function, String columnName) {
						importedInset.overrideName(function, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}

					@Override
					public <IN> EmbeddingOptions<C> overrideName(SerializableBiConsumer<C, IN> function, String columnName) {
						importedInset.overrideName(function, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}
				}, true)
				.fallbackOn(this)
				.build((Class<IFluentEmbeddableMappingBuilderEmbeddableOptions<C, O>>) (Class) IFluentEmbeddableMappingBuilderEmbeddableOptions.class);
	}
	
	private <O> IFluentEmbeddableMappingBuilderEmbedOptions<C, O> embed(Inset<C, O> inset) {
		insets.add(inset);
		return new MethodDispatcher()
				.redirect(EmbedOptions.class, new EmbedOptions() {
					@Override
					public EmbedOptions overrideName(SerializableFunction getter, String columnName) {
						// we affect the last inset definition, not the embed(..) argument because one can use innerEmbed(..) before
						// overrideName(..), making embed(..) argument an old/previous value
						currentInset().overrideName(getter, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedOptions innerEmbed(SerializableFunction getter) {
						// this can hardly be reused in other innerMebd method due to currentInset() & newInset(..) invokation side effect :
						// they must be call in order else it results in an endless loop
						Inset parent = currentInset();
						Inset<C, O> inset = newInset(getter);
						inset.setParent(parent);
						insets.add(inset);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedOptions innerEmbed(SerializableBiConsumer setter) {
						// this can hardly be reused in other innerMebd method due to currentInset() & newInset(..) invokation side effect :
						// they must be call in order else it results in an endless loop
						Inset parent = currentInset();
						Inset<C, O> inset = newInset(setter);
						inset.setParent(parent);
						insets.add(inset);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedOptions exclude(SerializableBiConsumer setter) {
						// we affect the last inset definition, not the embed(..) argument because one can use innerEmbed(..) before
						// exclude(..), making embed(..) argument an old/previous value
						currentInset().exclude(setter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedOptions exclude(SerializableFunction getter) {
						// we affect the last inset definition, not the embed(..) argument because one can use innerEmbed(..) before
						// exclude(..), making embed(..) argument an old/previous value
						currentInset().exclude(getter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
				}, true)
				.fallbackOn(this)
				.build((Class<IFluentEmbeddableMappingBuilderEmbedOptions<C, O>>) (Class) IFluentEmbeddableMappingBuilderEmbedOptions.class);
	}
		
	/**
	 * Gives the mapping between configured accessors and table columns.
	 * Necessary columns are added to the table
	 * 
	 * @param dialect necessary for some checking
	 * @param targetTable table that will own the columns
	 * @return the mapping between "property" to column
	 */
	private Map<IReversibleAccessor, Column> buildMapping(Dialect dialect, Table targetTable) {
		return new EmbeddableMappingBuilder(this, dialect, targetTable).build();
	}
	
	@Override
	public EmbeddedBeanMappingStrategy<C, Table> build(Dialect dialect) {
		return build(dialect, new Table<>(persistedClass.getSimpleName()));
	}
	
	@Override
	public <T extends Table> EmbeddedBeanMappingStrategy<C, T> build(Dialect dialect, T targetTable) {
		return new EmbeddedBeanMappingStrategy<>(persistedClass, targetTable, (Map) buildMapping(dialect, targetTable));
	}
	
	/**
	 * Small constract for mapping definition storage. See add(..) methods.
	 * 
	 * @param <T> property owner type
	 */
	interface Linkage<T> {
		
		<I> PropertyAccessor<T, I> getAccessor();
		
		String getColumnName();
		
		Class<?> getColumnType();
		
		void setParameterBinder(ParameterBinder parameterBinder);
		
		ParameterBinder getParameterBinder();
	}
	
	/**
	 * Simple support for {@link Linkage}
	 * 
	 * @param <T> property owner type
	 */
	static class LinkageByColumnName<T> implements Linkage<T> {
		
		private final PropertyAccessor function;
		private final Class<?> columnType;
		/** Column name override if not default */
		private final String columnName;
		/** Optional binder for this mapping */
		private ParameterBinder parameterBinder;
		
		/**
		 * Constructor by {@link Method}. Only accessor by method is implemented (since input is from method reference).
		 * (Doing it for field accessor is simple work but not necessary)
		 *
		 * @param method a {@link PropertyAccessor}
		 * @param columnName an override of the default name that will be generated
		 */
		LinkageByColumnName(Method method, String columnName) {
			this.function = Accessors.of(method);
			this.columnType = Reflections.propertyType(method);
			this.columnName = columnName;
		}
		
		public <I> PropertyAccessor<T, I> getAccessor() {
			return function;
		}
		
		public String getColumnName() {
			return columnName;
		}
		
		public Class<?> getColumnType() {
			return columnType;
		}
		
		@Override
		public void setParameterBinder(ParameterBinder parameterBinder) {
			this.parameterBinder = parameterBinder;
		}
		
		@Override
		public ParameterBinder getParameterBinder() {
			return parameterBinder;
		}
	}
	
	abstract static class AbstractInset<SRC, TRGT> {
		private final Class<TRGT> embeddedClass;
		private final PropertyAccessor<SRC, TRGT> accessor;
		private final Method insetAccessor;
		
		protected AbstractInset(SerializableBiConsumer<SRC, TRGT> targetSetter, LambdaMethodUnsheller lambdaMethodUnsheller) {
			this.insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetSetter);
			this.accessor = new PropertyAccessor<>(
					(AccessorByMethod<SRC, TRGT>) new MutatorByMethod<>(insetAccessor).toAccessor(),
					new MutatorByMethodReference<>(targetSetter));
			// looking for the target type because its necessary to find its persister (and other objects)
			this.embeddedClass = (Class<TRGT>) Reflections.javaBeanTargetType(getInsetAccessor());
		}
		
		protected AbstractInset(SerializableFunction<SRC, TRGT> targetGetter, LambdaMethodUnsheller lambdaMethodUnsheller) {
			this.insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetGetter);
			this.accessor = new PropertyAccessor<>(
					new AccessorByMethodReference<>(targetGetter),
					((AccessorByMethod<SRC, TRGT>) new AccessorByMethod<>(insetAccessor)).toMutator());
			// looking for the target type because its necessary to find its persister (and other objects)
			this.embeddedClass = (Class<TRGT>) Reflections.javaBeanTargetType(getInsetAccessor());
		}
		
		public PropertyAccessor<SRC, TRGT> getAccessor() {
			return accessor;
		}
		
		public Method getInsetAccessor() {
			return insetAccessor;
		}
		
		public Class<TRGT> getEmbeddedClass() {
			return embeddedClass;
		}
		
		public abstract ValueAccessPointSet getExcludedProperties();
		
		public abstract ValueAccessPointMap<String> getOverridenColumnNames();
	}
	
	static class ImportedInset<SRC, TRGT> extends AbstractInset<SRC, TRGT> implements LambdaMethodUnsheller {
		
		private final EmbeddedBeanMappingStrategyBuilder<TRGT> beanMappingBuilder;
		private final ValueAccessPointMap<String> overridenColumnNames = new ValueAccessPointMap<>();
		private final LambdaMethodUnsheller lambdaMethodUnsheller;
		
		ImportedInset(SerializableBiConsumer<SRC, TRGT> targetSetter, LambdaMethodUnsheller lambdaMethodUnsheller,
								EmbeddedBeanMappingStrategyBuilder<TRGT> beanMappingBuilder) {
			super(targetSetter, lambdaMethodUnsheller);
			this.beanMappingBuilder = beanMappingBuilder;
			this.lambdaMethodUnsheller = lambdaMethodUnsheller;
		}
		
		ImportedInset(SerializableFunction<SRC, TRGT> targetGetter, LambdaMethodUnsheller lambdaMethodUnsheller,
								EmbeddedBeanMappingStrategyBuilder<TRGT> beanMappingBuilder) {
			super(targetGetter, lambdaMethodUnsheller);
			this.beanMappingBuilder = beanMappingBuilder;
			this.lambdaMethodUnsheller = lambdaMethodUnsheller;
		}
		
		public EmbeddedBeanMappingStrategyBuilder<TRGT> getBeanMappingBuilder() {
			return beanMappingBuilder;
		}
		
		public void overrideName(SerializableFunction methodRef, String columnName) {
			this.overridenColumnNames.put(new AccessorByMethodReference(methodRef), columnName);
		}
		
		public void overrideName(SerializableBiConsumer methodRef, String columnName) {
			this.overridenColumnNames.put(new MutatorByMethodReference(methodRef), columnName);
		}
		
		public void overrideName(AccessorChain accessorChain, String columnName) {
			this.overridenColumnNames.put(accessorChain, columnName);
		}
		
		@Override
		public Method captureLambdaMethod(SerializableFunction getter) {
			return this.lambdaMethodUnsheller.captureLambdaMethod(getter);
		}
		
		@Override
		public Method captureLambdaMethod(SerializableBiConsumer setter) {
			return this.lambdaMethodUnsheller.captureLambdaMethod(setter);
		}
		
		@Override
		public ValueAccessPointSet getExcludedProperties() {
			return new ValueAccessPointSet();
		}
		
		@Override
		public ValueAccessPointMap<String> getOverridenColumnNames() {
			return overridenColumnNames;
		}
	}
	
	/**
	 * Represents a property that embeds a complex type
	 *
	 * @param <SRC> the owner type
	 * @param <TRGT> the target type
	 */
	static class Inset<SRC, TRGT> extends AbstractInset<SRC, TRGT> implements LambdaMethodUnsheller {
		private final ValueAccessPointMap<String> overridenColumnNames = new ValueAccessPointMap<>();
		private final ValueAccessPointSet excludedProperties = new ValueAccessPointSet();
		private final LambdaMethodUnsheller lambdaMethodUnsheller;
		/** For inner embedded inset : indicates parent embeddable */
		private Inset parent;
		
		Inset(SerializableBiConsumer<SRC, TRGT> targetSetter, LambdaMethodUnsheller lambdaMethodUnsheller) {
			super(targetSetter, lambdaMethodUnsheller);
			this.lambdaMethodUnsheller = lambdaMethodUnsheller;
		}
		
		Inset(SerializableFunction<SRC, TRGT> targetGetter, LambdaMethodUnsheller lambdaMethodUnsheller) {
			super(targetGetter, lambdaMethodUnsheller);
			this.lambdaMethodUnsheller = lambdaMethodUnsheller;
		}
		
		public void overrideName(SerializableFunction methodRef, String columnName) {
			this.overridenColumnNames.put(new AccessorByMethodReference(methodRef), columnName);
		}
		
		public void exclude(SerializableBiConsumer methodRef) {
			this.excludedProperties.add(new MutatorByMethodReference(methodRef));
		}
		
		public void exclude(SerializableFunction methodRef) {
			this.excludedProperties.add(new AccessorByMethodReference(methodRef));
		}
		
		@Override
		public Method captureLambdaMethod(SerializableFunction getter) {
			return this.lambdaMethodUnsheller.captureLambdaMethod(getter);
		}
		
		@Override
		public Method captureLambdaMethod(SerializableBiConsumer setter) {
			return this.lambdaMethodUnsheller.captureLambdaMethod(setter);
		}
		
		public Inset getParent() {
			return parent;
		}
		
		public void setParent(Inset parent) {
			this.parent = parent;
		}
		
		@Override
		public ValueAccessPointMap<String> getOverridenColumnNames() {
			return overridenColumnNames;
		}
		
		@Override
		public ValueAccessPointSet getExcludedProperties() {
			return excludedProperties;
		}
	}
	
	
	
}
