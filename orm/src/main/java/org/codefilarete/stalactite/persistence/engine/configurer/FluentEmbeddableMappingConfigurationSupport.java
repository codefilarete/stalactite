package org.codefilarete.stalactite.persistence.engine.configurer;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;

import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.reflection.MutatorByMethod;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.persistence.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.persistence.engine.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.persistence.engine.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.persistence.engine.EnumOptions;
import org.codefilarete.stalactite.persistence.engine.FluentEmbeddableMappingBuilder;
import org.codefilarete.stalactite.persistence.engine.ImportedEmbedOptions;
import org.codefilarete.stalactite.persistence.engine.PropertyOptions;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry.EnumBindType;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * @author Guillaume Mary
 */
public class FluentEmbeddableMappingConfigurationSupport<C> implements FluentEmbeddableMappingBuilder<C>, LambdaMethodUnsheller,
		EmbeddableMappingConfiguration<C> {
	
	private EmbeddableMappingConfigurationProvider<? super C> superMappingBuilder;
	
	/** Owning class of mapped properties */
	private final Class<C> classToPersist;
	
	@Nullable
	private ColumnNamingStrategy columnNamingStrategy;
	
	/** Mapping definitions */
	protected final List<Linkage> mapping = new ArrayList<>();
	
	/** Collection of embedded elements, even inner ones to help final build process */
	private final Collection<Inset<C, ?>> insets = new ArrayList<>();
	
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
	public Collection<Inset<C, ?>> getInsets() {
		return insets;
	}
	
	@Override
	public EmbeddableMappingConfiguration<? super C> getMappedSuperClassConfiguration() {
		return superMappingBuilder == null ? null : superMappingBuilder.getConfiguration();
	}
	
	@Override
	@Nullable
	public ColumnNamingStrategy getColumnNamingStrategy() {
		return columnNamingStrategy;
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
	public FluentEmbeddableMappingConfigurationSupport<C> withColumnNaming(ColumnNamingStrategy columnNamingStrategy) {
		this.columnNamingStrategy = columnNamingStrategy;
		return this;
	}
	
	/**
	 * Gives access to currently configured {@link Inset}. Made so one can access features of {@link Inset} which are wider than
	 * the one available through {@link FluentEmbeddableMappingBuilder}.
	 * 
	 * @return the last {@link Inset} built by {@link #newInset(SerializableFunction, EmbeddableMappingConfigurationProvider)}
	 * or {@link #newInset(SerializableBiConsumer, EmbeddableMappingConfigurationProvider)}
	 */
	protected Inset<C, ?> currentInset() {
		return currentInset;
	}
	
	protected <O> Inset<C, O> newInset(SerializableFunction<C, O> getter, EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder) {
		currentInset = new Inset<>(getter, embeddableMappingBuilder, this);
		return (Inset<C, O>) currentInset;
	}
	
	protected <O> Inset<C, O> newInset(SerializableBiConsumer<C, O> setter, EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder) {
		currentInset = new Inset<>(setter, embeddableMappingBuilder, this);
		return (Inset<C, O>) currentInset;
	}
	
	@Override
	public <O> FluentEmbeddableMappingBuilderPropertyOptions<C> map(SerializableBiConsumer<C, O> setter) {
		return addPropertyOptions(addMapping(setter, null));
	}
	
	@Override
	public <O> FluentEmbeddableMappingBuilderPropertyOptions<C> map(SerializableFunction<C, O> getter) {
		return addPropertyOptions(addMapping(getter, null));
	}
	
	@Override
	public <O> FluentEmbeddableMappingBuilderPropertyOptions<C> map(SerializableBiConsumer<C, O> setter, String columnName) {
		return addPropertyOptions(addMapping(setter, columnName));
	}
	
	@Override
	public <O> FluentEmbeddableMappingBuilderPropertyOptions<C> map(SerializableFunction<C, O> getter, String columnName) {
		return addPropertyOptions(addMapping(getter, columnName));
	}
	
	<O> FluentEmbeddableMappingBuilderPropertyOptions<C> addPropertyOptions(LinkageSupport<C, O> linkage) {
		return new MethodReferenceDispatcher()
				.redirect(PropertyOptions.class, new PropertyOptions() {
					@Override
					public PropertyOptions mandatory() {
						linkage.setNullable(false);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public PropertyOptions setByConstructor() {
						linkage.setByConstructor();
						return null;
					}
				}, true)
				.fallbackOn(this)
				.build((Class<FluentEmbeddableMappingBuilderPropertyOptions<C>>) (Class) FluentEmbeddableMappingBuilderPropertyOptions.class);
	}
	
	<E> LinkageSupport<C, E> addMapping(SerializableBiConsumer<C, E> setter, @Nullable String columnName) {
		return addMapping(Accessors.mutator(setter), columnName);
	}
	
	<E> LinkageSupport<C, E> addMapping(SerializableFunction<C, E> getter, @Nullable String columnName) {
		return addMapping(Accessors.accessor(getter), columnName);
	}
	
	<E> LinkageSupport<C, E> addMapping(ReversibleAccessor<C, E> propertyAccessor, @Nullable String columnName) {
		LinkageSupport<C, E> linkage1 = new LinkageSupport<>(propertyAccessor);
		linkage1.setColumnOptions(new ColumnLinkageOptionsByName(columnName));
		LinkageSupport<C, E> linkage = linkage1;
		this.mapping.add(linkage);
		return linkage;
	}
	
	@Override
	public <E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C> mapEnum(SerializableBiConsumer<C, E> setter) {
		LinkageSupport<C, E> linkage = addMapping(setter, null);
		return addEnumOptions(linkage);
	}
	
	@Override
	public <E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C> mapEnum(SerializableFunction<C, E> getter) {
		LinkageSupport<C, E> linkage = addMapping(getter, null);
		return addEnumOptions(linkage);
	}
	
	@Override
	public <E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C> mapEnum(SerializableBiConsumer<C, E> setter, String columnName) {
		LinkageSupport<C, E> linkage = addMapping(setter, columnName);
		return addEnumOptions(linkage);
	}
	
	@Override
	public <E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C> mapEnum(SerializableFunction<C, E> getter, String columnName) {
		LinkageSupport<C, E> linkage = addMapping(getter, columnName);
		return addEnumOptions(linkage);
	}
	
	<O extends Enum> FluentEmbeddableMappingBuilderEnumOptions<C> addEnumOptions(LinkageSupport<C, O> linkage) {
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
						linkage.setParameterBinder(ordinal.newParameterBinder(linkage.getColumnType()));
					}
					
					@Override
					public EnumOptions mandatory() {
						linkage.setNullable(false);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public PropertyOptions setByConstructor() {
						linkage.setByConstructor();
						return null;
					}
				}, true)
				.fallbackOn(this)
				.build((Class<FluentEmbeddableMappingBuilderEnumOptions<C>>) (Class) FluentEmbeddableMappingBuilderEnumOptions.class);
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
		insets.add(inset);
		return new MethodReferenceDispatcher()
				// Why capturing overrideName(AccessorChain, String) this way ? (I mean with the "one method" capture instead of the usual "interface methods capture")
				// Because of ... lazyness ;) : "interface method capture" (such as done with ImportedEmbedOptions) would have required a dedicated
				// interface (inheriting from ImportedEmbedOptions) to define overrideName(AccessorChain, String)
				.redirect((SerializableTriFunction<FluentEmbeddableMappingConfigurationImportedEmbedOptions, SerializableFunction, String, FluentEmbeddableMappingConfigurationImportedEmbedOptions>)
						FluentEmbeddableMappingConfigurationImportedEmbedOptions::overrideName,
						(BiConsumer<SerializableFunction, String>) inset::overrideName)
				.redirect((SerializableTriFunction<FluentEmbeddableMappingConfigurationImportedEmbedOptions, SerializableBiConsumer, String, FluentEmbeddableMappingConfigurationImportedEmbedOptions>)
						FluentEmbeddableMappingConfigurationImportedEmbedOptions::overrideName,
						(BiConsumer<SerializableBiConsumer, String>) inset::overrideName)
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
	
	/**
	 * Small constract for mapping definition storage. See add(..) methods.
	 * 
	 * @param <T> property owner type
	 */
	protected static class LinkageSupport<T, O> implements Linkage<T, O> {
		
		/** Optional binder for this mapping */
		private ParameterBinder<O> parameterBinder;
		
		private boolean nullable = true;
		
		private boolean setByConstructor = false;
		
		@Nullable
		private ColumnLinkageOptions columnOptions;
		
		private final ReversibleAccessor<T, ?> function;
		
		public LinkageSupport(ReversibleAccessor<T, ?> function) {
			this.function = function;
		}
		
		public void setParameterBinder(ParameterBinder<O> parameterBinder) {
			this.parameterBinder = parameterBinder;
		}
		
		@Override
		public ParameterBinder<O> getParameterBinder() {
			return parameterBinder;
		}
		
		@Override
		public boolean isNullable() {
			return nullable;
		}
		
		public void setNullable(boolean nullable) {
			this.nullable = nullable;
		}
		
		public void setByConstructor() {
			this.setByConstructor = true;
		}
		
		@Override
		public boolean isSetByConstructor() {
			return setByConstructor;
		}
		
		@Nullable
		public ColumnLinkageOptions getColumnOptions() {
			return columnOptions;
		}
		
		public void setColumnOptions(ColumnLinkageOptions columnOptions) {
			this.columnOptions = columnOptions;
		}
		
		@Override
		public ReversibleAccessor<T, O> getAccessor() {
			return (ReversibleAccessor<T, O>) function;
		}
		
		@Nullable
		@Override
		public String getColumnName() {
			return org.codefilarete.tool.Nullable.nullable(this.columnOptions).map(ColumnLinkageOptions::getColumnName).get();
		}
		
		@Override
		public Class<O> getColumnType() {
			return this.columnOptions instanceof ColumnLinkageOptionsByColumn
				? (Class<O>) ((ColumnLinkageOptionsByColumn) this.columnOptions).getColumnType()
				: AccessorDefinition.giveDefinition(this.function).getMemberType();
		}
	}
	
	interface ColumnLinkageOptions {
		
		@Nullable
		String getColumnName();
		
	}
	
	static class ColumnLinkageOptionsByName implements ColumnLinkageOptions {
		
		private final String columnName;
		
		ColumnLinkageOptionsByName(@Nullable String columnName) {
			this.columnName = columnName;
		}
		
		@Nullable
		@Override
		public String getColumnName() {
			return this.columnName;
		}
		
	}
	
	static class ColumnLinkageOptionsByColumn implements ColumnLinkageOptions {
		
		private final Column column;
		
		ColumnLinkageOptionsByColumn(Column column) {
			this.column = column;
		}
		
		public Column getColumn() {
			return column;
		}
		
		@Override
		public String getColumnName() {
			return this.column.getName();
		}
		
		public Class<?> getColumnType() {
			return this.column.getJavaType();
		}
	}
	
	/**
	 * Information storage of embedded mapping defined externally by an {@link EmbeddableMappingConfigurationProvider},
	 * see {@link #embed(SerializableFunction, EmbeddableMappingConfigurationProvider)}
	 *
	 * @param <SRC>
	 * @param <TRGT>
	 * @see #embed(SerializableFunction, EmbeddableMappingConfigurationProvider)}
	 * @see #embed(SerializableBiConsumer, EmbeddableMappingConfigurationProvider)}
	 */
	public static class Inset<SRC, TRGT> {
		private final Class<TRGT> embeddedClass;
		private final Method insetAccessor;
		/** Equivalent of {@link #insetAccessor} as a {@link PropertyAccessor}  */
		private final PropertyAccessor<SRC, TRGT> accessor;
		private final ValueAccessPointMap<String> overridenColumnNames = new ValueAccessPointMap<>();
		private final ValueAccessPointSet excludedProperties = new ValueAccessPointSet();
		private final EmbeddableMappingConfigurationProvider<? extends TRGT> beanMappingBuilder;
		private final ValueAccessPointMap<Column> overridenColumns = new ValueAccessPointMap<>();
		
		
		Inset(SerializableBiConsumer<SRC, TRGT> targetSetter,
			  EmbeddableMappingConfigurationProvider<? extends TRGT> beanMappingBuilder,
			  LambdaMethodUnsheller lambdaMethodUnsheller) {
			this.insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetSetter);
			this.accessor = new PropertyAccessor<>(
					new MutatorByMethod<SRC, TRGT>(insetAccessor).toAccessor(),
					new MutatorByMethodReference<>(targetSetter));
			// looking for the target type because it's necessary to find its persister (and other objects)
			this.embeddedClass = Reflections.javaBeanTargetType(getInsetAccessor());
			this.beanMappingBuilder = beanMappingBuilder;
		}
		
		Inset(SerializableFunction<SRC, TRGT> targetGetter,
			  EmbeddableMappingConfigurationProvider<? extends TRGT> beanMappingBuilder,
			  LambdaMethodUnsheller lambdaMethodUnsheller) {
			this.insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetGetter);
			this.accessor = new PropertyAccessor<>(
					new AccessorByMethodReference<>(targetGetter),
					new AccessorByMethod<SRC, TRGT>(insetAccessor).toMutator());
			// looking for the target type because it's necessary to find its persister (and other objects)
			this.embeddedClass = Reflections.javaBeanTargetType(getInsetAccessor());
			this.beanMappingBuilder = beanMappingBuilder;
		}
		
		/**
		 * Equivalent of {@link #insetAccessor} as a {@link PropertyAccessor}
		 */
		public PropertyAccessor<SRC, TRGT> getAccessor() {
			return accessor;
		}
		
		/**
		 * Equivalent of given getter or setter at construction time as a {@link Method}
		 */
		public Method getInsetAccessor() {
			return insetAccessor;
		}
		
		public Class<TRGT> getEmbeddedClass() {
			return embeddedClass;
		}
		
		public ValueAccessPointSet getExcludedProperties() {
			return this.excludedProperties;
		}
		
		public ValueAccessPointMap<String> getOverridenColumnNames() {
			return this.overridenColumnNames;
		}
		
		public ValueAccessPointMap<Column> getOverridenColumns() {
			return overridenColumns;
		}
		
		public EmbeddableMappingConfigurationProvider<TRGT> getBeanMappingBuilder() {
			return (EmbeddableMappingConfigurationProvider<TRGT>) beanMappingBuilder;
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
		
		public void override(SerializableFunction methodRef, Column column) {
			this.overridenColumns.put(new AccessorByMethodReference(methodRef), column);
		}
		
		public void override(SerializableBiConsumer methodRef, Column column) {
			this.overridenColumns.put(new MutatorByMethodReference(methodRef), column);
		}
		
		public void exclude(SerializableBiConsumer methodRef) {
			this.excludedProperties.add(new MutatorByMethodReference(methodRef));
		}
		
		public void exclude(SerializableFunction methodRef) {
			this.excludedProperties.add(new AccessorByMethodReference(methodRef));
		}
	}
}
