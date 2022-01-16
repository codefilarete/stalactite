package org.gama.stalactite.persistence.engine.configurer;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Reflections;
import org.gama.lang.function.SerializableTriFunction;
import org.gama.reflection.AccessorByMethod;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.AccessorChain;
import org.gama.reflection.AccessorDefinition;
import org.gama.reflection.Accessors;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.reflection.MutatorByMethod;
import org.gama.reflection.MutatorByMethodReference;
import org.gama.reflection.PropertyAccessor;
import org.gama.reflection.ReversibleAccessor;
import org.gama.reflection.ValueAccessPointMap;
import org.gama.reflection.ValueAccessPointSet;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfiguration;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfigurationProvider;
import org.gama.stalactite.persistence.engine.EnumOptions;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingBuilder;
import org.gama.stalactite.persistence.engine.ImportedEmbedOptions;
import org.gama.stalactite.persistence.engine.PropertyOptions;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.sql.binder.ParameterBinder;
import org.gama.stalactite.sql.binder.ParameterBinderRegistry.EnumBindType;

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
	public <O> FluentEmbeddableMappingBuilderPropertyOptions<C> add(SerializableBiConsumer<C, O> setter) {
		return addPropertyOptions(addMapping(setter, null));
	}
	
	@Override
	public <O> FluentEmbeddableMappingBuilderPropertyOptions<C> add(SerializableFunction<C, O> getter) {
		return addPropertyOptions(addMapping(getter, null));
	}
	
	@Override
	public <O> FluentEmbeddableMappingBuilderPropertyOptions<C> add(SerializableBiConsumer<C, O> setter, String columnName) {
		return addPropertyOptions(addMapping(setter, columnName));
	}
	
	@Override
	public <O> FluentEmbeddableMappingBuilderPropertyOptions<C> add(SerializableFunction<C, O> getter, String columnName) {
		return addPropertyOptions(addMapping(getter, columnName));
	}
	
	FluentEmbeddableMappingBuilderPropertyOptions<C> addPropertyOptions(AbstractLinkage<C> linkage) {
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
	
	<E> AbstractLinkage<C> addMapping(SerializableBiConsumer<C, E> setter, @Nullable String columnName) {
		ReversibleAccessor<C, E> mutator = Accessors.mutator(setter);
		return addMapping(mutator, AccessorDefinition.giveDefinition(mutator), columnName);
	}
	
	<E> AbstractLinkage<C> addMapping(SerializableFunction<C, E> getter, @Nullable String columnName) {
		ReversibleAccessor<C, E> accessor = Accessors.accessor(getter);
		return addMapping(accessor, AccessorDefinition.giveDefinition(accessor), columnName);
	}
	
	AbstractLinkage<C> addMapping(ReversibleAccessor<C, ?> propertyAccessor, AccessorDefinition accessorDefinition, @Nullable String columnName) {
		AbstractLinkage<C> linkage = newLinkage(propertyAccessor, accessorDefinition.getMemberType(), columnName);
		this.mapping.add(linkage);
		return linkage;
	}
	
	protected <O> LinkageByColumnName<C> newLinkage(ReversibleAccessor<C, O> accessor, Class<O> returnType, String linkName) {
		return new LinkageByColumnName<>(accessor, returnType, linkName);
	}
	
	@Override
	public <E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableBiConsumer<C, E> setter) {
		AbstractLinkage<C> linkage = addMapping(setter, null);
		return addEnumOptions(linkage);
	}
	
	@Override
	public <E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableFunction<C, E> getter) {
		AbstractLinkage<C> linkage = addMapping(getter, null);
		return addEnumOptions(linkage);
	}
	
	@Override
	public <E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableBiConsumer<C, E> setter, String columnName) {
		AbstractLinkage<C> linkage = addMapping(setter, columnName);
		return addEnumOptions(linkage);
	}
	
	@Override
	public <E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableFunction<C, E> getter, String columnName) {
		AbstractLinkage<C> linkage = addMapping(getter, columnName);
		return addEnumOptions(linkage);
	}
	
	FluentEmbeddableMappingBuilderEnumOptions<C> addEnumOptions(AbstractLinkage<C> linkage) {
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
	// TODO: rename as LinkageSupport, or even replace interface Linkage by this class: leave interface Linkage since we have setters here
	protected static class AbstractLinkage<T> implements Linkage<T> {
		
		/** Optional binder for this mapping */
		private ParameterBinder parameterBinder;
		
		private boolean nullable = true;
		
		private boolean setByConstructor = false;
		
		@Nullable
		private ColumnLinkageOptions columnOptions;
		
		private final ReversibleAccessor<T, ?> function;
		
		public AbstractLinkage(ReversibleAccessor<T, ?> function) {
			this.function = function;
		}
		
		public void setParameterBinder(ParameterBinder parameterBinder) {
			this.parameterBinder = parameterBinder;
		}
		
		@Override
		public ParameterBinder getParameterBinder() {
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
		public <O> ReversibleAccessor<T, O> getAccessor() {
			return (ReversibleAccessor<T, O>) function;
		}
		
		@Nullable
		@Override
		public String getColumnName() {
			return org.gama.lang.Nullable.nullable(this.columnOptions).map(ColumnLinkageOptions::getColumnName).get();
		}
		
		@Override
		public Class<?> getColumnType() {
			return org.gama.lang.Nullable.nullable(this.columnOptions).map(ColumnLinkageOptions::getColumnType).getOr(() -> AccessorDefinition.giveDefinition(this.function).getMemberType());
			
		}
	}
	
	interface ColumnLinkageOptions {
		
		@Nullable
		String getColumnName();
		
		Class<?> getColumnType();
		
		void primaryKey();
		
	}
	
	static class ColumnLinkageOptionsByName implements ColumnLinkageOptions {
		
		private final String columnName;
		
		private final Class<?> columnType;
		
		private boolean primaryKey;
		
		ColumnLinkageOptionsByName(@Nullable String columnName, Class<?> columnType) {
			this.columnName = columnName;
			this.columnType = columnType;
		}
		
		@Nullable
		@Override
		public String getColumnName() {
			return this.columnName;
		}
		
		@Override
		public Class<?> getColumnType() {
			return this.columnType;
		}
		
		@Override
		public void primaryKey() {
			this.primaryKey = true;
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
		
		@Override
		public Class<?> getColumnType() {
			return this.column.getJavaType();
		}
		
		@Override
		public void primaryKey() {
			if (!this.column.isPrimaryKey()){
				// safeguard about misconfiguration, even if mapping would work it smells bad configuration
				throw new IllegalArgumentException("Identifier policy is assigned to a non primary key column");
			}
		}
	}
	
	/**
	 * Simple support for {@link Linkage}
	 * 
	 * @param <T> property owner type
	 */
	public static class LinkageByColumnName<T> extends AbstractLinkage<T> {
		
//		private final ReversibleAccessor<T, ?> function;
		private final Class<?> columnType;
		/** Column name override if not default */
		@Nullable
		private final String columnName;
		
		/**
		 * Constructor by {@link ReversibleAccessor}
		 *
		 * @param accessor a {@link ReversibleAccessor}
		 * @param columnType the Java type of the column, will be converted to sql type thanks to {@link org.gama.stalactite.persistence.sql.ddl.SqlTypeRegistry}
		 * @param columnName an override of the default name that will be generated
		 */
		public <O> LinkageByColumnName(ReversibleAccessor<T, O> accessor, Class<O> columnType, @Nullable String columnName) {
			super(accessor);
			this.columnType = columnType;
			this.columnName = columnName;
		}
		
//		@Override
//		public <O> ReversibleAccessor<T, O> getAccessor() {
//			return (ReversibleAccessor<T, O>) function;
//		}
		
		@Override
		@Nullable
		public String getColumnName() {
			return columnName;
		}
		
		@Override
		public Class<?> getColumnType() {
			return columnType;
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
			// looking for the target type because its necessary to find its persister (and other objects)
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
			// looking for the target type because its necessary to find its persister (and other objects)
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
