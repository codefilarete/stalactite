package org.gama.stalactite.persistence.engine;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Reflections;
import org.gama.lang.function.SerializableTriFunction;
import org.gama.lang.reflect.MethodDispatcher;
import org.gama.reflection.AccessorByMethod;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.AccessorChain;
import org.gama.reflection.AccessorDefinition;
import org.gama.reflection.Accessors;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.reflection.MutatorByMethod;
import org.gama.reflection.MutatorByMethodReference;
import org.gama.reflection.PropertyAccessor;
import org.gama.reflection.ValueAccessPointMap;
import org.gama.reflection.ValueAccessPointSet;
import org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.binder.ParameterBinder;
import org.gama.stalactite.sql.binder.ParameterBinderRegistry.EnumBindType;

/**
 * @author Guillaume Mary
 */
public class FluentEmbeddableMappingConfigurationSupport<C> implements IFluentEmbeddableMappingBuilder<C>, LambdaMethodUnsheller,
		EmbeddableMappingConfiguration<C> {
	
	private EmbeddableMappingConfiguration<? super C> superMappingBuilder;
	
	/** Owning class of mapped properties */
	private final Class<C> classToPersist;
	
	private ColumnNamingStrategy columnNamingStrategy;
	
	/** Mapiing definitions */
	final List<Linkage> mapping = new ArrayList<>();
	
	/** Collection of embedded elements, even inner ones to help final build process */
	private final Collection<AbstractInset<C, ?>> insets = new ArrayList<>();
	
	/** Last embedded element, introduced to help inner embedding registration (kind of algorithm help). Has no purpose in whole mapping configuration. */
	protected AbstractInset<C, ?> currentInset;
	
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
	public <I extends AbstractInset<C, ?>> Collection<I> getInsets() {
		return (Collection<I>) insets;
	}
	
	@Override
	public EmbeddableMappingConfiguration<? super C> getMappedSuperClassConfiguration() {
		return superMappingBuilder;
	}
	
	@Override
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
	 *
	 * @return the last {@link Inset} built by {@link #newInset(SerializableFunction)} or {@link #newInset(SerializableBiConsumer)}
	 */
	protected AbstractInset<C, ?> currentInset() {
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
	
	protected <O> ImportedInset<C, O> newInset(SerializableFunction<C, O> getter, EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder) {
		currentInset = new ImportedInset<>(getter, this, embeddableMappingBuilder);
		return (ImportedInset<C, O>) currentInset;
	}
	
	protected <O> ImportedInset<C, O> newInset(SerializableBiConsumer<C, O> setter, EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder) {
		currentInset = new ImportedInset<>(setter, this, embeddableMappingBuilder);
		return (ImportedInset<C, O>) currentInset;
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilderPropertyOptions<C> add(SerializableBiConsumer<C, O> setter) {
		return addPropertyOptions(addMapping(setter, null));
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilderPropertyOptions<C> add(SerializableFunction<C, O> getter) {
		return addPropertyOptions(addMapping(getter, null));
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilderPropertyOptions<C> add(SerializableBiConsumer<C, O> setter, String columnName) {
		return addPropertyOptions(addMapping(setter, columnName));
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilderPropertyOptions<C> add(SerializableFunction<C, O> getter, String columnName) {
		return addPropertyOptions(addMapping(getter, columnName));
	}
	
	IFluentEmbeddableMappingBuilderPropertyOptions<C> addPropertyOptions(AbstractLinkage<C> linkage) {
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
				.build((Class<IFluentEmbeddableMappingBuilderPropertyOptions<C>>) (Class) IFluentEmbeddableMappingBuilderPropertyOptions.class);
	}
	
	<E> AbstractLinkage<C> addMapping(SerializableBiConsumer<C, E> setter, @Nullable String columnName) {
		IReversibleAccessor<C, E> mutator = Accessors.mutator(setter);
		return addMapping(mutator, AccessorDefinition.giveDefinition(mutator), columnName);
	}
	
	<E> AbstractLinkage<C> addMapping(SerializableFunction<C, E> getter, @Nullable String columnName) {
		IReversibleAccessor<C, E> accessor = Accessors.accessor(getter);
		return addMapping(accessor, AccessorDefinition.giveDefinition(accessor), columnName);
	}
	
	AbstractLinkage<C> addMapping(IReversibleAccessor<C, ?> propertyAccessor, AccessorDefinition accessorDefinition, @Nullable String columnName) {
		AbstractLinkage<C> linkage = newLinkage(propertyAccessor, accessorDefinition.getMemberType(), columnName);
		this.mapping.add(linkage);
		return linkage;
	}
	
	protected <O> LinkageByColumnName<C> newLinkage(IReversibleAccessor<C, O> accessor, Class<O> returnType, String linkName) {
		return new LinkageByColumnName<>(accessor, returnType, linkName);
	}
	
	@Override
	public <E extends Enum<E>> IFluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableBiConsumer<C, E> setter) {
		AbstractLinkage<C> linkage = addMapping(setter, null);
		return addEnumOptions(linkage);
	}
	
	@Override
	public <E extends Enum<E>> IFluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableFunction<C, E> getter) {
		AbstractLinkage<C> linkage = addMapping(getter, null);
		return addEnumOptions(linkage);
	}
	
	@Override
	public <E extends Enum<E>> IFluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableBiConsumer<C, E> setter, String columnName) {
		AbstractLinkage<C> linkage = addMapping(setter, columnName);
		return addEnumOptions(linkage);
	}
	
	@Override
	public <E extends Enum<E>> IFluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableFunction<C, E> getter, String columnName) {
		AbstractLinkage<C> linkage = addMapping(getter, columnName);
		return addEnumOptions(linkage);
	}
	
	IFluentEmbeddableMappingBuilderEnumOptions<C> addEnumOptions(AbstractLinkage<C> linkage) {
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
				.build((Class<IFluentEmbeddableMappingBuilderEnumOptions<C>>) (Class) IFluentEmbeddableMappingBuilderEnumOptions.class);
	}
	
	@Override
	public IFluentEmbeddableMappingBuilder<C> mapSuperClass(EmbeddableMappingConfiguration<? super C> superMappingConfiguration) {
		this.superMappingBuilder = superMappingConfiguration;
		return this;
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilderEmbedOptions<C, O> embed(SerializableBiConsumer<C, O> setter) {
		return addInset(newInset(setter));
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilderEmbedOptions<C, O> embed(SerializableFunction<C, O> getter) {
		return addInset(newInset(getter));
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embed(SerializableFunction<C, O> getter, EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder) {
		return addImportedInset(newInset(getter, embeddableMappingBuilder));
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embed(SerializableBiConsumer<C, O> setter, EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder) {
		return addImportedInset(newInset(setter, embeddableMappingBuilder));
	}
	
	private <O> IFluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> addImportedInset(ImportedInset<C, O> importedInset) {
		insets.add(importedInset);
		return new MethodReferenceDispatcher()
				// Why capturing overrideName(AccessorChain, String) this way ? (I mean with the "one method" capture instead of the usual "interface methods capture")
				// Because of ... lazyness ;) : "interface method capture" (such as done with EmbedingEmbeddableOptions) would have required a dedicated
				// interface (inheriting from EmbedingEmbeddableOptions) to define overrideName(AccessorChain, String)
				.redirect((SerializableTriFunction<IFluentEmbeddableMappingConfigurationImportedEmbedOptions, AccessorChain, String, IFluentEmbeddableMappingConfigurationImportedEmbedOptions>)
						IFluentEmbeddableMappingConfigurationImportedEmbedOptions::overrideName,
						(BiConsumer<AccessorChain, String>) importedInset::overrideName)
				.redirect(ImportedEmbedOptions.class, new ImportedEmbedOptions<C>() {

					@Override
					public <IN> ImportedEmbedOptions<C> overrideName(SerializableFunction<C, IN> function, String columnName) {
						importedInset.overrideName(function, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}

					@Override
					public <IN> ImportedEmbedOptions<C> overrideName(SerializableBiConsumer<C, IN> function, String columnName) {
						importedInset.overrideName(function, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ImportedEmbedOptions exclude(SerializableBiConsumer setter) {
						importedInset.exclude(setter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ImportedEmbedOptions exclude(SerializableFunction getter) {
						importedInset.exclude(getter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
				}, true)
				.fallbackOn(this)
				.build((Class<IFluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O>>) (Class) IFluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions.class);
	}
	
	private <O> IFluentEmbeddableMappingBuilderEmbedOptions<C, O> addInset(Inset<C, O> inset) {
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
					public EmbedOptions overrideName(SerializableBiConsumer setter, String columnName) {
						// we affect the last inset definition, not the embed(..) argument because one can use innerEmbed(..) before
						// overrideName(..), making embed(..) argument an old/previous value
						currentInset().overrideName(setter, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedOptions innerEmbed(SerializableFunction getter) {
						// this can hardly be reused in other innerEmbed method due to currentInset() & newInset(..) invokation side effect :
						// they must be call in order else it results in an endless loop
						Inset parent = (Inset) currentInset();
						Inset<C, O> inset = newInset(getter);
						inset.setParent(parent);
						insets.add(inset);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedOptions innerEmbed(SerializableBiConsumer setter) {
						// this can hardly be reused in other innerEmbed method due to currentInset() & newInset(..) invokation side effect :
						// they must be call in order else it results in an endless loop
						Inset parent = (Inset) currentInset();
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
		return new EmbeddableMappingBuilder<>(this).build(dialect, targetTable);
	}
	
	@Override
	public EmbeddedBeanMappingStrategy<C, Table> build(Dialect dialect) {
		return build(dialect, new Table<>(classToPersist.getSimpleName()));
	}
	
	@Override
	public <T extends Table> EmbeddedBeanMappingStrategy<C, T> build(Dialect dialect, T targetTable) {
		return new EmbeddedBeanMappingStrategy<>(classToPersist, targetTable, (Map) buildMapping(dialect, targetTable));
	}
	
	/**
	 * Small constract for mapping definition storage. See add(..) methods.
	 * 
	 * @param <T> property owner type
	 */
	protected abstract static class AbstractLinkage<T> implements Linkage<T> {
		
		/** Optional binder for this mapping */
		private ParameterBinder parameterBinder;
		
		private boolean nullable = true;
		
		private boolean setByConstructor = false;
		
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
	}
	
	/**
	 * Simple support for {@link Linkage}
	 * 
	 * @param <T> property owner type
	 */
	public static class LinkageByColumnName<T> extends AbstractLinkage<T> {
		
		private final IReversibleAccessor<T, ?> function;
		private final Class<?> columnType;
		/** Column name override if not default */
		@Nullable
		private final String columnName;
		
		/**
		 * Constructor by {@link IReversibleAccessor}
		 *
		 * @param accessor a {@link IReversibleAccessor}
		 * @param columnType the Java type of the column, will be converted to sql type thanks to {@link org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping}
		 * @param columnName an override of the default name that will be generated
		 */
		public <O> LinkageByColumnName(IReversibleAccessor<T, O> accessor, Class<O> columnType, @Nullable String columnName) {
			this.function = accessor;
			this.columnType = columnType;
			this.columnName = columnName;
		}
		
		@Override
		public <O> IReversibleAccessor<T, O> getAccessor() {
			return (IReversibleAccessor<T, O>) function;
		}
		
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
	
	public abstract static class AbstractInset<SRC, TRGT> {
		private final Class<TRGT> embeddedClass;
		private final Method insetAccessor;
		/** Equivalent of {@link #insetAccessor} as a {@link PropertyAccessor}  */
		private final PropertyAccessor<SRC, TRGT> accessor;
		private final ValueAccessPointMap<String> overridenColumnNames = new ValueAccessPointMap<>();
		private final ValueAccessPointSet excludedProperties = new ValueAccessPointSet();
		
		protected AbstractInset(SerializableBiConsumer<SRC, TRGT> targetSetter, LambdaMethodUnsheller lambdaMethodUnsheller) {
			this.insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetSetter);
			this.accessor = new PropertyAccessor<>(
					new MutatorByMethod<SRC, TRGT>(insetAccessor).toAccessor(),
					new MutatorByMethodReference<>(targetSetter));
			// looking for the target type because its necessary to find its persister (and other objects)
			this.embeddedClass = Reflections.javaBeanTargetType(getInsetAccessor());
		}
		
		protected AbstractInset(SerializableFunction<SRC, TRGT> targetGetter, LambdaMethodUnsheller lambdaMethodUnsheller) {
			this.insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetGetter);
			this.accessor = new PropertyAccessor<>(
					new AccessorByMethodReference<>(targetGetter),
					new AccessorByMethod<SRC, TRGT>(insetAccessor).toMutator());
			// looking for the target type because its necessary to find its persister (and other objects)
			this.embeddedClass = Reflections.javaBeanTargetType(getInsetAccessor());
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
		
		public void overrideName(SerializableFunction methodRef, String columnName) {
			this.overridenColumnNames.put(new AccessorByMethodReference(methodRef), columnName);
		}
		
		public void overrideName(SerializableBiConsumer methodRef, String columnName) {
			this.overridenColumnNames.put(new MutatorByMethodReference(methodRef), columnName);
		}
		
		public void overrideName(AccessorChain accessorChain, String columnName) {
			this.overridenColumnNames.put(accessorChain, columnName);
		}
		
		public void exclude(SerializableBiConsumer methodRef) {
			this.excludedProperties.add(new MutatorByMethodReference(methodRef));
		}
		
		public void exclude(SerializableFunction methodRef) {
			this.excludedProperties.add(new AccessorByMethodReference(methodRef));
		}
	}
	
	/**
	 * Information storage of embedded mapping defined externally by an {@link EmbeddedBeanMappingStrategyBuilder},
	 * see {@link #embed(SerializableFunction, EmbeddedBeanMappingStrategyBuilder)}
	 * 
	 * @param <SRC>
	 * @param <TRGT>
	 * @see #embed(SerializableFunction, EmbeddedBeanMappingStrategyBuilder)}
	 * @see #embed(SerializableBiConsumer, EmbeddedBeanMappingStrategyBuilder)}
	 */
	public static class ImportedInset<SRC, TRGT> extends AbstractInset<SRC, TRGT> implements LambdaMethodUnsheller {
		
		private final EmbeddedBeanMappingStrategyBuilder<TRGT> beanMappingBuilder;
		private final LambdaMethodUnsheller lambdaMethodUnsheller;
		private final ValueAccessPointMap<Column> overridenColumns = new ValueAccessPointMap<>();
		
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
		
		@Override
		public Method captureLambdaMethod(SerializableFunction getter) {
			return this.lambdaMethodUnsheller.captureLambdaMethod(getter);
		}
		
		@Override
		public Method captureLambdaMethod(SerializableBiConsumer setter) {
			return this.lambdaMethodUnsheller.captureLambdaMethod(setter);
		}
		
		public void override(SerializableFunction methodRef, Column column) {
			this.overridenColumns.put(new AccessorByMethodReference(methodRef), column);
		}
		
		public ValueAccessPointMap<Column> getOverridenColumns() {
			return overridenColumns;
		}
	}
	
	/**
	 * Represents a property that embeds a complex type
	 *
	 * @param <SRC> the owner type
	 * @param <TRGT> the target type
	 */
	public static class Inset<SRC, TRGT> extends AbstractInset<SRC, TRGT> implements LambdaMethodUnsheller {
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
	}
	
	
	
}
