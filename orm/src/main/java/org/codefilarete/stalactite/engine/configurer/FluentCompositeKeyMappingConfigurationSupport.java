package org.codefilarete.stalactite.engine.configurer;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByField;
import org.codefilarete.reflection.AccessorByMember;
import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.MutatorByField;
import org.codefilarete.reflection.MutatorByMember;
import org.codefilarete.reflection.MutatorByMethod;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.CompositeKeyMappingConfiguration;
import org.codefilarete.stalactite.engine.CompositeKeyMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.CompositeKeyPropertyOptions;
import org.codefilarete.stalactite.engine.FluentCompositeKeyMappingBuilder;
import org.codefilarete.stalactite.engine.ImportedEmbedOptions;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry.EnumBindType;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.function.SerializableThrowingFunction;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.codefilarete.tool.function.ThreadSafeLazyInitializer;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * @author Guillaume Mary
 */
public class FluentCompositeKeyMappingConfigurationSupport<C> implements FluentCompositeKeyMappingBuilder<C>, LambdaMethodUnsheller,
		CompositeKeyMappingConfiguration<C> {
	
	private CompositeKeyMappingConfigurationProvider<? super C> superMappingBuilder;
	
	/** Owning class of mapped properties */
	private final Class<C> classToPersist;
	
	@Nullable
	private ColumnNamingStrategy columnNamingStrategy;
	
	/** Mapping definitions */
	protected final List<CompositeKeyLinkage> mapping = new ArrayList<>();
	
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
	public FluentCompositeKeyMappingConfigurationSupport(Class<C> classToPersist) {
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
		return (Collection) insets;
	}
	
	@Override
	public CompositeKeyMappingConfiguration<? super C> getMappedSuperClassConfiguration() {
		return superMappingBuilder == null ? null : superMappingBuilder.getConfiguration();
	}
	
	@Override
	@Nullable
	public ColumnNamingStrategy getColumnNamingStrategy() {
		return columnNamingStrategy;
	}
	
	@Override
	public List<CompositeKeyLinkage> getPropertiesMapping() {
		return mapping;
	}
	
	@Override
	public CompositeKeyMappingConfiguration<C> getConfiguration() {
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
	public FluentCompositeKeyMappingConfigurationSupport<C> withColumnNaming(ColumnNamingStrategy columnNamingStrategy) {
		this.columnNamingStrategy = columnNamingStrategy;
		return this;
	}
	
	/**
	 * Gives access to currently configured {@link Inset}. Made so one can access features of {@link Inset} which are wider than
	 * the one available through {@link FluentCompositeKeyMappingBuilder}.
	 * 
	 * @return the last {@link Inset} built by {@link #newInset(SerializableFunction, CompositeKeyMappingConfigurationProvider)}
	 * or {@link #newInset(SerializableBiConsumer, CompositeKeyMappingConfigurationProvider)}
	 */
	protected Inset<C, ?> currentInset() {
		return currentInset;
	}
	
	protected <O> Inset<C, O> newInset(SerializableFunction<C, O> getter, CompositeKeyMappingConfigurationProvider<? extends O> CompositeKeyMappingBuilder) {
		currentInset = new Inset<>(getter, CompositeKeyMappingBuilder, this);
		return (Inset<C, O>) currentInset;
	}
	
	protected <O> Inset<C, O> newInset(SerializableBiConsumer<C, O> setter, CompositeKeyMappingConfigurationProvider<? extends O> CompositeKeyMappingBuilder) {
		currentInset = new Inset<>(setter, CompositeKeyMappingBuilder, this);
		return (Inset<C, O>) currentInset;
	}
	
	@Override
	public <O> FluentCompositeKeyMappingBuilderPropertyOptions<C> map(SerializableBiConsumer<C, O> setter) {
		return wrapWithPropertyOptions(addMapping(setter));
	}
	
	@Override
	public <O> FluentCompositeKeyMappingBuilderPropertyOptions<C> map(SerializableFunction<C, O> getter) {
		return wrapWithPropertyOptions(addMapping(getter));
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
	
	<O> FluentCompositeKeyMappingBuilderPropertyOptions<C> wrapWithPropertyOptions(LinkageSupport<C, O> linkage) {
		return new MethodReferenceDispatcher()
				.redirect(CompositeKeyPropertyOptions.class, new CompositeKeyPropertyOptions() {
					
					@Override
					public CompositeKeyPropertyOptions columnName(String name) {
						linkage.setColumnName(name);
						return null;
					}
					
					@Override
					public CompositeKeyPropertyOptions fieldName(String name) {
						// Note that getField(..) will throw an exception if field is not found, at the opposite of findField(..)
						// Note that we use "classToPersist" for field lookup instead of setter/getter declaring class
						// because this one can be abstract/interface
						Field field = Reflections.getField(FluentCompositeKeyMappingConfigurationSupport.this.classToPersist, name);
						linkage.setField(field);
						return null;
					}
				}, true)
				.fallbackOn(this)
				.build((Class<FluentCompositeKeyMappingBuilderPropertyOptions<C>>) (Class) FluentCompositeKeyMappingBuilderPropertyOptions.class);
	}
	
	@Override
	public <E extends Enum<E>> FluentCompositeKeyMappingBuilderEnumOptions<C> mapEnum(SerializableBiConsumer<C, E> setter) {
		LinkageSupport<C, E> linkage = addMapping(setter);
		return wrapWithEnumOptions(linkage);
	}
	
	@Override
	public <E extends Enum<E>> FluentCompositeKeyMappingBuilderEnumOptions<C> mapEnum(SerializableFunction<C, E> getter) {
		LinkageSupport<C, E> linkage = addMapping(getter);
		return wrapWithEnumOptions(linkage);
	}
	
	<O extends Enum> FluentCompositeKeyMappingBuilderEnumOptions<C> wrapWithEnumOptions(LinkageSupport<C, O> linkage) {
		return new MethodReferenceDispatcher()
				.redirect(CompositeKeyEnumOptions.class, new CompositeKeyEnumOptions() {
					
					@Override
					public CompositeKeyEnumOptions byName() {
						setLinkageParameterBinder(EnumBindType.NAME);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public CompositeKeyEnumOptions byOrdinal() {
						setLinkageParameterBinder(EnumBindType.ORDINAL);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					private void setLinkageParameterBinder(EnumBindType ordinal) {
						linkage.setParameterBinder(ordinal.newParameterBinder(linkage.getColumnType()));
					}
				}, true)
				.redirect(CompositeKeyPropertyOptions.class, new CompositeKeyPropertyOptions() {
					
					@Override
					public CompositeKeyPropertyOptions columnName(String name) {
						linkage.setColumnName(name);
						return null;
					}
					
					@Override
					public CompositeKeyPropertyOptions fieldName(String name) {
						// Note that getField(..) will throw an exception if field is not found, at the opposite of findField(..)
						// Note that we use "classToPersist" for field lookup instead of setter/getter declaring class
						// because this one can be abstract/interface
						Field field = Reflections.getField(FluentCompositeKeyMappingConfigurationSupport.this.classToPersist, name);
						linkage.setField(field);
						return null;
					}
				}, true)
				.fallbackOn(this)
				.build((Class<FluentCompositeKeyMappingBuilderEnumOptions<C>>) (Class) FluentCompositeKeyMappingBuilderEnumOptions.class);
	}
	
	@Override
	public FluentCompositeKeyMappingBuilder<C> mapSuperClass(CompositeKeyMappingConfigurationProvider<? super C> superMappingConfiguration) {
		this.superMappingBuilder = superMappingConfiguration;
		return this;
	}
	
	@Override
	public <O> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> embed(
			SerializableFunction<C, O> getter,
			CompositeKeyMappingConfigurationProvider<? extends O> compositeKeyMappingBuilder) {
		return addImportedInset(newInset(getter, compositeKeyMappingBuilder));
	}
	
	@Override
	public <O> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> embed(
			SerializableBiConsumer<C, O> setter,
			CompositeKeyMappingConfigurationProvider<? extends O> compositeKeyMappingBuilder) {
		return addImportedInset(newInset(setter, compositeKeyMappingBuilder));
	}
	
	private <O> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> addImportedInset(Inset<C, O> inset) {
		insets.add(inset);
		return new MethodReferenceDispatcher()
				// Why capturing overrideName(AccessorChain, String) this way ? (I mean with the "one method" capture instead of the usual "interface methods capture")
				// Because of ... lazyness ;) : "interface method capture" (such as done with ImportedEmbedOptions) would have required a dedicated
				// interface (inheriting from ImportedEmbedOptions) to define overrideName(AccessorChain, String)
				.redirect((SerializableTriFunction<FluentCompositeKeyMappingConfigurationImportedEmbedOptions, SerializableFunction, String, FluentCompositeKeyMappingConfigurationImportedEmbedOptions>)
						FluentCompositeKeyMappingConfigurationImportedEmbedOptions::overrideName,
						(BiConsumer<SerializableFunction, String>) inset::overrideName)
				.redirect((SerializableTriFunction<FluentCompositeKeyMappingConfigurationImportedEmbedOptions, SerializableBiConsumer, String, FluentCompositeKeyMappingConfigurationImportedEmbedOptions>)
						FluentCompositeKeyMappingConfigurationImportedEmbedOptions::overrideName,
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
				.build((Class<FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O>>) (Class) FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions.class);
	}
	
	/**
	 * Small contract for mapping definition storage. See add(..) methods.
	 * 
	 * @param <T> property owner type
	 */
	protected static class LinkageSupport<T, O> implements CompositeKeyLinkage<T, O> {
		
		/**
		 * Optional binder for this mapping.
		 * May be not of column type in cases of converted data with {@link ParameterBinder#preApply(SerializableThrowingFunction)}
		 * or {@link ParameterBinder#thenApply(SerializableThrowingFunction)}.
		 */
		private ParameterBinder<Object> parameterBinder;
		
		@Nullable
		private String columnName;
		
		private final AccessorFieldLazyInitializer accessor = new AccessorFieldLazyInitializer();
		
		private SerializableFunction<T, ?> getter;
		
		private SerializableBiConsumer<T, ?> setter;
		
		private Field field;
		
		public LinkageSupport(SerializableFunction<T, ?> getter) {
			this.getter = getter;
		}
		
		public LinkageSupport(SerializableBiConsumer<T, ?> setter) {
			this.setter = setter;
		}
		
		public void setParameterBinder(ParameterBinder<?> parameterBinder) {
			this.parameterBinder = (ParameterBinder<Object>) parameterBinder;
		}
		
		@Override
		@Nullable
		public ParameterBinder<Object> getParameterBinder() {
			return parameterBinder;
		}
		
		public void setColumnName(String name) {
			this.columnName = name;
		}
		
		@Override
		public ReversibleAccessor<T, O> getAccessor() {
			return accessor.get();
		}
		
		@Nullable
		@Override
		public Field getField() {
			return field;
		}
		
		public void setField(Field field) {
			this.field = field;
		}
		
		@Nullable
		@Override
		public String getColumnName() {
			return this.columnName;
		}
		
		@Override
		public Class<O> getColumnType() {
			return (Class<O>) AccessorDefinition.giveDefinition(this.accessor.get()).getMemberType();
		}
		
		/**
		 * Internal class that computes a {@link PropertyAccessor} from getter or setter according to which one is set up
		 */
		private class AccessorFieldLazyInitializer extends ThreadSafeLazyInitializer<ReversibleAccessor<T, O>> {
			
			@Override
			protected ReversibleAccessor<T, O> createInstance() {
				Accessor<T, O> accessor = null;
				Mutator<T, O> mutator = null;
				if (LinkageSupport.this.getter != null) {
					accessor = (Accessor<T, O>) Accessors.accessorByMethodReference(LinkageSupport.this.getter);
					AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(accessor);
					
					String capitalizedProperty = Strings.capitalize(accessorDefinition.getName());
					String methodPrefix = boolean.class.equals(accessorDefinition.getMemberType()) || Boolean.class.equals(accessorDefinition.getMemberType())
							? "is"
							: "set";
					Method method = Reflections.findMethod(accessorDefinition.getDeclaringClass(), methodPrefix + capitalizedProperty, accessorDefinition.getMemberType());
					MutatorByMember<T, O, ?> propertySetter = null;
					if (method != null) {
						propertySetter = new MutatorByMethod<>(method);
					}
					if (propertySetter == null) {
						if (LinkageSupport.this.field != null) {
							propertySetter = new MutatorByField<>(LinkageSupport.this.field);
						} else {
							// NB: we use getField(..) instead of findField(..) because the latter returns null if field wasn't found
							// so AccessorByField will throw a NPE later whereas getField(..) throws a MemberNotFoundException which is clearer
							propertySetter = new MutatorByField<>(Reflections.getField(accessorDefinition.getDeclaringClass(), accessorDefinition.getName()));
						}
					}
					mutator = propertySetter;
				} else if (LinkageSupport.this.setter != null) {
					mutator = (Mutator<T, O>) Accessors.mutatorByMethodReference(LinkageSupport.this.setter);
					AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(mutator);
					AccessorByMember<T, O, ?> propertyGetter = Accessors.accessorByMethod(accessorDefinition.getDeclaringClass(), accessorDefinition.getName());
					if (propertyGetter == null) {
						if (LinkageSupport.this.field != null) {
							propertyGetter = new AccessorByField<>(LinkageSupport.this.field);
						} else {
							// NB: we use getField(..) instead of findField(..) because the latter returns null if field wasn't found
							// so AccessorByField will throw a NPE later whereas getField(..) throws a MemberNotFoundException which is clearer
							propertyGetter = new AccessorByField<>(Reflections.getField(accessorDefinition.getDeclaringClass(), accessorDefinition.getName()));
						}
					}
					accessor = propertyGetter;
				}
				return new PropertyAccessor<>(accessor, mutator);
			}
		}
	}
	
	/**
	 * Information storage of embedded mapping defined externally by an {@link CompositeKeyMappingConfigurationProvider},
	 * see {@link #embed(SerializableFunction, CompositeKeyMappingConfigurationProvider)}
	 *
	 * @param <SRC>
	 * @param <TRGT>
	 * @see #embed(SerializableFunction, CompositeKeyMappingConfigurationProvider)}
	 * @see #embed(SerializableBiConsumer, CompositeKeyMappingConfigurationProvider)}
	 */
	public static class Inset<SRC, TRGT> {
		private final Class<TRGT> embeddedClass;
		private final Method insetAccessor;
		/** Equivalent of {@link #insetAccessor} as a {@link PropertyAccessor}  */
		private final PropertyAccessor<SRC, TRGT> accessor;
		private final ValueAccessPointMap<SRC, String> overriddenColumnNames = new ValueAccessPointMap<>();
		private final ValueAccessPointSet<SRC> excludedProperties = new ValueAccessPointSet<>();
		private final CompositeKeyMappingConfigurationProvider<? extends TRGT> configurationProvider;
		private final ValueAccessPointMap<SRC, Column> overriddenColumns = new ValueAccessPointMap<>();
		
		
		Inset(SerializableBiConsumer<SRC, TRGT> targetSetter,
			  CompositeKeyMappingConfigurationProvider<? extends TRGT> configurationProvider,
			  LambdaMethodUnsheller lambdaMethodUnsheller) {
			this.insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetSetter);
			this.accessor = new PropertyAccessor<>(
					new MutatorByMethod<SRC, TRGT>(insetAccessor).toAccessor(),
					new MutatorByMethodReference<>(targetSetter));
			// looking for the target type because it's necessary to find its persister (and other objects)
			this.embeddedClass = Reflections.javaBeanTargetType(getInsetAccessor());
			this.configurationProvider = configurationProvider;
		}
		
		Inset(SerializableFunction<SRC, TRGT> targetGetter,
			  CompositeKeyMappingConfigurationProvider<? extends TRGT> configurationProvider,
			  LambdaMethodUnsheller lambdaMethodUnsheller) {
			this.insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetGetter);
			this.accessor = new PropertyAccessor<>(
					new AccessorByMethodReference<>(targetGetter),
					new AccessorByMethod<SRC, TRGT>(insetAccessor).toMutator());
			// looking for the target type because it's necessary to find its persister (and other objects)
			this.embeddedClass = Reflections.javaBeanTargetType(getInsetAccessor());
			this.configurationProvider = configurationProvider;
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
		
		public ValueAccessPointSet<SRC> getExcludedProperties() {
			return this.excludedProperties;
		}
		
		public ValueAccessPointMap<SRC, String> getOverriddenColumnNames() {
			return this.overriddenColumnNames;
		}
		
		public ValueAccessPointMap<SRC, Column> getOverriddenColumns() {
			return overriddenColumns;
		}
		
		public CompositeKeyMappingConfigurationProvider<TRGT> getConfigurationProvider() {
			return (CompositeKeyMappingConfigurationProvider<TRGT>) configurationProvider;
		}
		
		public void overrideName(SerializableFunction methodRef, String columnName) {
			this.overriddenColumnNames.put(new AccessorByMethodReference(methodRef), columnName);
		}
		
		public void overrideName(SerializableBiConsumer methodRef, String columnName) {
			this.overriddenColumnNames.put(new MutatorByMethodReference(methodRef), columnName);
		}
		
		public void overrideName(AccessorChain accessorChain, String columnName) {
			this.overriddenColumnNames.put(accessorChain, columnName);
		}
		
		public void override(SerializableFunction methodRef, Column column) {
			this.overriddenColumns.put(new AccessorByMethodReference(methodRef), column);
		}
		
		public void override(SerializableBiConsumer methodRef, Column column) {
			this.overriddenColumns.put(new MutatorByMethodReference(methodRef), column);
		}
		
		public void exclude(SerializableBiConsumer methodRef) {
			this.excludedProperties.add(new MutatorByMethodReference(methodRef));
		}
		
		public void exclude(SerializableFunction methodRef) {
			this.excludedProperties.add(new AccessorByMethodReference(methodRef));
		}
	}
}
