package org.codefilarete.stalactite.engine.configurer;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.reflection.MutatorByMethod;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.ColumnLinkageOptions;
import org.codefilarete.stalactite.dsl.property.EnumOptions;
import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilder;
import org.codefilarete.stalactite.dsl.embeddable.ImportedEmbedOptions;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.property.PropertyOptions;
import org.codefilarete.stalactite.engine.configurer.PropertyAccessorResolver.PropertyMapping;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry.EnumBindType;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.codefilarete.tool.function.ThreadSafeLazyInitializer;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * @author Guillaume Mary
 */
public class FluentEmbeddableMappingConfigurationSupport<C> implements FluentEmbeddableMappingBuilder<C>, LambdaMethodUnsheller,
		EmbeddableMappingConfiguration<C> {
	
	@Nullable
	private EmbeddableMappingConfigurationProvider<? super C> superMappingBuilder;
	
	/** Owning class of mapped properties */
	private final Class<C> classToPersist;
	
	@Nullable
	private ColumnNamingStrategy columnNamingStrategy;
	
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
						// Note that getField(..) will throw an exception if field is not found, at the opposite of findField(..)
						// Note that we use "classToPersist" for field lookup instead of setter/getter declaring class
						// because this one can be abstract/interface
						Field field = Reflections.getField(FluentEmbeddableMappingConfigurationSupport.this.classToPersist, name);
						linkage.setField(field);
						return null;
					}
					
					@Override
					public PropertyOptions<O> readConverter(Converter<O, O> converter) {
						linkage.setReadConverter(converter);
						return null;
					}
					
					@Override
					public PropertyOptions<O> writeConverter(Converter<O, O> converter) {
						linkage.setWriteConverter(converter);
						return null;
					}
					
					@Override
					public <V> PropertyOptions<O> sqlBinder(ParameterBinder<V> parameterBinder) {
						linkage.setParameterBinder(parameterBinder);
						return null;
					}
				}, true)
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
	
	<E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C, E> wrapWithEnumOptions(LinkageSupport<C, E> linkage) {
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
						Field field = Reflections.findField(FluentEmbeddableMappingConfigurationSupport.this.classToPersist, name);
						if (field == null) {
							throw new MappingConfigurationException(("Field " + name
									+ " was not found in " + Reflections.toString(FluentEmbeddableMappingConfigurationSupport.this.classToPersist)));
						}
						linkage.setField(field);
						return null;
					}
					
					@Override
					public EnumOptions<E> readConverter(Converter<E, E> converter) {
						linkage.setReadConverter(converter);
						return null;
					}
					
					@Override
					public EnumOptions<E> writeConverter(Converter<E, E> converter) {
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
	
	/**
	 * Small contract for mapping definition storage. See add(..) methods.
	 * 
	 * @param <T> property owner type
	 * @param <O> property type
	 */
	public static class LinkageSupport<T, O> implements Linkage<T, O> {
		
		/** Optional binder for this mapping */
		private ParameterBinder<?> parameterBinder;
		
		@Nullable
		private EnumBindType enumBindType;
		
		private boolean nullable = true;
		
		private boolean setByConstructor = false;
		
		private LocalColumnLinkageOptions columnOptions = new ColumnLinkageOptionsSupport();
		
		private final ThreadSafeLazyInitializer<ReversibleAccessor<T, O>> accessor;
		
		private SerializableFunction<T, O> getter;
		
		private SerializableBiConsumer<T, O> setter;
		
		private Field field;
		
		private boolean readonly;
		
		private String extraTableName;
		
		private Converter<? /* value coming from database */, O> readConverter;
		
		private Converter<O, ? /* value going to database */> writeConverter;
		
		public LinkageSupport(SerializableFunction<T, O> getter) {
			this.getter = getter;
			this.accessor = new AccessorFieldLazyInitializer();
		}
		
		public LinkageSupport(SerializableBiConsumer<T, O> setter) {
			this.setter = setter;
			this.accessor = new AccessorFieldLazyInitializer();
		}
		
		SerializableFunction<T, O> getGetter() {
			return getter;
		}
		
		SerializableBiConsumer<T, O> getSetter() {
			return setter;
		}
		
		@Override
		@Nullable
		public ParameterBinder<Object> getParameterBinder() {
			return (ParameterBinder<Object>) parameterBinder;
		}
		
		public void setParameterBinder(@Nullable ParameterBinder<?> parameterBinder) {
			this.parameterBinder = parameterBinder;
		}
		
		@Override
		@Nullable
		public EnumBindType getEnumBindType() {
			return enumBindType;
		}
		
		public void setEnumBindType(@Nullable EnumBindType enumBindType) {
			this.enumBindType = enumBindType;
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
		
		public LocalColumnLinkageOptions getColumnOptions() {
			return columnOptions;
		}
		
		public void setColumnOptions(LocalColumnLinkageOptions columnOptions) {
			this.columnOptions = columnOptions;
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
			return nullable(this.columnOptions).map(ColumnLinkageOptions::getColumnName).get();
		}
		
		@Nullable
		@Override
		public Size getColumnSize() {
			return nullable(this.columnOptions).map(ColumnLinkageOptions::getColumnSize).get();
		}
		
		@Override
		public Class<O> getColumnType() {
			return this.columnOptions instanceof ColumnLinkageOptionsByColumn
				? (Class<O>) ((ColumnLinkageOptionsByColumn) this.columnOptions).getColumnType()
				: AccessorDefinition.giveDefinition(this.accessor.get()).getMemberType();
		}
		
		@Override
		public boolean isReadonly() {
			return readonly;
		}
		
		public void readonly() {
			this.readonly = true;
		}
		
		@Override
		public String getExtraTableName() {
			return extraTableName;
		}
		
		public void setExtraTableName(String extraTableName) {
			this.extraTableName = extraTableName;
		}
		
		@Override
		public Converter<?, O> getReadConverter() {
			return readConverter;
		}
		
		public void setReadConverter(Converter<?, O> readConverter) {
			this.readConverter = readConverter;
		}
		
		@Override
		public Converter<O, ?> getWriteConverter() {
			return writeConverter;
		}
		
		public void setWriteConverter(Converter<O, ?> writeConverter) {
			this.writeConverter = writeConverter;
		}
		
		/**
		 * Internal class that computes a {@link PropertyAccessor} from getter or setter according to which one is set up
		 */
		private class AccessorFieldLazyInitializer extends ThreadSafeLazyInitializer<ReversibleAccessor<T, O>> {
			
			@Override
			protected ReversibleAccessor<T, O> createInstance() {
				return new PropertyAccessorResolver<>(new PropertyMapping<T, O>() {
					@Override
					public SerializableFunction<T, O> getGetter() {
						return LinkageSupport.this.getter;
					}
					
					@Override
					public SerializableBiConsumer<T, O> getSetter() {
						return LinkageSupport.this.setter;
					}
					
					@Override
					public Field getField() {
						return LinkageSupport.this.getField();
					}
				}).resolve();
			}
		}
	}
	
	public interface LocalColumnLinkageOptions extends ColumnLinkageOptions {
		
		void setColumnName(String columnName);
		
		void setColumnSize(Size columnSize);
	}
	
	static class ColumnLinkageOptionsSupport implements LocalColumnLinkageOptions {
		
		private String columnName;
		
		private Size columnSize;
		
		ColumnLinkageOptionsSupport() {
		}
		
		ColumnLinkageOptionsSupport(@Nullable String columnName) {
			this.columnName = columnName;
		}
		
		ColumnLinkageOptionsSupport(@Nullable Size columnSize) {
			this.columnSize = columnSize;
		}
		
		@Nullable
		@Override
		public String getColumnName() {
			return this.columnName;
		}
		
		@Override
		public void setColumnName(String columnName) {
			this.columnName = columnName;
		}
		
		@Nullable
		@Override
		public Size getColumnSize() {
			return columnSize;
		}
		
		@Override
		public void setColumnSize(Size columnSize) {
			this.columnSize = columnSize;
		}
	}
	
	static class ColumnLinkageOptionsByColumn implements LocalColumnLinkageOptions {
		
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
		
		@Nullable
		@Override
		public Size getColumnSize() {
			return this.column.getSize();
		}
		
		@Override
		public void setColumnName(String columnName) {
			// no-op, column is already defined
		}
		
		@Override
		public void setColumnSize(Size columnSize) {
			// no-op, column is already defined
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
		
		static <SRC, TRGT> Inset<SRC, TRGT> fromSetter(SerializableBiConsumer<SRC, TRGT> targetSetter,
													   EmbeddableMappingConfigurationProvider<? extends TRGT> beanMappingBuilder,
													   LambdaMethodUnsheller lambdaMethodUnsheller) {
			Method insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetSetter);
			return new Inset<>(insetAccessor,
					new PropertyAccessor<>(
							new MutatorByMethod<SRC, TRGT>(insetAccessor).toAccessor(),
							new MutatorByMethodReference<>(targetSetter)),
					beanMappingBuilder);
		}
		
		static <SRC, TRGT> Inset<SRC, TRGT> fromGetter(SerializableFunction<SRC, TRGT> targetGetter,
													   EmbeddableMappingConfigurationProvider<? extends TRGT> beanMappingBuilder,
													   LambdaMethodUnsheller lambdaMethodUnsheller) {
			Method insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetGetter);
			return new Inset<>(insetAccessor,
					new PropertyAccessor<>(
							new AccessorByMethodReference<>(targetGetter),
							new AccessorByMethod<SRC, TRGT>(insetAccessor).toMutator()),
					beanMappingBuilder);
		}
		
		private final Class<TRGT> embeddedClass;
		private final Method insetAccessor;
		/** Equivalent of {@link #insetAccessor} as a {@link PropertyAccessor}  */
		private final Accessor<SRC, TRGT> accessor;
		private final ValueAccessPointMap<SRC, String> overriddenColumnNames = new ValueAccessPointMap<>();
		private final ValueAccessPointMap<SRC, Size> overriddenColumnSizes = new ValueAccessPointMap<>();
		private final ValueAccessPointSet<SRC> excludedProperties = new ValueAccessPointSet<>();
		private final EmbeddableMappingConfigurationProvider<? extends TRGT> configurationProvider;
		private final ValueAccessPointMap<SRC, Column> overriddenColumns = new ValueAccessPointMap<>();
		
		
		Inset(SerializableBiConsumer<SRC, TRGT> targetSetter,
			  EmbeddableMappingConfigurationProvider<? extends TRGT> configurationProvider,
			  LambdaMethodUnsheller lambdaMethodUnsheller) {
			this.insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetSetter);
			this.accessor = new PropertyAccessor<>(
					new MutatorByMethod<SRC, TRGT>(insetAccessor).toAccessor(),
					new MutatorByMethodReference<>(targetSetter));
			// looking for the target type because it's necessary to find its persister (and other objects)
			this.embeddedClass = Reflections.javaBeanTargetType(getInsetAccessor());
			this.configurationProvider = configurationProvider;
		}

		Inset(Method insetAccessor,
			  Accessor<SRC, TRGT> accessor,
			  EmbeddableMappingConfigurationProvider<? extends TRGT> configurationProvider) {
			this.insetAccessor = insetAccessor;
			// looking for the target type because it's necessary to find its persister (and other objects)
			this.embeddedClass = Reflections.javaBeanTargetType(insetAccessor);
			this.accessor = accessor;
			this.configurationProvider = configurationProvider;
		}
		
		/**
		 * Equivalent of {@link #insetAccessor} as a {@link PropertyAccessor}
		 */
		public Accessor<SRC, TRGT> getAccessor() {
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
		
		public ValueAccessPointMap<SRC, Size> getOverriddenColumnSizes() {
			return overriddenColumnSizes;
		}
		
		public ValueAccessPointMap<SRC, Column> getOverriddenColumns() {
			return overriddenColumns;
		}
		
		public EmbeddableMappingConfigurationProvider<TRGT> getConfigurationProvider() {
			return (EmbeddableMappingConfigurationProvider<TRGT>) configurationProvider;
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
		
		public void overrideSize(SerializableFunction methodRef, Size columnSize) {
			this.overriddenColumnSizes.put(new AccessorByMethodReference(methodRef), columnSize);
		}
		
		public void overrideSize(SerializableBiConsumer methodRef, Size columnSize) {
			this.overriddenColumnSizes.put(new MutatorByMethodReference<>(methodRef), columnSize);
		}
		
		public void overrideSize(AccessorChain accessorChain, Size columnSize) {
			this.overriddenColumnSizes.put(accessorChain, columnSize);
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
