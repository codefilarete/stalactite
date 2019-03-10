package org.gama.stalactite.persistence.engine;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Reflections;
import org.gama.lang.StringAppender;
import org.gama.lang.Strings;
import org.gama.lang.bean.FieldIterator;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.SerializableTriFunction;
import org.gama.lang.reflect.MethodDispatcher;
import org.gama.reflection.AccessorByMethod;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.AccessorChain;
import org.gama.reflection.AccessorChainMutator;
import org.gama.reflection.Accessors;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.MethodReferences;
import org.gama.reflection.MutatorByMethod;
import org.gama.reflection.MutatorByMethodReference;
import org.gama.reflection.PropertyAccessor;
import org.gama.sql.dml.SQLStatement.BindingException;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.bean.Objects.preventNull;
import static org.gama.reflection.MethodReferences.toMethodReferenceString;
import static org.gama.stalactite.persistence.engine.AccessorChainComparator.accessComparator;

/**
 * @author Guillaume Mary
 */
public class FluentEmbeddableMappingConfigurationSupport<C> implements IFluentEmbeddableMappingBuilder<C>, LambdaMethodUnsheller {
	
	/**
	 * Will start a {@link FluentMappingBuilder} for a given class which will target a table that as the class name.
	 *
	 * @param persistedClass the class to be persisted by the {@link EmbeddedBeanMappingStrategy} that will be created by {@link #buildMapping(Dialect, Table)}}
	 * @param <T> any type to be persisted
	 * @return a new {@link FluentMappingBuilder}
	 */
	public static <T extends Identified> IFluentEmbeddableMappingBuilder<T> from(Class<T> persistedClass) {
		return new FluentEmbeddableMappingConfigurationSupport<>(persistedClass);
	}
	
	/** Owning class of mapped properties */
	private final Class<C> persistedClass;
	
	/** Mapiing defintions */
	private final List<Linkage> mapping = new ArrayList<>();
	
	/** Collection of embedded elements, even inner ones to help final build process */
	private final Collection<AbstractInset<C, ?>> insets = new ArrayList<>();
	
	private final Map<Class<? super C>, EmbeddedBeanMappingStrategy<? super C, ?>> inheritanceMapping = new HashMap<>();
	
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
	
	@Override
	public Method captureLambdaMethod(SerializableFunction getter) {
		return this.methodSpy.findMethod(getter);
	}
	
	@Override
	public Method captureLambdaMethod(SerializableBiConsumer setter) {
		return this.methodSpy.findMethod(setter);
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
		return Reflections.propertyName(method);
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
	public IFluentEmbeddableMappingBuilder<C> mapSuperClass(Class<? super C> superType, EmbeddedBeanMappingStrategy<? super C, ?> mappingStrategy) {
		inheritanceMapping.put(superType, mappingStrategy);
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
		insets.add(importedInset);
		return new MethodDispatcher()
				.redirect(EmbedingEmbeddableOptions.class, new EmbedingEmbeddableOptions<C>() {
					
					@Override
					public <IN> EmbedingEmbeddableOptions<C> overrideName(SerializableFunction<C, IN> function, String columnName) {
						importedInset.overrideName(function, columnName);
						return null;
					}
					
					@Override
					public <IN> EmbedingEmbeddableOptions<C> overrideName(SerializableBiConsumer<C, IN> function, String columnName) {
						importedInset.overrideName(function, columnName);
						return null;
					}
				}, true)
				.fallbackOn(this)
				.build((Class<IFluentEmbeddableMappingBuilderEmbeddableOptions<C, O>>) (Class) IFluentEmbeddableMappingBuilderEmbeddableOptions.class);
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilderEmbeddableOptions<C, O> embed(SerializableBiConsumer<C, O> getter, EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder) {
		ImportedInset<C, O> importedInset = new ImportedInset<>(getter, this, embeddableMappingBuilder);
		insets.add(importedInset);
		return new MethodDispatcher()
				.redirect(EmbedingEmbeddableOptions.class, new EmbedingEmbeddableOptions<C>() {
					
					@Override
					public <IN> EmbedingEmbeddableOptions<C> overrideName(SerializableFunction<C, IN> function, String columnName) {
						importedInset.overrideName(function, columnName);
						return null;
					}
					
					@Override
					public <IN> EmbedingEmbeddableOptions<C> overrideName(SerializableBiConsumer<C, IN> function, String columnName) {
						importedInset.overrideName(function, columnName);
						return null;
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
		return new Builder(dialect, targetTable).build();
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
	}
	
	static abstract class AbstractInset<SRC, TRGT> {
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
		
		public abstract Set<Field> getExcludedProperties();
		
		public abstract Map<Field, String> getOverridenColumnNames();
	}
	
	static class ImportedInset<SRC, TRGT> extends AbstractInset<SRC, TRGT> implements LambdaMethodUnsheller {
		
		private final EmbeddedBeanMappingStrategyBuilder<TRGT> beanMappingBuilder;
		private final Map<Field, String> overridenColumnNames = new HashMap<>();
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
			Method method = captureLambdaMethod(methodRef);
			this.overridenColumnNames.put(Reflections.wrappedField(method), columnName);
		}
		
		public void overrideName(SerializableBiConsumer methodRef, String columnName) {
			Method method = captureLambdaMethod(methodRef);
			this.overridenColumnNames.put(Reflections.wrappedField(method), columnName);
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
		public Set<Field> getExcludedProperties() {
			return Collections.EMPTY_SET;
		}
		
		@Override
		public Map<Field, String> getOverridenColumnNames() {
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
		private final Map<Field, String> overridenColumnNames = new HashMap<>();
		private final Set<Field> excludedProperties = new HashSet<>();
		private final LambdaMethodUnsheller lambdaMethodUnsheller;
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
			Method method = captureLambdaMethod(methodRef);
			this.overridenColumnNames.put(Reflections.wrappedField(method), columnName);
		}
		
		public void exclude(SerializableBiConsumer methodRef) {
			Method method = captureLambdaMethod(methodRef);
			this.excludedProperties.add(Reflections.wrappedField(method));
		}
		
		public void exclude(SerializableFunction methodRef) {
			Method method = captureLambdaMethod(methodRef);
			this.excludedProperties.add(Reflections.wrappedField(method));
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
		public Map<Field, String> getOverridenColumnNames() {
			return overridenColumnNames;
		}
		
		@Override
		public Set<Field> getExcludedProperties() {
			return excludedProperties;
		}
	}
	
	/**
	 * Engine that converts mapping definition of the enclosing instance {@link FluentEmbeddableMappingConfigurationSupport} into a simple {@link Map}
	 *
	 * @see #build()  
	 */
	class Builder {
		
		private final Dialect dialect;
		private final Table targetTable;
		/** Result of {@link #build()}, shared between methods */
		private final Map<IReversibleAccessor, Column> result = new HashMap<>();
		
		Builder(Dialect dialect, Table targetTable) {
			this.dialect = dialect;
			this.targetTable = targetTable;
		}
		
		public Dialect getDialect() {
			return dialect;
		}
		
		public Table getTargetTable() {
			return targetTable;
		}
		
		/**
		 * Converts mapping definition of the enclosing instance {@link FluentEmbeddableMappingConfigurationSupport} into a simple {@link Map} 
		 * 
		 * @return a {@link Map} representing the definition of the mapping done through the fluent API of {@link FluentEmbeddableMappingConfigurationSupport} 
		 */
		Map<IReversibleAccessor, Column> build() {
			// first we add mapping coming from inheritance, then it can be overwritten by class mapping 
			result.putAll(buildMappingFromInheritance());
			// converting direct mapping
			mapping.forEach(linkage -> {
				Column column = addLinkage(linkage);
				result.put(linkage.getAccessor(), column);
			});
			// adding embeddable (no particular thinking about order compared to previous inherited & class mapping) 
			result.putAll(buildEmbeddedMapping());
			return result;
		}
		
		protected Column addLinkage(Linkage linkage) {
			Column column = targetTable.addColumn(linkage.getColumnName(), linkage.getColumnType());
			// assert that column binder is registered : it will throw en exception if the binder is not found
			try {
				dialect.getColumnBinderRegistry().getBinder(column);
			} catch (BindingException e) {
				throw new MappingConfigurationException(column.getAbsoluteName() + " has no matching binder,"
						+ " please consider adding one to dialect binder registry or use one of the "
						+ toMethodReferenceString(
								(SerializableBiFunction<IFluentEmbeddableMappingConfiguration, SerializableFunction, IFluentEmbeddableMappingConfigurationEmbedOptions>)
										IFluentEmbeddableMappingConfiguration::embed) + " methods"
				);
			}
			return column;
		}
		
		protected Map<IReversibleAccessor, Column> buildMappingFromInheritance() {
			Map<IReversibleAccessor, Column> inheritanceResult = new HashMap<>();
			inheritanceMapping.forEach((superType, embeddableMappingStrategy) -> inheritanceResult.putAll(collectMapping(embeddableMappingStrategy, targetTable)));
			return inheritanceResult;
		}
		
		/**
		 * Collects mapping of given {@link EmbeddedBeanMappingStrategy} and created equivalent {@link Column} on the given target table.
		 * 
		 * @param embeddableMappingStrategy the source of mapping 
		 * @param localTargetTable the table on which columns must be added
		 * @return a mapping between properties of {@link EmbeddedBeanMappingStrategy#getPropertyToColumn()} and their column in our {@link #getTargetTable()}
		 */
		protected Map<IReversibleAccessor, Column> collectMapping(EmbeddedBeanMappingStrategy<? super C, ?> embeddableMappingStrategy,
																  Table localTargetTable) {
			Map<IReversibleAccessor, Column> embeddedBeanResult = new HashMap<>();
			Map<? extends IReversibleAccessor<? super C, Object>, ? extends Column<?, Object>> propertyToColumn =
					embeddableMappingStrategy.getPropertyToColumn();
			propertyToColumn.forEach((accessor, column) -> {
				Column projectedColumn = localTargetTable.addColumn(column.getName(), column.getJavaType());
				projectedColumn.setAutoGenerated(column.isAutoGenerated());
				projectedColumn.setNullable(column.isNullable());
				embeddedBeanResult.put(accessor, projectedColumn);
			});
			return embeddedBeanResult;
		}
		
		private Map<IReversibleAccessor, Column> buildEmbeddedMapping() {
			Map<String, Column<Table, Object>> columnsPerName = targetTable.mapColumnsOnName();
			Map<IReversibleAccessor, Column> toReturn = new HashMap<>();
			Set<AbstractInset<C, ?>> treatedInsets = new HashSet<>();
			// Registry of mapped "properties" to skip inner embeds, check duplicates, etc.
			Set<Member> insetsPropertyRegistry = new TreeSet<>(new Comparator<Member>() {
				@Override
				public int compare(Member o1, Member o2) {
					// implementation done so that members are equal if they represent the same property (we don't care about type because 
					// there can't be any misconfiguration on it at this step)
					int classGap = o1.getDeclaringClass().hashCode() - o2.getDeclaringClass().hashCode();
					int nameGap = properyName(o1).hashCode() - properyName(o2).hashCode();
					return classGap * 31 + nameGap;
				}
				
				private String properyName(Member member) {
					if (member instanceof Method) {
						return Reflections.propertyName((Method) member);
					} else {
						return member.getName();
					}
				}
			});
			insets.forEach(i -> insetsPropertyRegistry.add(i.getInsetAccessor()));
			
			// starting mapping
			insets.forEach(inset -> {
				assertNotAlreadyMapped(inset, treatedInsets);
				
				if (inset instanceof ImportedInset) {
					Table pawnTable = new Table("pawnTable");
					EmbeddedBeanMappingStrategy embeddedBeanMappingStrategy = ((ImportedInset<C, ?>) inset).getBeanMappingBuilder().build(dialect, pawnTable);
					Map<IReversibleAccessor, Column> embeddableMapping = embeddedBeanMappingStrategy.getPropertyToColumn();
					// looking for conflicting columns between those expected by embeddable bean and those of the entity
					// Note : we need to access "result" instead of "toReturn" because "toReturn" is too local : dedicated to embedded whereas
					// conflict may occur with single property mapping
					Map<IReversibleAccessor, Column> intermediaryResult = new HashMap<>();
					intermediaryResult.putAll(result);
					intermediaryResult.putAll(toReturn);
					Map<String, IReversibleAccessor> alreadyMappedPropertyPerColumnName = intermediaryResult.entrySet().stream()
							.collect(Collectors.toMap(e -> e.getValue().getName(), Entry::getKey));
					Map<IReversibleAccessor, Column> notOverridenEmbeddedProperties = new TreeMap<>(accessComparator());
					notOverridenEmbeddedProperties.putAll(embeddableMapping);
					inset.getOverridenColumnNames().forEach((field, nameOverride) -> notOverridenEmbeddedProperties.remove(Accessors.mutatorByField(field)));
					
					
					// conflicting mapping are properties which column names are not overriden
					Set<String> mappedPropertiesColumnNames = alreadyMappedPropertyPerColumnName.keySet();
					Map<String, IReversibleAccessor> conflictingMapping = notOverridenEmbeddedProperties.entrySet().stream()
							.filter(embeddedEntry -> {
								String embeddedColumnName = embeddedEntry.getValue().getName();
								return mappedPropertiesColumnNames.contains(embeddedColumnName);
							})
							.collect(Collectors.toMap(e -> e.getValue().getName(), Entry::getKey));
					
					if (!conflictingMapping.isEmpty()) {
						// looking for conflicting properties for better exception message
						class Conflict {
							private final IReversibleAccessor embeddableBeanProperty;
							private final IReversibleAccessor entityProperty;
							private final String columnName;
							
							private Conflict(IReversibleAccessor embeddableBeanProperty, IReversibleAccessor entityProperty, String columnName) {
								this.embeddableBeanProperty = embeddableBeanProperty;
								this.entityProperty = entityProperty;
								this.columnName = columnName;
							}
							
							@Override
							public String toString() {
								return "Embeddable definition '" + AccessorChainComparator.toString(embeddableBeanProperty)
										+ "' vs entity definition '" + AccessorChainComparator.toString(entityProperty)
										+ "' on column name '" + columnName + "'";
							}
						}
						List<Conflict> conflicts = new ArrayList<>();
						for (Entry<String, IReversibleAccessor> conflict : conflictingMapping.entrySet()) {
							conflicts.add(new Conflict(conflict.getValue(), alreadyMappedPropertyPerColumnName.get(conflict.getKey()), conflict.getKey()));
						}
						throw new MappingConfigurationException("Some embedded columns conflict with entity ones on their name, please override it or change it :"
								+ System.lineSeparator()
								+ new StringAppender().ccat(conflicts, System.lineSeparator()));
					}
					
					Map<IReversibleAccessor, Column> map = collectMapping(embeddedBeanMappingStrategy, targetTable);
					
					Map<IReversibleAccessor, String> nameOverrides = new HashMap<>();
					
					// Overriding column name, problem is that we get a IReversibleAccessor from EmbeddedBeanMappingStrategy whereas we have
					// Fields from ImportedInset, so it can hardly match to find out which Column must have its name overriden ...
					// NB : since ther's no common ancestor to IAccessor and IMutator generics type of Set is Object
					SortedSet<Object> embeddedBeanMappingRegistry = new TreeSet<>(accessComparator());
					embeddedBeanMappingRegistry.addAll(embeddableMapping.keySet());
					inset.getOverridenColumnNames().forEach((field, nameOverride) -> {
						if (embeddedBeanMappingRegistry.contains(Accessors.mutatorByField(field))) {
							Column addedColumn = targetTable.addColumn(nameOverride, field.getType());
							// adding the newly created column to our index for next iterations because it may conflicts with mapping of other iterations
							columnsPerName.put(nameOverride, addedColumn);
						} else {
							// Case : property is not mapped by embeddable strategy, so overriding it has no purpose
							String methodSignature = toMethodReferenceString(Reflections.getMethod(field.getDeclaringClass(), "get" + Strings.capitalize(field.getName())));
							throw new MappingConfigurationException(methodSignature + " is not mapped by embeddable strategy, so its column name override '" + nameOverride + "' can't apply");
						}
					});
					toReturn.putAll(embeddableMapping);
				} else if (inset instanceof Inset) {
					Inset refinedInset = (Inset) inset;
					// Building the mapping of the value-object's fields to the table
					FieldIterator fieldIterator = new FieldIterator(inset.getEmbeddedClass());
					// NB: we skip fields that are registered as inset (inner embeddable) because they'll be treated by their own, skipping them avoids conflicts
					Predicate<Field> innerEmbeddableExcluder = Objects.not(insetsPropertyRegistry::contains);
					// we also skip excluded fields by user
					Predicate<Field> excludedFieldsExcluder = Objects.not(inset.getExcludedProperties()::contains);
					Iterables.consume(fieldIterator, innerEmbeddableExcluder.and(excludedFieldsExcluder), (field, i) -> {
						// looking for the targeted column
						Column targetColumn = findColumn(field, columnsPerName, refinedInset);
						if (targetColumn == null) {
							// Column isn't declared in table => we create one from field informations
							String columnName = field.getName();
							String overridenName = inset.getOverridenColumnNames().get(field);
							if (overridenName != null) {
								columnName = overridenName;
							}
							targetColumn = targetTable.addColumn(columnName, field.getType());
							// adding the newly created column to our index for next iterations because it may conflicts with mapping of other iterations
							columnsPerName.put(columnName, targetColumn);
						} else {
							// checking that column is not already mapped by a previous definition
							Column finalTargetColumn = targetColumn;
							Optional<Entry<IReversibleAccessor, Column>> existingMapping =
									Builder.this.result.entrySet().stream().filter(entry -> entry.getValue().equals(finalTargetColumn)).findFirst();
							if (existingMapping.isPresent()) {
								Method currentMethod = inset.getInsetAccessor();
								String currentMethodReference = toMethodReferenceString(currentMethod);
								throw new MappingConfigurationException("Error while mapping "
										+ currentMethodReference + " : field " + Reflections.toString(field.getDeclaringClass()) + '.' + field.getName()
										+ " conflicts with " + existingMapping.get().getKey() + " because they use same column" +
										", override one of their name to avoid the conflict" +
										", see " + MethodReferences.toMethodReferenceString(
										(SerializableTriFunction<EmbedOptions, SerializableFunction, String, EmbedOptions>) EmbedOptions::overrideName));
							}
						}
						
						// we create a chain that
						// - returns null when beans are not instanciated, so null will be inserted/updated in embedded columns
						// - initializes values when its mutator will be used, so bean will create its embedded properties on select
						// (voluntary dissimetric behavior)
						Iterable<Inset> insetParentIterable = () -> new HierarchyIterator(refinedInset);
						List<PropertyAccessor> chain = Iterables.collectToList(insetParentIterable, Inset::getAccessor);
						Collections.reverse(chain);
						
						chain.add(Accessors.of(field));
						AccessorChain c = new AccessorChain(chain) {
							@Override
							public AccessorChainMutator toMutator() {
								AccessorChainMutator toReturn = super.toMutator();
								toReturn.setNullValueHandler(AccessorChain.INITIALIZE_VALUE);
								return toReturn;
							}
							
							@Override
							public String toString() {
								return inset.getAccessor() + " > " + field.getDeclaringClass().getSimpleName() + "." + field.getName();
							}
						}.setNullValueHandler(AccessorChain.RETURN_NULL);
						toReturn.put(c, targetColumn);
					});
				}
				treatedInsets.add(inset);
			});
			return toReturn;
		}
		
		private void assertNotAlreadyMapped(AbstractInset<C, ?> inset, Set<AbstractInset<C, ?>> treatedInsets) {
			Optional<AbstractInset<C, ?>> alreadyMappedType = treatedInsets.stream().filter(i -> i.getEmbeddedClass() == inset.getEmbeddedClass()).findFirst();
			if (alreadyMappedType.isPresent()) {
				// accessors are exactly the same ?
				if (alreadyMappedType.get().getInsetAccessor().equals(inset.getInsetAccessor())) {
					Method currentMethod = inset.getInsetAccessor();
					String currentMethodReference = toMethodReferenceString(currentMethod);
					throw new MappingConfigurationException(currentMethodReference + " is already mapped");
				}
				if (inset instanceof Inset<?, ?>) {
				// else : type is already mapped throught a different property
				// we're going to check if all subproperties override their column name, else an exception will be thrown
				// to prevent 2 properties from being mapped on same column
				Set<Field> expectedOverridenFields = new HashSet<>(Iterables.copy(new FieldIterator(inset.getEmbeddedClass())));
				expectedOverridenFields.removeAll(inset.getExcludedProperties());
				Set<Field> overridenFields = inset.getOverridenColumnNames().keySet();
				boolean allFieldsAreOverriden = overridenFields.equals(expectedOverridenFields);
				if (!allFieldsAreOverriden) {
					Method currentMethod = inset.getInsetAccessor();
					String currentMethodReference = toMethodReferenceString(currentMethod);
					Method conflictingMethod = alreadyMappedType.get().getInsetAccessor();
					String conflictingDeclaration = toMethodReferenceString(conflictingMethod);
					expectedOverridenFields.removeAll(overridenFields);
					throw new MappingConfigurationException(
							currentMethodReference + " conflicts with " + conflictingDeclaration + " while embedding a " + Reflections.toString(inset.getEmbeddedClass())
									+ ", column names should be overriden : "
									+ Iterables.minus(expectedOverridenFields, overridenFields).stream().map(Reflections::toString).collect(Collectors.joining(", ")));
				}}
			}
		}
		
		/**
		 * Expected to give the {@link Column} for the given {@link Field} and {@link Inset} in the target {@link Table} (represented by its column
		 * registry {@code tableColumnsPerName}).
		 * 
		 * @param field the proerty to be mapped
		 * @param tableColumnsPerName persistence table's column registry
		 * @param configuration mapping configuration
		 * @return the {@link Column} of the target table that should be used to the field
		 */
		protected Column findColumn(Field field, Map<String, Column<Table, Object>> tableColumnsPerName, Inset<C, ?> configuration) {
			String fieldColumnName = preventNull(configuration.overridenColumnNames.get(field), field.getName());
			return tableColumnsPerName.get(fieldColumnName);
		}
	}
	
	private static class HierarchyIterator implements Iterator<Inset> {
		
		private Inset current;
		
		private HierarchyIterator(Inset starter) {
			this.current = starter;
		}
		
		@Override
		public boolean hasNext() {
			return current != null;
		}
		
		@Override
		public Inset next() {
			Inset result = this.current;
			current = current.getParent();
			return result;
		}
	}
	
}
