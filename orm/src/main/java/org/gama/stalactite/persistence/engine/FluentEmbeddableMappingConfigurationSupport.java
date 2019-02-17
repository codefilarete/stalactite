package org.gama.stalactite.persistence.engine;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Reflections;
import org.gama.lang.bean.FieldIterator;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.exception.NotImplementedException;
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
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.bean.Objects.preventNull;
import static org.gama.reflection.MethodReferences.toMethodReferenceString;

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
	public static <T extends Identified> FluentEmbeddableMappingConfigurationSupport<T> from(Class<T> persistedClass) {
		return new FluentEmbeddableMappingConfigurationSupport<>(persistedClass);
	}
	
	private final Class<C> persistedClass;
	
	private final MethodReferenceCapturer methodSpy;
	
	private final List<Linkage> mapping = new ArrayList<>();
	
	private final Collection<Inset<C, ?>> insets = new ArrayList<>();
	
	private final Map<Class<? super C>, EmbeddedBeanMappingStrategy<? super C, ?>> inheritanceMapping = new HashMap<>();
	
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
		Inset<C, O> inset = newInset(setter);
		insets.add(inset);
		return embed(inset);
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilderEmbedOptions<C, O> embed(SerializableFunction<C, O> getter) {
		Inset<C, O> inset = newInset(getter);
		insets.add(inset);
		return embed(inset);
	}
	
	private <O> IFluentEmbeddableMappingBuilderEmbedOptions<C, O> embed(Inset<C, O> inset) {
		IFluentEmbeddableMappingBuilderEmbedOptions<C, O>[] build = new IFluentEmbeddableMappingBuilderEmbedOptions[1];
		build[0] = new MethodDispatcher()
				.redirect(EmbedOptions.class, new EmbedOptions() {
					@Override
					public EmbedOptions overrideName(SerializableFunction getter, String columnName) {
						inset.overrideName(getter, columnName);
						// we can't return this nor FluentMappingBuilder.this because none of them implements IFluentMappingBuilderEmbedOptions
						// so we return anything (null) and ask for returning proxy
						return build[0];
					}
					
					@Override
					public EmbedOptions innerEmbed(SerializableFunction getter) {
						Inset<C, O> inset = newInset(getter);
						insets.add(inset);
						// we return this local object so one can use overrideName(..) on it
						return embed(inset);
					}
					
					@Override
					public EmbedOptions innerEmbed(SerializableBiConsumer setter) {
						Inset<C, O> inset = newInset(setter);
						insets.add(inset);
						// we return this local object so one can use overrideName(..) on it
						return embed(inset);
					}
				}) // NB: we don't return proxy because implementation returns its own object, in order to "inner" insets
				.fallbackOn(this)
				.build((Class<IFluentEmbeddableMappingBuilderEmbedOptions<C, O>>) (Class) IFluentEmbeddableMappingBuilderEmbedOptions.class);
		return build[0];
	}
	
	protected <O> Inset<C, O> newInset(SerializableFunction<C, O> getter) {
		return new Inset<>(getter, this);
	}
	
	protected <O> Inset<C, O> newInset(SerializableBiConsumer<C, O> setter) {
		return new Inset<>(setter, this);
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
	
	/**
	 * Represents a property that embeds a complex type
	 *
	 * @param <SRC> the owner type
	 * @param <TRGT> the target type
	 */
	static class Inset<SRC, TRGT> implements LambdaMethodUnsheller {
		private final PropertyAccessor<SRC, TRGT> accessor;
		private final Class<TRGT> embeddedClass;
		private final Method insetAccessor;
		private final Map<Field, String> overridenColumnNames = new HashMap<>();
		private final LambdaMethodUnsheller lambdaMethodUnsheller;
		
		Inset(SerializableBiConsumer<SRC, TRGT> targetSetter, LambdaMethodUnsheller lambdaMethodUnsheller) {
			this.lambdaMethodUnsheller = lambdaMethodUnsheller;
			this.insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetSetter);
			this.accessor = new PropertyAccessor<>(
					(AccessorByMethod<SRC, TRGT>) new MutatorByMethod<>(insetAccessor).toAccessor(),
					new MutatorByMethodReference<>(targetSetter));
			// looking for the target type because its necessary to find its persister (and other objects)
			this.embeddedClass = (Class<TRGT>) Reflections.javaBeanTargetType(insetAccessor);
		}
		
		Inset(SerializableFunction<SRC, TRGT> targetGetter, LambdaMethodUnsheller lambdaMethodUnsheller) {
			this.lambdaMethodUnsheller = lambdaMethodUnsheller;
			this.insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetGetter);
			this.accessor = new PropertyAccessor<>(
					new AccessorByMethodReference<>(targetGetter),
					((AccessorByMethod<SRC, TRGT>) new AccessorByMethod<>(insetAccessor)).toMutator());
			// looking for the target type because its necessary to find its persister (and other objects)
			this.embeddedClass = (Class<TRGT>) Reflections.javaBeanTargetType(insetAccessor);
		}
		
		public void overrideName(SerializableFunction methodRef, String columnName) {
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
				Column column;
				if (linkage instanceof LinkageByColumnName) {
					column = addLinkage(linkage);
				} else {
					throw new NotImplementedException(linkage.getClass());
				}
				result.put(linkage.getAccessor(), column);
			});
			// adding embeddable (no particular thinking about order compared to previous inherited & class mapping) 
			result.putAll(buildEmbeddedMapping());
			return result;
		}
		
		protected Column addLinkage(Linkage linkage) {
			Column column = targetTable.addColumn(linkage.getColumnName(), linkage.getColumnType());
			// assert that column binder is registered : it will throw en exception if the binder is not found
			dialect.getColumnBinderRegistry().getBinder(column);
			return column;
		}
		
		protected Map<IReversibleAccessor, Column> buildMappingFromInheritance() {
			Map<IReversibleAccessor, Column> inheritanceResult = new HashMap<>();
			inheritanceMapping.forEach((superType, embeddableMappingStrategy) -> inheritanceResult.putAll(collectMapping(embeddableMappingStrategy)));
			return inheritanceResult;
		}
		
		protected Map<IReversibleAccessor, Column> collectMapping(EmbeddedBeanMappingStrategy<? super C, ?> embeddableMappingStrategy) {
			Map<IReversibleAccessor, Column> embeddedBeanResult = new HashMap<>();
			Map<? extends IReversibleAccessor<? super C, Object>, ? extends Column<?, Object>> propertyToColumn =
					embeddableMappingStrategy.getPropertyToColumn();
			propertyToColumn.forEach((accessor, column) -> {
				Column projectedColumn = targetTable.addColumn(column.getName(), column.getJavaType());
				projectedColumn.setAutoGenerated(column.isAutoGenerated());
				projectedColumn.setNullable(column.isNullable());
				embeddedBeanResult.put(accessor, projectedColumn);
			});
			return embeddedBeanResult;
		}
		
		private Map<IReversibleAccessor, Column> buildEmbeddedMapping() {
			Map<String, Column<Table, Object>> columnsPerName = targetTable.mapColumnsOnName();
			Map<IReversibleAccessor, Column> toReturn = new HashMap<>();
			Set<Inset> treatedInsets = new HashSet<>();
			// Registry of mapped "properties" to skip inner embeds. Should be done this way because insets are stored as a flat list
			// This could be avoided if insets were stored as a tree
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
			insets.forEach(i -> insetsPropertyRegistry.add(i.insetAccessor));
			
			insets.forEach(inset -> {
				Optional<Inset> alreadyMappedType = treatedInsets.stream().filter(i -> i.embeddedClass == inset.embeddedClass).findFirst();
				if (alreadyMappedType.isPresent()) {
					Set<Field> expectedOverridenFields = new HashSet<>(Iterables.copy(new FieldIterator(inset.embeddedClass)));
					Set<Field> overridenFields = inset.overridenColumnNames.keySet();
					boolean allFieldsAreOverriden = overridenFields.equals(expectedOverridenFields);
					if (!allFieldsAreOverriden) {
						Method currentMethod = inset.insetAccessor;
						String currentMethodReference = toMethodReferenceString(currentMethod);
						Method conflictingMethod = alreadyMappedType.get().insetAccessor;
						String conflictingDeclaration = toMethodReferenceString(conflictingMethod);
						expectedOverridenFields.removeAll(overridenFields);
						throw new MappingConfigurationException(
								currentMethodReference + " conflicts with " + conflictingDeclaration + " for embedding a " + Reflections.toString(inset.embeddedClass)
										+ ", field names should be overriden : "
										+ Iterables.minus(expectedOverridenFields, overridenFields).stream().map(Reflections::toString).collect(Collectors.joining(", ")));
					}
				}
				// Building the mapping of the value-object's fields to the table
				FieldIterator fieldIterator = new FieldIterator(inset.embeddedClass);
				// NB: we skip fields that are registered as inset (inner embeddable) because they'll be treated by their own, skipping them avoids conflicts
				Iterables.consumeAll(fieldIterator, Objects.not(insetsPropertyRegistry::contains), (field, i) -> {
					// looking for the targeted column
					Column targetColumn = findColumn(field, columnsPerName, inset);
					if (targetColumn == null) {
						// Column isn't declared in table => we create one from field informations
						String columnName = field.getName();
						String overridenName = inset.overridenColumnNames.get(field);
						if (overridenName != null) {
							columnName = overridenName;
						}
						targetColumn = targetTable.addColumn(columnName, field.getType());
						// adding the newly created column to our index for next iterations because it may conflicts with mapping of other iteration
						columnsPerName.put(columnName, targetColumn);
					} else {
						// checking that column is not already mapped by a previous definition
						Column finalTargetColumn = targetColumn;
						Optional<Entry<IReversibleAccessor, Column>> existingMapping =
								Builder.this.result.entrySet().stream().filter(entry -> entry.getValue().equals(finalTargetColumn)).findFirst();
						if (existingMapping.isPresent()) {
							Method currentMethod = inset.insetAccessor;
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
					// - returns null when bean are not instanciated, so null will be inserted/updated in embedded columns
					// - initializes values when its mutator will be used, so bean will create its embedded properties on select
					// (voluntary dissimetric behavior)
					AccessorChain c = new AccessorChain(Arrays.asList(inset.accessor, Accessors.of(field))) {
						@Override
						public AccessorChainMutator toMutator() {
							AccessorChainMutator toReturn = super.toMutator();
							toReturn.setNullValueHandler(AccessorChain.INITIALIZE_VALUE);
							return toReturn;
						}
					}.setNullValueHandler(AccessorChain.RETURN_NULL);
					toReturn.put(c, targetColumn);
				});
				treatedInsets.add(inset);
			});
			return toReturn;
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
}
