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
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Reflections;
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
	
	private Inset currentInset;
	
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
	protected Inset currentInset() {
		return currentInset;
	}
	
	protected <O> Inset<C, O> newInset(SerializableFunction<C, O> getter) {
		currentInset = new Inset<>(getter, this);
		return currentInset;
	}
	
	protected <O> Inset<C, O> newInset(SerializableBiConsumer<C, O> setter) {
		currentInset = new Inset<>(setter, this);
		return currentInset;
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
						Inset parent = currentInset();
						Inset<C, O> inset = newInset(getter);
						inset.parent = parent;
						embed(inset);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedOptions innerEmbed(SerializableBiConsumer setter) {
						Inset parent = currentInset();
						Inset<C, O> inset = newInset(setter);
						inset.parent = parent;
						embed(inset);
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
		private final Set<Field> excludedProperties = new HashSet<>();
		private final LambdaMethodUnsheller lambdaMethodUnsheller;
		public Inset parent;
		
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
			
			// starting mapping
			insets.forEach(inset -> {
				Optional<Inset> alreadyMappedType = treatedInsets.stream().filter(i -> i.embeddedClass == inset.embeddedClass).findFirst();
				if (alreadyMappedType.isPresent()) {
					Set<Field> expectedOverridenFields = new HashSet<>(Iterables.copy(new FieldIterator(inset.embeddedClass)));
					expectedOverridenFields.removeAll(inset.excludedProperties);
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
				Predicate<Field> innerEmbeddableExcluder = Objects.not(insetsPropertyRegistry::contains);
				// we also skip excluded fields by user
				Predicate<Field> excludedFieldsExcluder = Objects.not(inset.excludedProperties::contains);
				Iterables.consume(fieldIterator, innerEmbeddableExcluder.and(excludedFieldsExcluder), (field, i) -> {
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
					List<PropertyAccessor> chain = new ArrayList<>();
					HierarchyIterator it = new HierarchyIterator(inset);
					it.forEachRemaining(e -> chain.add(e.accessor));
					
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
							return inset.accessor + " -> " + field.getDeclaringClass().getSimpleName() + "." + field.getName();
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
			current = current.parent;
			return result;
		}
	}
	
}
