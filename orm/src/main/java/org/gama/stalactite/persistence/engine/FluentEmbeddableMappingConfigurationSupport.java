package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Reflections;
import org.gama.lang.bean.FieldIterator;
import org.gama.lang.collection.Arrays;
import org.gama.lang.exception.NotImplementedException;
import org.gama.lang.reflect.MethodDispatcher;
import org.gama.reflection.AccessorChain;
import org.gama.reflection.AccessorChainMutator;
import org.gama.reflection.Accessors;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public class FluentEmbeddableMappingConfigurationSupport<C> implements IFluentEmbeddableMappingBuilder<C> {
	
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
	
	private Method captureLambdaMethod(SerializableFunction getter) {
		return this.methodSpy.findMethod(getter);
	}
	
	private Method captureLambdaMethod(SerializableBiConsumer setter) {
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
	public IFluentEmbeddableMappingBuilder<C> add(SerializableFunction<C, ?> getter, String columnName) {
		Method method = captureLambdaMethod(getter);
		return add(method, columnName);
	}
	
	private IFluentEmbeddableMappingBuilder<C> add(Method method, @javax.annotation.Nullable String columnName) {
		PropertyAccessor<Object, Object> propertyAccessor = Accessors.of(method);
		assertMappingIsNotAlreadyDefined(columnName, propertyAccessor);
		String linkName = columnName;
		if (columnName == null) {
			linkName = Reflections.propertyName(method);
		}
		Linkage<C> linkage = new LinkageByColumnName<>(method, linkName);
		this.mapping.add(linkage);
		return this;
	}
	
	private void assertMappingIsNotAlreadyDefined(String columnName, PropertyAccessor propertyAccessor) {
		Predicate<Linkage> checker = ((Predicate<Linkage>) linkage -> {
			PropertyAccessor<C, ?> accessor = linkage.getAccessor();
			if (accessor.equals(propertyAccessor)) {
				throw new IllegalArgumentException("Mapping is already defined by method " + accessor.getAccessor());
			}
			return true;
		}).and(linkage -> {
			if (columnName != null && columnName.equals(linkage.getColumnName())) {
				throw new IllegalArgumentException("Mapping is already defined for column " + columnName);
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
	public <O> IFluentEmbeddableMappingBuilderEmbedOptions<C> embed(SerializableBiConsumer<C, O> setter) {
		Inset<C, O> inset = new Inset<>(setter);
		insets.add(inset);
		return embed(inset);
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilderEmbedOptions<C> embed(SerializableFunction<C, O> getter) {
		Inset<C, O> inset = new Inset<>(getter);
		insets.add(inset);
		return embed(inset);
	}
	
	private <O> IFluentEmbeddableMappingBuilderEmbedOptions<C> embed(Inset<C, O> inset) {
		return new MethodDispatcher()
				.redirect(EmbedOptions.class, new EmbedOptions() {
					@Override
					public IFluentEmbeddableMappingBuilderEmbedOptions overrideName(SerializableFunction getter, String columnName) {
						inset.overrideName(getter, columnName);
						// we can't return this nor FluentMappingBuilder.this because none of them implements IFluentMappingBuilderEmbedOptions
						// so we return anything (null) and ask for returning proxy
						return null;
					}
				}, true)
				.fallbackOn(this)
				.build((Class<IFluentEmbeddableMappingBuilderEmbedOptions<C>>) (Class) IFluentEmbeddableMappingBuilderEmbedOptions.class);
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
	
	private interface Linkage<T> {
		
		<I> PropertyAccessor<T, I> getAccessor();
		
		String getColumnName();
		
		Class<?> getColumnType();
		
		boolean isPrimaryKey();
	}
	
	private static class LinkageByColumnName<T> implements Linkage<T> {
		
		private final PropertyAccessor function;
		private final Class<?> columnType;
		/** Column name override if not default */
		private final String columnName;
		private boolean primaryKey;
		
		/**
		 * Constructor by {@link Method}. Only accessor by method is implemented (since input is from method reference).
		 * (Doing it for field accessor is simple work but not necessary)
		 *
		 * @param method a {@link PropertyAccessor}
		 * @param columnName an override of the default name that will be generated
		 */
		private LinkageByColumnName(Method method, String columnName) {
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
		
		public boolean isPrimaryKey() {
			return primaryKey;
		}
		
		public void primaryKey() {
			this.primaryKey = true;
		}
	}
	
	/**
	 * Represents a property that embeds a complex type
	 *
	 * @param <SRC> the owner type
	 * @param <TRGT> the target type
	 */
	private class Inset<SRC, TRGT> {
		private final Class<TRGT> embeddedClass;
		private final Method insetAccessor;
		private final Map<Field, String> overridenColumnNames = new HashMap<>();
		private final Map<Field, Column> overridenColumns = new HashMap<>();
		
		private Inset(SerializableBiConsumer<SRC, TRGT> targetProvider) {
			this(captureLambdaMethod(targetProvider));
		}
		
		private Inset(SerializableFunction<SRC, TRGT> targetProvider) {
			this(captureLambdaMethod(targetProvider));
		}
		
		private Inset(Method insetAccessor) {
			this.insetAccessor = insetAccessor;
			// looking for the target type because its necessary to find its persister (and other objects)
			this.embeddedClass = (Class<TRGT>) Reflections.javaBeanTargetType(this.insetAccessor);
		}
		
		public void overrideName(SerializableFunction methodRef, String columnName) {
			Method method = captureLambdaMethod(methodRef);
			this.overridenColumnNames.put(Reflections.wrappedField(method), columnName);
		}
		
		public void override(SerializableFunction methodRef, Column column) {
			Method method = captureLambdaMethod(methodRef);
			this.overridenColumns.put(Reflections.wrappedField(method), column);
		}
	}
	
	
	private class Builder {
		
		private final Dialect dialect;
		private final Table targetTable;
		
		public Builder(Dialect dialect, Table targetTable) {
			this.dialect = dialect;
			this.targetTable = targetTable;
		}
		
		private Map<IReversibleAccessor, Column> build() {
			Map<IReversibleAccessor, Column> result = new HashMap<>();
			// first we add mapping coming from inheritance, then it can be overwritten by class mapping 
			result.putAll(buildMappingFromInheritance());
			// converting mapping field to method result
			mapping.forEach(linkage -> {
				Column column;
				if (linkage instanceof LinkageByColumnName) {
					column = targetTable.addColumn(linkage.getColumnName(), linkage.getColumnType());
					// assert that column binder is registered : it will throw en exception if the binder is not found
					dialect.getColumnBinderRegistry().getBinder(column);
					// setting the primary key option as asked
					if (linkage.isPrimaryKey()) {
						column.primaryKey();
					}
				} else {
					throw new NotImplementedException(linkage.getClass());
				}
				result.put(linkage.getAccessor(), column);
			});
			// adding embeddable (no particular thinking about order compared to previous inherited & class mapping) 
			result.putAll(buildEmbeddedMapping());
			return result;
		}
		
		private Map<IReversibleAccessor, Column> buildMappingFromInheritance() {
			Map<IReversibleAccessor, Column> result = new HashMap<>();
			inheritanceMapping.forEach((superType, embeddableMappingStrategy) -> result.putAll(collectMapping(embeddableMappingStrategy)));
			return result;
		}
		
		private Map<IReversibleAccessor, Column> collectMapping(EmbeddedBeanMappingStrategy<? super C, ?> embeddableMappingStrategy) {
			Map<IReversibleAccessor, Column> result = new HashMap<>();
			Map<? extends IReversibleAccessor<? super C, Object>, ? extends Column<?, Object>> propertyToColumn =
					embeddableMappingStrategy.getPropertyToColumn();
			propertyToColumn.forEach((accessor, column) -> {
				Column projectedColumn = targetTable.addColumn(column.getName(), column.getJavaType());
				projectedColumn.setAutoGenerated(column.isAutoGenerated());
				projectedColumn.setNullable(column.isNullable());
				result.put(accessor, projectedColumn);
			});
			return result;
		}
		
		private Map<IReversibleAccessor, Column> buildEmbeddedMapping() {
			Map<String, Column<Table, Object>> columnsPerName = targetTable.mapColumnsOnName();
			Map<IReversibleAccessor, Column> result = new HashMap<>();
			for (Inset<?, ?> inset : insets) {
				// Building the mapping of the value-object's fields to the table
				FieldIterator fieldIterator = new FieldIterator(inset.embeddedClass);
				fieldIterator.forEachRemaining(field -> {
					// looking for the targeted column
					Column targetColumn;
					// overriden column is taken first
					Column overridenColumn = inset.overridenColumns.get(field);
					if (overridenColumn != null) {
						targetColumn = overridenColumn;
					} else {
						// then we try an overriden name 
						targetColumn = columnsPerName.get(field.getName());
						if (targetColumn == null) {
							// Column isn't declared in table => we create one from field informations
							String columnName = field.getName();
							String overridenName = inset.overridenColumnNames.get(field);
							if (overridenName != null) {
								columnName = overridenName;
							}
							targetColumn = targetTable.addColumn(columnName, field.getType());
							columnsPerName.put(columnName, targetColumn);
						}
					}
					
					// we create a chain that
					// - returns null when bean are not instanciated, so null will be inserted/updated in embedded columns
					// - initializes values when its mutator will be used, so bean will create its embedded properties on select
					// (voluntary dissimetric behavior)
					AccessorChain c = new AccessorChain(Arrays.asList(Accessors.of(inset.insetAccessor), Accessors.of(field))) {
						@Override
						public AccessorChainMutator toMutator() {
							AccessorChainMutator result = super.toMutator();
							result.setNullValueHandler(AccessorChain.INITIALIZE_VALUE);
							return result;
						}
					}.setNullValueHandler(AccessorChain.RETURN_NULL);
					result.put(c, targetColumn);
				});
			}
			return result;
		}
	}
}
