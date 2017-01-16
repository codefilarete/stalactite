package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.lang.Reflections;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.Accessors;
import org.gama.reflection.PropertyAccessor;
import org.gama.spy.MethodReferenceCapturer;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public class FluentMappingBuilder<T, I> implements IFluentMappingBuilder<T, I> {
	

	
	public static enum IdentifierPolicy {
		ALREADY_ASSIGNED,
		BEFORE_INSERT,
		AFTER_INSERT;
	}
	/**
	 * Will start a {@link FluentMappingBuilder} for a given class which will target a table that as the class name.
	 * @param persistedClass the class to be persisted by the {@link ClassMappingStrategy} that will be created by {@link #build(Dialect)}
	 * @param identifierClass the class of the identifier
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link FluentMappingBuilder}
	 */
	public static <T, I> FluentMappingBuilder<T, I> from(Class<T> persistedClass, Class<I> identifierClass) {
		return from(persistedClass, identifierClass, new Table(persistedClass.getSimpleName()));
	}
	
	/**
	 * Will start a {@link FluentMappingBuilder} for a given class and a given target table.
	 * @param persistedClass the class to be persisted by the {@link ClassMappingStrategy} that will be created by {@link #build(Dialect)}
	 * @param identifierClass the class of the identifier
	 * @param table the table which will store instances of the persistedClass
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link FluentMappingBuilder}
	 */
	public static <T, I> FluentMappingBuilder<T, I> from(Class<T> persistedClass, Class<I> identifierClass, Table table) {
		return new FluentMappingBuilder<>(persistedClass, table);
	}
	
	private final Class<T> persistedClass;
	
	private final Table table;
	
	private List<Linkage> mapping;
	
	private IdentifierInsertionManager<T, I> identifierInsertionManager;
	
	private PropertyAccessor<T, I> identifierAccessor;
	
	private final MethodReferenceCapturer<T> spy;
	
	private Cascade<T, ?, ?> cascade;
	
	public FluentMappingBuilder(Class<T> persistedClass, Table table) {
		this.persistedClass = persistedClass;
		this.table = table;
		this.mapping = new ArrayList<>();
		
		// Code enhancer for creation of a proxy that will support functions invocations
		this.spy = new MethodReferenceCapturer<>(persistedClass);
	}
	
	public Table getTable() {
		return table;
	}
	
	private Method captureLambdaMethod(Function<T, ?> function) {
		return this.spy.capture(function);
	}
	
	private <O> Method captureLambdaMethod(BiConsumer<T, O> function) {
		return this.spy.capture(function);
	}
	
	@Override
	public <O> IFluentMappingBuilderColumnOptions<T, I> add(BiConsumer<T, O> function) {
		Method method = captureLambdaMethod(function);
		Class<?> columnType = Reflections.propertyType(method);
		String columnName = Accessors.propertyName(method);
		return add(method, columnName, columnType);
	}
	
	@Override
	public IFluentMappingBuilderColumnOptions<T, I> add(Function<T, ?> function) {
		Method method = captureLambdaMethod(function);
		Class<?> columnType = Reflections.propertyType(method);
		String columnName = Accessors.propertyName(method);
		return add(method, columnName, columnType);
	}
	
	@Override
	public IFluentMappingBuilderColumnOptions<T, I> add(Function<T, ?> function, String columnName) {
		Method method = captureLambdaMethod(function);
		Class<?> columnType = Reflections.propertyType(method);
		return add(method, columnName, columnType);
	}
	
	private IFluentMappingBuilderColumnOptions<T, I> add(Method method, String columnName, Class<?> columnType) {
		Column newColumn = table.new Column(columnName, columnType);
		PropertyAccessor<T, I> propertyAccessor = PropertyAccessor.of(method);
		this.mapping.stream().map(Linkage::getFunction).filter(propertyAccessor::equals)
				.findAny()
				.ifPresent(f -> { throw new IllegalArgumentException("Mapping is already defined by a method " + f.getAccessor()); });
		this.mapping.stream().map(Linkage::getColumn).filter(newColumn::equals)
				.findAny()
				.ifPresent(f -> { throw new IllegalArgumentException("Mapping is already defined for " + columnName); });
		this.mapping.add(new Linkage(propertyAccessor, newColumn));
		Class<IFluentMappingBuilderColumnOptions<T, I>> returnType = (Class) IFluentMappingBuilderColumnOptions.class;
		return new Decorator<>(ColumnOptions.class).decorate(this, returnType, new ColumnOptions() {
			@Override
			public FluentMappingBuilder identifier(IdentifierPolicy identifierPolicy) {
				if (FluentMappingBuilder.this.identifierAccessor != null) {
					throw new IllegalArgumentException("Identifier is already defined by " + identifierAccessor.getAccessor());
				}
				switch (identifierPolicy) {
					case ALREADY_ASSIGNED:
						Class<I> e = Reflections.propertyType(method);
						FluentMappingBuilder.this.identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(e);
						newColumn.primaryKey();
						break;
					default:
						throw new NotYetSupportedOperationException();
				}
				FluentMappingBuilder.this.identifierAccessor = propertyAccessor;
				// we could return null because the decorator return embedder.this for us, but I find cleaner to do so (if we change our mind)
				return FluentMappingBuilder.this;
			}
		});
	}
	
	@Override
	public <O> IFluentMappingBuilder<T, I> cascade(Function<T, O> function, IFluentMappingBuilder<O, ?> targetFluentMappingBuilder) {
		cascade = new Cascade<>(function, (Class<O>) Reflections.propertyType(captureLambdaMethod(function)), targetFluentMappingBuilder);
		return this;
	}
	
	private Map<PropertyAccessor, Column> getMapping() {
		return mapping.stream().collect(HashMap::new, (hashMap, linkage) -> hashMap.put(linkage.getFunction(), linkage.getColumn()), (a, b) -> {});
	}
	
	@Override
	public ClassMappingStrategy<T, I> build(Dialect dialect) {
		// Assertion that binders are present: this will throw en exception if the binder is not found
		mapping.stream().map(Linkage::getColumn).forEach(c -> dialect.getColumnBinderRegistry().getBinder(c));
		return buildClassMappingStrategy();
	}
	
	@Override
	public Persister<T, I> build(PersistenceContext persistenceContext) {
		if (this.cascade != null) {
			add(cascade.targetProvider);
		}
		ClassMappingStrategy<T, I> mappingStrategy = build(persistenceContext.getDialect());
		Persister<T, I> persister = persistenceContext.add(mappingStrategy);
		if (this.cascade != null) {
			JoinedTablesPersister<T, I> joinedTablesPersister = new JoinedTablesPersister<>(persistenceContext, mappingStrategy);
			persister = joinedTablesPersister;
			
			ClassMappingStrategy targetMappingStrategy = this.cascade.targetFluentMappingBuilder.build(persistenceContext.getDialect());

			Map<PropertyAccessor, Column> propertyToColumn = mappingStrategy.getDefaultMappingStrategy().getPropertyToColumn();
			Method member = captureLambdaMethod(this.cascade.targetProvider);
			Class<?> targetPropertyType = Reflections.onJavaBeanPropertyWrapper(member, () -> member.getReturnType(), () -> member.getParameterTypes()[0], null);
			PropertyAccessor<Object, Object> propertyAccessor = PropertyAccessor.of(member);
			Persister<?, ?> persister1 = persistenceContext.getPersister(targetPropertyType);
			if (persister1 == null) {
				throw new IllegalArgumentException("Target of cascade is not registered for persistence : " + targetPropertyType.getName());
			}
			Column rightColumn = persister1.getTargetTable().getPrimaryKey();
			Column leftColumn = propertyToColumn.get(propertyAccessor);
			Function<Iterable<T>, Iterable> function = ts -> Iterables.stream(ts).map(t -> cascade.targetProvider.apply(t)).collect(Collectors.toList());
			joinedTablesPersister.addMappingStrategy(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, targetMappingStrategy, (Function) function, BeanRelationFixer.of((a, b) -> {
				propertyAccessor.getMutator().set(a, b);
			}), leftColumn, rightColumn);
		}
		return persister;
	}
	
	private ClassMappingStrategy<T, I> buildClassMappingStrategy() {
		Map<PropertyAccessor, Column> columnMapping = getMapping();
		List<Entry<PropertyAccessor, Column>> identifierProperties = columnMapping.entrySet().stream().filter(e -> e.getValue().isPrimaryKey()).collect
				(Collectors.toList());
		PropertyAccessor<T, I> identifierProperty;
		switch (identifierProperties.size()) {
			case 0:
				throw new IllegalArgumentException("Table without primary key is not supported");
			case 1:
				identifierProperty = (PropertyAccessor<T, I>) identifierProperties.get(0).getKey();
				break;
			default:
				throw new IllegalArgumentException("Multiple columned primary key is not supported");
		}
		
		return new ClassMappingStrategy<>(persistedClass, table, columnMapping, identifierProperty, this.identifierInsertionManager);
	}
	
	
	private class Linkage {
		
		private final PropertyAccessor<T, ?> function;
		private final Column column;
		
		private Linkage(PropertyAccessor<T, ?> function, Column column) {
			this.function = function;
			this.column = column;
		}
		
		public PropertyAccessor<T, ?> getFunction() {
			return function;
		}
		
		public Column getColumn() {
			return column;
		}
	}
	
	private static class Cascade<I, O, J> {
		
		private final Function<I, O> targetProvider;
		private final Class<O> cascadingTargetClass;
		private final IFluentMappingBuilder<O, J> targetFluentMappingBuilder;
		
		private Cascade(Function<I, O> targetProvider, Class<O> cascadingTargetClass, IFluentMappingBuilder<O, J> targetFluentMappingBuilder) {
			this.targetProvider = targetProvider;
			this.cascadingTargetClass = cascadingTargetClass;
			this.targetFluentMappingBuilder = targetFluentMappingBuilder;
		}
	}
}
