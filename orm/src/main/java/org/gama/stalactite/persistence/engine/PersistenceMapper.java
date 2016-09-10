package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.lang.Reflections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.PairIterator;
import org.gama.reflection.PropertyAccessor;
import org.gama.spy.ByteBuddySpy;
import org.gama.spy.MethodInvocationHistory;
import org.gama.spy.MethodInvocationHistory.MethodInvocation;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public class PersistenceMapper<T> {
	
	private Map<Function<T, ?>, Column> mapping;
	
	public static <I> PersistenceMapper<I> with(Class<I> persistedClass, Table table) {
		return new PersistenceMapper<>(persistedClass, table);
	}
	
	private final Class<T> persistedClass;
	private final Table table;
	
	public PersistenceMapper(Class<T> persistedClass, Table table) {
		this.persistedClass = persistedClass;
		this.table = table;
		this.mapping = new HashMap<>();
	}
	
	public PersistenceMapper<T> map(Function<T, ?> function, Column column) {
		this.mapping.put(function, column);
		return this;
	}
	
	private Map<PropertyAccessor, Column> getMapping() {
		Map<PropertyAccessor, Column> result = new HashMap<>();
		T targetInstance = Reflections.newInstance(persistedClass);
		Set<Function<T, ?>> functions = mapping.keySet();
		List<Method> methods = captureLambdaMethods(targetInstance, functions);
		Iterable<Entry<Method, Function<T, ?>>> methodsAndFunctions = Iterables.asIterable(new PairIterator<>(methods, functions));
		for (Entry<Method, Function<T, ?>> methodsAndFunction : methodsAndFunctions) {
			result.put(PropertyAccessor.of(methodsAndFunction.getKey()), mapping.get(methodsAndFunction.getValue()));
		}
		return result;
	}
	
	private <I> ClassMappingStrategy<T, I> buildClassMappingStrategy(IdentifierInsertionManager<T> identifierInsertionManager) {
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
		
		return new ClassMappingStrategy<>(persistedClass, Iterables.firstValue(this.mapping).getTable(), columnMapping,
				identifierProperty, identifierInsertionManager);
	}
	
	public <I> ClassMappingStrategy<T, I> forDialect(Dialect dialect) {
		for (Entry<Function<T, ?>, Column> functionColumnEntry : mapping.entrySet()) {
			dialect.getColumnBinderRegistry().getBinder(functionColumnEntry.getValue());
		}
		return buildClassMappingStrategy(null);
	}
	
	private static <I> List<Method> captureLambdaMethods(I targetInstance, Iterable<Function<I, ?>> functions) {
		// Code enhancer for creation of a proxy that will support functions invokations
		ByteBuddySpy<I> testInstance = new ByteBuddySpy<>();
		// Capturer of method calls
		MethodInvocationHistory<I> invocationHistory = new MethodInvocationHistory<>();
		targetInstance = testInstance.spy(targetInstance, invocationHistory);
		// calling functions for method harvesting
		for (Function<I, ?> function : functions) {
			function.apply(targetInstance);
		}
		return invocationHistory.getHistory().stream().map(MethodInvocation::getMethod).collect(Collectors.toList());
	}
}
