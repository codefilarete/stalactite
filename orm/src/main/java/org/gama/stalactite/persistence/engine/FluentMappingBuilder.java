package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.lang.Reflections;
import org.gama.lang.bean.FieldIterator;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.Accessors;
import org.gama.reflection.PropertyAccessor;
import org.gama.spy.MethodReferenceCapturer;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.IInsertListener;
import org.gama.stalactite.persistence.engine.listening.NoopInsertListener;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public class FluentMappingBuilder<T extends Identified, I extends StatefullIdentifier> implements IFluentMappingBuilder<T, I> {
	

	
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
	public static <T extends Identified, I extends StatefullIdentifier> FluentMappingBuilder<T, I> from(Class<T> persistedClass, Class<I> identifierClass) {
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
	public static <T extends Identified, I extends StatefullIdentifier> FluentMappingBuilder<T, I> from(Class<T> persistedClass, Class<I> identifierClass, Table table) {
		return new FluentMappingBuilder<>(persistedClass, table);
	}
	
	private final Class<T> persistedClass;
	
	private final Table table;
	
	private List<Linkage> mapping;
	
	private IdentifierInsertionManager<T, I> identifierInsertionManager;
	
	private PropertyAccessor<T, I> identifierAccessor;
	
	private final MethodReferenceCapturer<T> spy;
	
	private Cascade<T, ? extends Identified, ? extends StatefullIdentifier> cascade;
	
	private CascadeMany<T, ? extends Identified, ? extends StatefullIdentifier, ? extends Collection> cascadeMany;
	
	private Inset<T, ?> embed;
	
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
		Linkage linkage = new Linkage(propertyAccessor, newColumn);
		this.mapping.add(linkage);
		return new Decorator<>(ColumnOptions.class).decorate(this, (Class<IFluentMappingBuilderColumnOptions<T, I>>) (Class) 
				IFluentMappingBuilderColumnOptions.class, identifierPolicy -> {
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
		});
	}
	
	@Override
	public <O extends Identified, J extends StatefullIdentifier> IFluentMappingBuilder<T, I> cascade(Function<T, O> function, Persister<O, J> persister) {
		cascade = new Cascade<>(function, persister);
		return this;
	}
	
	@Override
	public <O extends Identified, J extends StatefullIdentifier, C extends Collection<O>> IFluentMappingBuilderCascadeManyOptions<T, I, O> addOneToMany(Function<T, C> function, Persister<O, J> persister) {
		cascadeMany = new CascadeMany<>(function, persister);
		return new Decorator<>(CascadeManyOptions.class).decorate(this, (Class<IFluentMappingBuilderCascadeManyOptions<T, I, O>>) (Class)
				IFluentMappingBuilderCascadeManyOptions.class, reverseLink -> {
					cascadeMany.reverseMember = reverseLink;
					// we could return null because the decorator return embedder.this for us, but I find cleaner to do so (if we change our mind)
					return FluentMappingBuilder.this;
				});
	}
	
	@Override
	public IFluentMappingBuilder<T, I> embed(Function<T, ?> function) {
		this.embed = new Inset<>(function);
		return this;
	}
	
	private Map<PropertyAccessor, Column> collectMapping() {
		return mapping.stream().collect(HashMap::new, (hashMap, linkage) -> hashMap.put(linkage.getFunction(), linkage.getColumn()), (a, b) -> {});
	}
	
	@Override
	public ClassMappingStrategy<T, I> build(Dialect dialect) {
		assertColumnBindersRegistered(dialect);
		return buildClassMappingStrategy();
	}
	
	/**
	 * Asserts that binders of all mapped columns are present: it will throw en exception if the binder is not found
	 */
	private void assertColumnBindersRegistered(Dialect dialect) {
		mapping.stream().map(Linkage::getColumn).forEach(c -> dialect.getColumnBinderRegistry().getBinder(c));
	}
	
	@Override
	public Persister<T, I> build(PersistenceContext persistenceContext) {
		ClassMappingStrategy<T, I> mappingStrategy = build(persistenceContext.getDialect());
		Persister<T, I> localPersister = persistenceContext.add(mappingStrategy);
		if (this.cascade != null) {
			add(cascade.targetProvider);
			JoinedTablesPersister<T, I> joinedTablesPersister = new JoinedTablesPersister<>(persistenceContext, mappingStrategy);
			localPersister = joinedTablesPersister;
			
			Persister<Identified, StatefullIdentifier> targetPersister = (Persister<Identified, StatefullIdentifier>) this.cascade.persister;
			Method member = this.cascade.member;
			
//			Class<?> targetPropertyType = Reflections.onJavaBeanPropertyWrapper(member, member::getReturnType, () -> member.getParameterTypes()[0], null);
			PropertyAccessor<Identified, Identified> propertyAccessor = PropertyAccessor.of(member);
			Function<Iterable<T>, Iterable<Identified>> function = alreadyPersistedInstanceRemover(this.cascade.targetProvider::apply);
			joinedTablesPersister.getPersisterListener().addInsertListener((IInsertListener<T>) SetPersistedFlagAfterInsertListener.INSTANCE);
			targetPersister.getPersisterListener().addInsertListener(SetPersistedFlagAfterInsertListener.INSTANCE);
			
			// finding joined columns: left one is given by current mapping strategy throught the property accessor. Right one is target primary key
			// because we don't yet support "not owner of the property"
			Column leftColumn = mappingStrategy.getDefaultMappingStrategy().getPropertyToColumn().get(propertyAccessor);
			Column rightColumn = targetPersister.getTargetTable().getPrimaryKey();
			joinedTablesPersister.addPersister(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, targetPersister, function,
					BeanRelationFixer.of((a, b) -> propertyAccessor.getMutator().set((Identified) a, (Identified) b)),
					leftColumn, rightColumn, false);
		}
		
		if (this.cascadeMany != null) {
			PropertyAccessor<Identified, Collection<Identified>> propertyAccessor = PropertyAccessor.of(cascadeMany.member);
			Column newColumn = cascadeMany.persister.getTargetTable().new Column(cascadeMany.member.getName(), cascadeMany.persister.getMappingStrategy().getClassToPersist());
			Linkage linkage = new Linkage(propertyAccessor, newColumn);
			this.mapping.add(linkage);
//			add(cascadeMany.targetProvider);
			JoinedTablesPersister<T, I> joinedTablesPersister = new JoinedTablesPersister<>(persistenceContext, mappingStrategy);
			localPersister = joinedTablesPersister;
			
			Persister<Identified, StatefullIdentifier> targetPersister = (Persister<Identified, StatefullIdentifier>) this.cascadeMany.persister;
			
//			Class<?> targetPropertyType = Reflections.onJavaBeanPropertyWrapper(member, member::getReturnType, () -> member.getParameterTypes()[0], null);
//			PropertyAccessor<Identified, Collection<Identified>> propertyAccessor = PropertyAccessor.of(member);
			Function<T, Collection<Identified>> targetProvider = (Function<T, Collection<Identified>>) this.cascadeMany.targetProvider;
			Function<Iterable<T>, Iterable<Identified>> function = alreadyPersistedInstanceRemover2(targetProvider);
			joinedTablesPersister.getPersisterListener().addInsertListener((IInsertListener<T>) SetPersistedFlagAfterInsertListener.INSTANCE);
			targetPersister.getPersisterListener().addInsertListener(SetPersistedFlagAfterInsertListener.INSTANCE);
			
			// finding joined columns: left one is given by current mapping strategy throught the property accessor. Right one is target primary key
			// because we don't yet support "not owner of the property"
			Column leftColumn = localPersister.getTargetTable().getPrimaryKey();
			MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer(cascadeMany.persister.getMappingStrategy().getClassToPersist());
			Method capture = methodReferenceCapturer.capture(cascadeMany.reverseMember);
			Column rightColumn = cascadeMany.persister.getMappingStrategy().getDefaultMappingStrategy().getPropertyToColumn().get(PropertyAccessor.of(capture));
			BiConsumer<T, Collection<Identified>> scBiConsumer = (a, b) -> propertyAccessor.getMutator().set(a, b);
			joinedTablesPersister.addPersister(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, targetPersister, function,
					BeanRelationFixer.of((BiConsumer) scBiConsumer, (Function) this.cascadeMany.targetProvider, this.cascadeMany.collectionTargetClass,
							(BiConsumer) cascadeMany.reverseMember),
					leftColumn, rightColumn, false);
		}
		
		if (this.embed != null) {
			// Building the mapping of the value-object's fields to the table
			Map<String, Column> columnsPerName = localPersister.getTargetTable().mapColumnsOnName();
			Map<PropertyAccessor, Column> mapping = new HashMap<>();
			FieldIterator fieldIterator = new FieldIterator(embed.cascadingTargetClass);
			while(fieldIterator.hasNext()) {
				Field field = fieldIterator.next();
				Column column = columnsPerName.get(field.getName());
				if (column == null) {
					// Column isn't declared in table => we create one from field informations
					column = localPersister.getTargetTable().new Column(field.getName(), field.getType());
				}
				mapping.put(PropertyAccessor.of(field), column);
			}
			// We simply register a specialized mapping strategy for the field into the main strategy
			EmbeddedBeanMappingStrategy beanMappingStrategy = new EmbeddedBeanMappingStrategy(embed.cascadingTargetClass, mapping);
			mappingStrategy.put(PropertyAccessor.of(embed.member), beanMappingStrategy);
		}
		
		return localPersister;
	}
	
	/**
	 * Give a function that will remove already persisted instance from the result of the given function
	 * @param function a simple function
	 * @return a massive vesion of the given function, additionnaly will remove {@link StatefullIdentifier#isPersisted()} instances
	 */
	private static <I, O> Function<Iterable<I>, Iterable<O>> alreadyPersistedInstanceRemover(Function<I, O> function) {
		return ts -> Iterables.stream(ts)
				.map(function)
				.filter(o -> !(o instanceof Identified && ((Identified) o).getId().isPersisted()))
				.collect(Collectors.toList());
	}
	
	private static <I, O> Function<Iterable<I>, Iterable<O>> alreadyPersistedInstanceRemover2(Function<I, Collection<O>> function) {
		return ts -> Iterables.stream(ts)
				.flatMap(function.andThen(Collection::stream))
				.filter(o -> !(o instanceof Identified && ((Identified) o).getId().isPersisted()))
				.collect(Collectors.toList());
	}
	
	private ClassMappingStrategy<T, I> buildClassMappingStrategy() {
		Map<PropertyAccessor, Column> columnMapping = collectMapping();
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
		
		private final PropertyAccessor function;
		private final Column column;
		private Class<Collection> targetManyType;
		
		private Linkage(PropertyAccessor function, Column column) {
			this.function = function;
			this.column = column;
		}
		
		public PropertyAccessor<T, ?> getFunction() {
			return function;
		}
		
		public Column getColumn() {
			return column;
		}
		
		public Class<Collection> getTargetManyType() {
			return targetManyType;
		}
		
		public void setTargetManyType(Class<Collection> targetManyType) {
			this.targetManyType = targetManyType;
		}
	}
	
	private class Cascade<SRC extends Identified, O extends Identified, J extends StatefullIdentifier> {
		
		private final Function<SRC, O> targetProvider;
		private final Persister<O, J> persister;
		private final Method member;
		
		private Cascade(Function<SRC, O> targetProvider, Persister<O, J> persister) {
			this.targetProvider = targetProvider;
			this.persister = persister;
			// looking for the target type because its necessary to find its persister (and other objects). Done thru a method capturer (weird thing).
			this.member = captureLambdaMethod((Function) targetProvider);
		}
	}
	
	private class CascadeMany<SRC extends Identified, O extends Identified, J extends StatefullIdentifier, C extends Collection<O>> {
		
		private final Function<SRC, C> targetProvider;
		private final Persister<O, J> persister;
		private final Method member;
		private final Class<C> collectionTargetClass;
		private BiConsumer<O, SRC> reverseMember;
		
		private CascadeMany(Function<SRC, C> targetProvider, Persister<O, J> persister) {
			this.targetProvider = targetProvider;
			this.persister = persister;
			// looking for the target type because its necessary to find its persister (and other objects). Done thru a method capturer (weird thing).
			this.member = captureLambdaMethod((Function) targetProvider);
			this.collectionTargetClass = (Class<C>) Reflections.onJavaBeanPropertyWrapper(member, member::getReturnType, () -> member.getParameterTypes()[0], null);;
		}
		
		private CascadeMany(Function<SRC, C> targetProvider, Persister<O, J> persister, Class<C> collectionTargetClass) {
			this.targetProvider = targetProvider;
			this.persister = persister;
			// looking for the target type because its necesary to find its persister (and other objects). Done thru a method capturer (weird thing).
			this.member = captureLambdaMethod((Function) targetProvider);
			this.collectionTargetClass = collectionTargetClass;
		}
	}
	
	/**
	 * Represents a property that embeds a complex type
	 * 
	 * @param <SRC> the owner type
	 * @param <TRGT> the target type
	 */
	private class Inset<SRC, TRGT> {
		private final Class<TRGT> cascadingTargetClass;
		private final Method member;
		
		private Inset(Function<SRC, TRGT> targetProvider) {
			// looking for the target type because its necesary to find its persister (and other objects). Done thru a method capturer (weird thing).
			this.member = captureLambdaMethod((Function) targetProvider);
			this.cascadingTargetClass = (Class<TRGT>) Reflections.onJavaBeanPropertyWrapper(member, member::getReturnType, () -> member.getParameterTypes()[0], null);
		}
	} 
	
	private static class SetPersistedFlagAfterInsertListener extends NoopInsertListener<Identified> {
		
		public static final SetPersistedFlagAfterInsertListener INSTANCE = new SetPersistedFlagAfterInsertListener();
		
		@Override
		public void afterInsert(Iterable<Identified> iterables) {
			for (Identified t : iterables) {
				if (t.getId() instanceof PersistableIdentifier) {
					((PersistableIdentifier) t.getId()).setPersisted(true);
				}
			}
		}
	}
}
