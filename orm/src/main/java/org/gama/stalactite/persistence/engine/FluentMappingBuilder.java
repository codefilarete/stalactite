package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.lang.Reflections;
import org.gama.lang.bean.FieldIterator;
import org.gama.reflection.Accessors;
import org.gama.reflection.PropertyAccessor;
import org.gama.spy.MethodReferenceCapturer;
import org.gama.stalactite.persistence.engine.CascadeOption.CascadeType;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.IInsertListener;
import org.gama.stalactite.persistence.engine.listening.NoopInsertListener;
import org.gama.stalactite.persistence.engine.listening.NoopUpdateListener;
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
	
	public enum IdentifierPolicy {
		ALREADY_ASSIGNED,
		BEFORE_INSERT,
		AFTER_INSERT
	}
	
	/**
	 * Will start a {@link FluentMappingBuilder} for a given class which will target a table that as the class name.
	 *
	 * @param persistedClass the class to be persisted by the {@link ClassMappingStrategy} that will be created by {@link #build(Dialect)}
	 * @param identifierClass the class of the identifier
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link FluentMappingBuilder}
	 */
	public static <T extends Identified, I extends StatefullIdentifier> FluentMappingBuilder<T, I> from(Class<T> persistedClass,
																										Class<I> identifierClass) {
		return from(persistedClass, identifierClass, new Table(persistedClass.getSimpleName()));
	}
	
	/**
	 * Will start a {@link FluentMappingBuilder} for a given class and a given target table.
	 *
	 * @param persistedClass the class to be persisted by the {@link ClassMappingStrategy} that will be created by {@link #build(Dialect)}
	 * @param identifierClass the class of the identifier
	 * @param table the table which will store instances of the persistedClass
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link FluentMappingBuilder}
	 */
	public static <T extends Identified, I extends StatefullIdentifier> FluentMappingBuilder<T, I> from(Class<T> persistedClass,
																										Class<I> identifierClass, Table table) {
		return new FluentMappingBuilder<>(persistedClass, table);
	}
	
	private final Class<T> persistedClass;
	
	private final Table table;
	
	private List<Linkage> mapping;
	
	private IdentifierInsertionManager<T, I> identifierInsertionManager;
	
	private PropertyAccessor<T, I> identifierAccessor;
	
	private final MethodReferenceCapturer<T> spy;
	
	private List<CascadeOne<T, ? extends Identified, ? extends StatefullIdentifier>> cascadeOnes = new ArrayList<>();
	
	private List<CascadeMany<T, ? extends Identified, ? extends StatefullIdentifier, ? extends Collection>> cascadeManys = new ArrayList<>();
	
	private Collection<Inset<T, ?>> insets = new ArrayList<>();
	
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
		PropertyAccessor<T, I> propertyAccessor = PropertyAccessor.of(method);
		Column newColumn = addMapping(columnName, columnType, propertyAccessor);
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
	
	/**
	 * @return a new Column aded to the target table, throws an exception if already mapped
	 */
	private Column addMapping(String columnName, Class<?> columnType, PropertyAccessor<T, I> propertyAccessor) {
		Column newColumn = table.new Column(columnName, columnType);
		this.mapping.forEach(l -> {
			if (l.getFunction().equals(propertyAccessor)) {
				throw new IllegalArgumentException("Mapping is already defined by the method " + l.getFunction().getAccessor());
			}
			if (l.getColumn().equals(newColumn)) {
				throw new IllegalArgumentException("Mapping is already defined for " + columnName);
			}
		});
		this.mapping.add(new Linkage(propertyAccessor, newColumn));
		return newColumn;
	}
	
	@Override
	public <O extends Identified, J extends StatefullIdentifier> IFluentMappingBuilderOneToOneOptions<T, I> addOneToOne(Function<T, O> function,
																														Persister<O, J> persister) {
		CascadeOne<T, O, J> cascadeOne = new CascadeOne<>(function, persister, captureLambdaMethod((Function) function));
		this.cascadeOnes.add(cascadeOne);
		// we declare the column on our side
		add(cascadeOne.getTargetProvider());
		IFluentMappingBuilderOneToOneOptions[] finalHack = new IFluentMappingBuilderOneToOneOptions[1];
		IFluentMappingBuilderOneToOneOptions<T, I> proxy = new Decorator<>(OneToOneOptions.class).decorate(this,
				(Class<IFluentMappingBuilderOneToOneOptions<T, I>>) (Class) IFluentMappingBuilderOneToOneOptions.class, new OneToOneOptions() {
					
					@Override
					public IFluentMappingBuilderOneToOneOptions cascade(CascadeType cascadeType, CascadeType... cascadeTypes) {
						cascadeOne.addCascadeType(cascadeType);
						for (CascadeType type : cascadeTypes) {
							cascadeOne.addCascadeType(type);
						}
						return finalHack[0];
					}
					
					@Override
					public IFluentMappingBuilderOneToOneOptions mandatory() {
						cascadeOne.nullable = false;
						return finalHack[0];
					}
				});
		finalHack[0] = proxy;
		return proxy;
	}
	
	@Override
	public <O extends Identified, J extends StatefullIdentifier, C extends Collection<O>> IFluentMappingBuilderOneToManyOptions<T, I, O> addOneToMany(
			Function<T, C> function, Persister<O, J> persister) {
		CascadeMany<T, O, J, C> cascadeMany = new CascadeMany<>(function, persister, captureLambdaMethod((Function) function));
		this.cascadeManys.add(cascadeMany);
		IFluentMappingBuilderOneToManyOptions[] finalHack = new IFluentMappingBuilderOneToManyOptions[1];
		IFluentMappingBuilderOneToManyOptions<T, I, O> proxy = new Decorator<>(OneToManyOptions.class).decorate(
				this,
				(Class<IFluentMappingBuilderOneToManyOptions<T, I, O>>) (Class) IFluentMappingBuilderOneToManyOptions.class,
				new OneToManyOptions() {
					@Override
					public IFluentMappingBuilderOneToManyOptions mappedBy(BiConsumer reverseLink) {
						cascadeMany.reverseMember = reverseLink;
						return finalHack[0];
					}
					
					@Override
					public IFluentMappingBuilderOneToManyOptions cascade(CascadeType cascadeType, CascadeType... cascadeTypes) {
						cascadeMany.addCascadeType(cascadeType);
						for (CascadeType type : cascadeTypes) {
							cascadeMany.addCascadeType(type);
						}
						return finalHack[0];
					}
					
					@Override
					public IFluentMappingBuilderOneToManyOptions deleteRemoved() {
						cascadeMany.deleteRemoved = true;
						return finalHack[0];
					}
				});
		finalHack[0] = proxy;
		return proxy;
	}
	
	@Override
	public IFluentMappingBuilder<T, I> embed(Function<T, ?> function) {
		insets.add(new Inset<>(function));
		return this;
	}
	
	private Map<PropertyAccessor, Column> collectMapping() {
		return mapping.stream().collect(HashMap::new, (hashMap, linkage) -> hashMap.put(linkage.getFunction(), linkage.getColumn()), (a, b) -> { });
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
		if (!cascadeOnes.isEmpty()) {
			JoinedTablesPersister<T, I> joinedTablesPersister = new JoinedTablesPersister<>(persistenceContext, mappingStrategy);
			localPersister = joinedTablesPersister;
			// adding persistence flag setters on this side
			joinedTablesPersister.getPersisterListener().addInsertListener((IInsertListener<T>) SetPersistedFlagAfterInsertListener.INSTANCE);
			for (CascadeOne<T, ? extends Identified, ? extends StatefullIdentifier> cascadeOne : cascadeOnes) {
				new CascadeOneConfigurer().appendCascade(cascadeOne, joinedTablesPersister, mappingStrategy, joinedTablesPersister);
			}
		}
		
		if (!cascadeManys.isEmpty()) {
			JoinedTablesPersister<T, I> joinedTablesPersister = new JoinedTablesPersister<>(persistenceContext, mappingStrategy);
			localPersister = joinedTablesPersister;
			
			for(CascadeMany<T, ? extends Identified, ? extends StatefullIdentifier, ? extends Collection> cascadeMany : cascadeManys) {
				new CascadeManyConfigurer().appendCascade(cascadeMany, joinedTablesPersister, joinedTablesPersister);
			}
		}
		
		for (Inset embed : this.insets) {
			// Building the mapping of the value-object's fields to the table
			Map<String, Column> columnsPerName = localPersister.getTargetTable().mapColumnsOnName();
			Map<PropertyAccessor, Column> mapping = new HashMap<>();
			FieldIterator fieldIterator = new FieldIterator(embed.cascadingTargetClass);
			while (fieldIterator.hasNext()) {
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

	private ClassMappingStrategy<T, I> buildClassMappingStrategy() {
		Map<PropertyAccessor, Column> columnMapping = collectMapping();
		List<Entry<PropertyAccessor, Column>> identifierProperties = columnMapping.entrySet().stream().filter(e -> e.getValue().isPrimaryKey())
				.collect(Collectors.toList());
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
	
	public static class CascadeOne<SRC extends Identified, O extends Identified, J extends StatefullIdentifier> {
		
		private final Function<SRC, O> targetProvider;
		private final Method member;
		private final Persister<O, J> persister;
		private final Set<CascadeType> cascadeTypes = new HashSet<>();
		private boolean nullable = true;
		
		private CascadeOne(Function<SRC, O> targetProvider, Persister<O, J> persister, Method method) {
			this.targetProvider = targetProvider;
			this.persister = persister;
			// looking for the target type because its necessary to find its persister (and other objects). Done thru a method capturer (weird thing).
			this.member = method;
		}
		
		/** Original method reference given for mapping */
		public Function<SRC, O> getTargetProvider() {
			return targetProvider;
		}
		
		/** Equivalent of {@link #targetProvider} as a Reflection API element */
		public Method getMember() {
			return member;
		}
		
		/** The {@link Persister} that will be used to persist the target of the relation */
		public Persister<O, J> getPersister() {
			return persister;
		}
		
		/** Events of the cascade, default is none */
		public Set<CascadeType> getCascadeTypes() {
			return cascadeTypes;
		}
		
		public void addCascadeType(CascadeType cascadeType) {
			this.getCascadeTypes().add(cascadeType);
		}
		
		/** Nullable option, mainly for column join and DDL schema generation */
		public boolean isNullable() {
			return nullable;
		}
	}
	
	public static class CascadeMany<SRC extends Identified, O extends Identified, J extends StatefullIdentifier, C extends Collection<O>> {
		
		private final Function<SRC, C> targetProvider;
		private final Persister<O, J> persister;
		private final Method member;
		private final Class<C> collectionTargetClass;
		private BiConsumer<O, SRC> reverseMember;
		private final Set<CascadeType> cascadeTypes = new HashSet<>();
		/** Should we delete removed entities from the Collection (for UPDATE cascade) */
		public boolean deleteRemoved = false;
		
		private CascadeMany(Function<SRC, C> targetProvider, Persister<O, J> persister, Method method) {
			this.targetProvider = targetProvider;
			this.persister = persister;
			// looking for the target type because its necessary to find its persister (and other objects). Done thru a method capturer (weird thing).
			this.member = method;
			this.collectionTargetClass = (Class<C>) Reflections.onJavaBeanPropertyWrapper(member, member::getReturnType, () -> member
					.getParameterTypes()[0], null);
		}
		
		private CascadeMany(Function<SRC, C> targetProvider, Persister<O, J> persister, Class<C> collectionTargetClass, Method method) {
			this.targetProvider = targetProvider;
			this.persister = persister;
			// looking for the target type because its necesary to find its persister (and other objects). Done thru a method capturer (weird thing).
			this.member = method;
			this.collectionTargetClass = collectionTargetClass;
		}
		
		public Function<SRC, C> getTargetProvider() {
			return targetProvider;
		}
		
		public Persister<O, J> getPersister() {
			return persister;
		}
		
		public Method getMember() {
			return member;
		}
		
		public Class<C> getCollectionTargetClass() {
			return collectionTargetClass;
		}
		
		public BiConsumer<O, SRC> getReverseMember() {
			return reverseMember;
		}
		
		public Set<CascadeType> getCascadeTypes() {
			return cascadeTypes;
		}
		
		public void addCascadeType(CascadeType cascadeType) {
			this.cascadeTypes.add(cascadeType);
		}
		
		public boolean shouldDeleteRemoved() {
			return deleteRemoved;
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
			this.cascadingTargetClass = (Class<TRGT>) Reflections.onJavaBeanPropertyWrapper(member, member::getReturnType, () -> member
					.getParameterTypes()[0], null);
		}
	}
	
	public static class SetPersistedFlagAfterInsertListener extends NoopInsertListener<Identified> {
		
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
	
	public static class MandatoryRelationCheckingBeforeInsertListener<T extends Identified> extends NoopInsertListener<T> {
		
		private final Function<T, ? extends Identified> targetProvider;
		private final Method member;
		
		public MandatoryRelationCheckingBeforeInsertListener(Function<T, ? extends Identified> targetProvider, Method member) {
			this.targetProvider = targetProvider;
			this.member = member;
		}
		
		@Override
		public void beforeInsert(Iterable<T> iterable) {
			for (T pawn : iterable) {
				Identified modifiedTarget = targetProvider.apply(pawn);
				if (modifiedTarget == null) {
					throw newRuntimeMappingException(pawn, member);
				}
			}
		}
	}
	
	public static class MandatoryRelationCheckingBeforeUpdateListener<T extends Identified> extends NoopUpdateListener<T> {
		
		private final Method member;
		private final Function<T, ? extends Identified> targetProvider;
		
		public MandatoryRelationCheckingBeforeUpdateListener(Method member, Function<T, ? extends Identified> targetProvider) {
			this.member = member;
			this.targetProvider = targetProvider;
		}
		
		@Override
		public void beforeUpdate(Iterable<Entry<T, T>> iterable, boolean allColumnsStatement) {
			for (Entry<T, T> entry : iterable) {
				T t = entry.getKey();
				Identified modifiedTarget = targetProvider.apply(t);
				if (modifiedTarget == null) {
					throw newRuntimeMappingException(t, member);
				}
			}
		}
	}
	
	public static RuntimeMappingException newRuntimeMappingException(Object pawn, Method member) {
		return new RuntimeMappingException("Non null value expected for relation "
				+ Reflections.toString(member) + " on object " + pawn);
	}
	
}
