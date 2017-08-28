package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.bean.FieldIterator;
import org.gama.lang.function.Serie;
import org.gama.reflection.Accessors;
import org.gama.reflection.PropertyAccessor;
import org.gama.spy.MethodReferenceCapturer;
import org.gama.stalactite.persistence.engine.AbstractVersioningStrategy.VersioningStrategySupport;
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
	
	private ForeignKeyNamingStrategy foreignKeyNamingStrategy = ForeignKeyNamingStrategy.DEFAULT;
	
	private JoinColumnNamingStrategy columnNamingStrategy = JoinColumnNamingStrategy.DEFAULT;
	
	private OptimiticLockOption optimiticLockOption;
	
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
		return add(method, null);
	}
	
	@Override
	public IFluentMappingBuilderColumnOptions<T, I> add(Function<T, ?> function) {
		Method method = captureLambdaMethod(function);
		return add(method, null);
	}
	
	@Override
	public <O> IFluentMappingBuilderColumnOptions<T, I> add(BiConsumer<T, O> function, String columnName) {
		Method method = captureLambdaMethod(function);
		return add(method, columnName);
	}
	
	@Override
	public IFluentMappingBuilderColumnOptions<T, I> add(Function<T, ?> function, String columnName) {
		Method method = captureLambdaMethod(function);
		return add(method, columnName);
	}
	
	private IFluentMappingBuilderColumnOptions<T, I> add(Method method, String columnName) {
		Linkage newMapping = addMapping(method, columnName);
		return new Decorator<>(ColumnOptions.class).decorate(this, (Class<IFluentMappingBuilderColumnOptions<T, I>>) (Class)
				IFluentMappingBuilderColumnOptions.class, identifierPolicy -> {
			if (FluentMappingBuilder.this.identifierAccessor != null) {
				throw new IllegalArgumentException("Identifier is already defined by " + identifierAccessor.getAccessor());
			}
			switch (identifierPolicy) {
				case ALREADY_ASSIGNED:
					Class<I> e = Reflections.propertyType(method);
					FluentMappingBuilder.this.identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(e);
					newMapping.primaryKey();
					break;
				default:
					throw new NotYetSupportedOperationException(identifierPolicy + " is not yet supported");
			}
			FluentMappingBuilder.this.identifierAccessor = (PropertyAccessor<T, I>) newMapping.getFunction();
			// we could return null because the decorator return embedder.this for us, but I find cleaner to do so (if we change our mind)
			return FluentMappingBuilder.this;
		});
	}
	
	/**
	 * @return a new Column aded to the target table, throws an exception if already mapped
	 */
	private Linkage addMapping(Method method, String columnName) {
		PropertyAccessor<Object, Object> propertyAccessor = Accessors.of(method);
		Predicate<Linkage> checker = ((Predicate<Linkage>) (l -> {
			PropertyAccessor<T, ?> function = l.getFunction();
			if (function.equals(propertyAccessor)) {
				throw new IllegalArgumentException("Mapping is already defined by the method " + function.getAccessor());
			}
			return true;
		})).and(l -> {
			if (columnName != null && columnName.equals(l.getColumnName())) {
				throw new IllegalArgumentException("Mapping is already defined for " + columnName);
			}
			return true;
		});
		mapping.forEach(checker::test);
		String linkName = columnName;
		if (columnName == null && Identified.class.isAssignableFrom(Reflections.javaBeanTargetType(method))) {
			linkName = columnNamingStrategy.giveName(propertyAccessor);
		}
		Linkage linkage = new Linkage(method, linkName);
		this.mapping.add(linkage);
		return linkage;
	}
	
	@Override
	public <O extends Identified, J extends StatefullIdentifier> IFluentMappingBuilderOneToOneOptions<T, I> addOneToOne(Function<T, O> function,
																														Persister<O, J> persister) {
		// we declare the column on our side: we do it first because it checks some rules
		add(function);
		// we keep it
		CascadeOne<T, O, J> cascadeOne = new CascadeOne<>(function, persister, captureLambdaMethod(function));
		this.cascadeOnes.add(cascadeOne);
		// then we return an object that allows fluent settings over our OneToOne cascade instance
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
		CascadeMany<T, O, J, C> cascadeMany = new CascadeMany<>(function, persister, captureLambdaMethod(function));
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
	
	@Override
	public IFluentMappingBuilder<T, I> foreignKeyNamingStrategy(ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		return this;
	}
	
	@Override
	public IFluentMappingBuilder<T, I> joinColumnNamingStrategy(JoinColumnNamingStrategy columnNamingStrategy) {
		this.columnNamingStrategy = columnNamingStrategy;
		return this;
	}
	
	@Override
	public <C> IFluentMappingBuilder<T, I> versionedBy(Function<T, C> property) {
		Method method = captureLambdaMethod(property);
		Serie<C> serie;
		if (Integer.class.isAssignableFrom(method.getReturnType())) {
			serie = (Serie<C>) Serie.INTEGER_SERIE;
		} else if (Long.class.isAssignableFrom(method.getReturnType())) {
			serie = (Serie<C>) Serie.LONG_SERIE;
		} else if (Date.class.isAssignableFrom(method.getReturnType())) {
			serie = (Serie<C>) Serie.NOW_SERIE;
		} else {
			throw new UnsupportedOperationException("Type of versioned property is not implemented, please provide a "
					+ Serie.class.getSimpleName() + " for it : " + method.getReturnType());
		}
		return versionedBy(property, method, serie);
	}
	
	@Override
	public <C> IFluentMappingBuilder<T, I> versionedBy(Function<T, C> property, Serie<C> serie) {
		return versionedBy(property, captureLambdaMethod(property), serie);
	}
	
	public <C> IFluentMappingBuilder<T, I> versionedBy(Function<T, C> property, Method method, Serie<C> serie) {
		optimiticLockOption = new OptimiticLockOption<>(Accessors.of(method), serie);
		add(property);
		return this;
	}
	
	/**
	 * Create all necessary columns on the table
	 * @param dialect necessary for some checking
	 * @return the mapping between "property" to column
	 */
	private Map<PropertyAccessor, Column> buildMapping(Dialect dialect) {
		Map<PropertyAccessor, Column> columnMapping = new HashMap<>();
		mapping.forEach(linkage -> {
					Column newColumn = getTable().new Column(linkage.getColumnName(), linkage.getColumnType());
					// assert that column binder is registered : it will throw en exception if the binder is not found
					dialect.getColumnBinderRegistry().getBinder(newColumn);
					// setting the primary key option as asked
					if (linkage.isPrimaryKey()) {
						newColumn.primaryKey();
					}
					columnMapping.put(linkage.getFunction(), newColumn);
				}
		);
		return columnMapping;
	}
	
	@Override
	public ClassMappingStrategy<T, I> build(Dialect dialect) {
		return buildClassMappingStrategy(dialect);
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
				new CascadeOneConfigurer().appendCascade(cascadeOne, joinedTablesPersister, mappingStrategy, joinedTablesPersister,
						foreignKeyNamingStrategy);
			}
		}
		
		if (!cascadeManys.isEmpty()) {
			JoinedTablesPersister<T, I> joinedTablesPersister = new JoinedTablesPersister<>(persistenceContext, mappingStrategy);
			localPersister = joinedTablesPersister;
			
			for(CascadeMany<T, ? extends Identified, ? extends StatefullIdentifier, ? extends Collection> cascadeMany : cascadeManys) {
				new CascadeManyConfigurer().appendCascade(cascadeMany, joinedTablesPersister, joinedTablesPersister, foreignKeyNamingStrategy);
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
				mapping.put(Accessors.of(field), column);
			}
			// We simply register a specialized mapping strategy for the field into the main strategy
			EmbeddedBeanMappingStrategy beanMappingStrategy = new EmbeddedBeanMappingStrategy(embed.cascadingTargetClass, mapping);
			mappingStrategy.put(Accessors.of(embed.member), beanMappingStrategy);
		}
		
		Nullable<VersioningStrategy> versionigStrategy = Nullable.of(optimiticLockOption).orApply(OptimiticLockOption::getVersioningStrategy);
		if (versionigStrategy.isPresent()) {
			// we have to declare it to the lapping strategy. To do that we must find the versionning column
			Column column = localPersister.getMappingStrategy().getDefaultMappingStrategy().getPropertyToColumn().get(optimiticLockOption
					.propertyAccessor);
			localPersister.getMappingStrategy().addVersionedColumn(optimiticLockOption.propertyAccessor, column);
			// and don't forget to give it to the workers !
			localPersister.getUpdateExecutor().setVersioningStrategy(versionigStrategy.get());
			// TODO: take exception into account for deletion
//			localPersister.getDeleteExecutor().setVersioningStrategy(versionigStrategy.get());
			// TODO: ask initial value for insert
//			localPersister.getInsertExecutor().setVersioningStrategy(versionigStrategy.get());
		}
		
		return localPersister;
	}

	private ClassMappingStrategy<T, I> buildClassMappingStrategy(Dialect dialect) {
		Map<PropertyAccessor, Column> columnMapping = buildMapping(dialect);
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
		private final Class<?> columnType;
		/** Column name override if not default */
		private final String columnName;
		private Class<Collection> targetManyType;
		private boolean primaryKey;
		
		/**
		 * Constructor by {@link Method}. Only accessor by method is implemented (since input is from method reference).
		 * (Doing it for field accessor is simple work but not necessary)
		 * @param method a {@link PropertyAccessor}
		 * @param columnName an override of the default name that will be generated
		 */
		private Linkage(Method method, String columnName) {
			this.function = Accessors.of(method);
			this.columnType = Reflections.propertyType(method);
			this.columnName = columnName == null ? Accessors.propertyName(method) : columnName;
		}
		
		public PropertyAccessor<T, ?> getFunction() {
			return function;
		}
		
		public String getColumnName() {
			return columnName;
		}
		
		public Class<?> getColumnType() {
			return columnType;
		}
		
		public Class<Collection> getTargetManyType() {
			return targetManyType;
		}
		
		public void setTargetManyType(Class<Collection> targetManyType) {
			this.targetManyType = targetManyType;
		}
		
		public boolean isPrimaryKey() {
			return primaryKey;
		}
		
		public void primaryKey() {
			this.primaryKey = true;
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
			this.collectionTargetClass = (Class<C>) Reflections.javaBeanTargetType(member);
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
			this.cascadingTargetClass = (Class<TRGT>) Reflections.javaBeanTargetType(member);
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
	
	private class OptimiticLockOption<C> {
		
		private final VersioningStrategy<Object, C> versioningStrategy;
		private final PropertyAccessor<Object, C> propertyAccessor;
		
		public OptimiticLockOption(PropertyAccessor<Object, C> propertyAccessor, Serie<C> serie) {
			this.propertyAccessor = propertyAccessor;
			this.versioningStrategy = new VersioningStrategySupport<>(propertyAccessor, serie);
		}
		
		public VersioningStrategy getVersioningStrategy() {
			return versioningStrategy;
		}
	}
}
