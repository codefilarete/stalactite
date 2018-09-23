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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.bean.FieldIterator;
import org.gama.lang.exception.NotImplementedException;
import org.gama.lang.function.Serie;
import org.gama.lang.reflect.MethodDispatcher;
import org.gama.reflection.Accessors;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.AbstractVersioningStrategy.VersioningStrategySupport;
import org.gama.stalactite.persistence.engine.CascadeOption.CascadeType;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.builder.CascadeManyList;
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
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public class FluentMappingBuilder<T extends Identified, I extends StatefullIdentifier> implements IFluentMappingBuilder<T, I> {
	
	/**
	 * Available identifier policies for entities.
	 * Only {@link #ALREADY_ASSIGNED} is supported for now
	 * @see IdentifierInsertionManager
	 */
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
	
	private final MethodReferenceCapturer spy;
	
	private List<CascadeOne<T, ? extends Identified, ? extends StatefullIdentifier>> cascadeOnes = new ArrayList<>();
	
	private List<CascadeMany<T, ? extends Identified, ? extends StatefullIdentifier, ? extends Collection>> cascadeManys = new ArrayList<>();
	
	private Collection<Inset<T, ?>> insets = new ArrayList<>();
	
	private ForeignKeyNamingStrategy foreignKeyNamingStrategy = ForeignKeyNamingStrategy.DEFAULT;
	
	private JoinColumnNamingStrategy columnNamingStrategy = JoinColumnNamingStrategy.DEFAULT;
	
	private OptimisticLockOption optimisticLockOption;
	
	/**
	 * Creates a builder to map the given class for persistence
	 *
	 * @param persistedClass the class to create a mapping for
	 * @param table the target table of the persisted class
	 */
	public FluentMappingBuilder(Class<T> persistedClass, Table table) {
		this.persistedClass = persistedClass;
		this.table = table;
		this.mapping = new ArrayList<>();
		
		// Helper to capture Method behind method reference
		this.spy = new MethodReferenceCapturer();
	}
	
	/**
	 * Creates a builder to map the given class on a same name table
	 * 
	 * @param persistedClass the class to create a mapping for
	 */
	public FluentMappingBuilder(Class<T> persistedClass) {
		this(persistedClass, new Table(persistedClass.getSimpleName()));
	}
	
	public Table getTable() {
		return table;
	}
	
	private Method captureLambdaMethod(SerializableFunction getter) {
		return this.spy.findMethod(getter);
	}
	
	private Method captureLambdaMethod(SerializableBiConsumer setter) {
		return this.spy.findMethod(setter);
	}
	
	@Override
	public <O> IFluentMappingBuilderColumnOptions<T, I> add(SerializableBiConsumer<T, O> setter) {
		Method method = captureLambdaMethod(setter);
		return add(method, (String) null);
	}
	
	@Override
	public <O> IFluentMappingBuilderColumnOptions<T, I> add(SerializableFunction<T, O> getter) {
		Method method = captureLambdaMethod(getter);
		return add(method, (String) null);
	}
	
	@Override
	public <O> IFluentMappingBuilderColumnOptions<T, I> add(SerializableBiConsumer<T, O> setter, String columnName) {
		Method method = captureLambdaMethod(setter);
		return add(method, columnName);
	}
	
	@Override
	public IFluentMappingBuilderColumnOptions<T, I> add(SerializableFunction<T, ?> getter, String columnName) {
		Method method = captureLambdaMethod(getter);
		return add(method, columnName);
	}
	
	@Override
	public <O> IFluentMappingBuilderColumnOptions<T, I> add(SerializableFunction<T, O> getter, Column<Table, O> column) {
		Method method = captureLambdaMethod(getter);
		return add(method, column);
	}
	
	private <O> IFluentMappingBuilderColumnOptions<T, I> add(Method method, Column<Table, O> column) {
		Linkage<T> newMapping = addMapping(method, column);
		return addMapping(method, newMapping);
	}
	
	private IFluentMappingBuilderColumnOptions<T, I> add(Method method, @javax.annotation.Nullable String columnName) {
		Linkage<T> newMapping = addMapping(method, columnName);
		return addMapping(method, newMapping);
	}
	
	/**
	 * @return a new Column aded to the target table, throws an exception if already mapped
	 */
	private Linkage<T> addMapping(Method method, @javax.annotation.Nullable String columnName) {
		PropertyAccessor<Object, Object> propertyAccessor = Accessors.of(method);
		assertMappingIsNotAlreadyDefined(columnName, propertyAccessor);
		String linkName = columnName;
		if (columnName == null && Identified.class.isAssignableFrom(Reflections.javaBeanTargetType(method))) {
			linkName = columnNamingStrategy.giveName(propertyAccessor);
		}
		Linkage<T>linkage = new LinkageByColumnName<>(method, linkName);
		this.mapping.add(linkage);
		return linkage;
	}
	
	/**
	 * @return a new Column aded to the target table, throws an exception if already mapped
	 */
	private <O> Linkage<T> addMapping(Method method, Column<Table, O> column) {
		PropertyAccessor<Object, Object> propertyAccessor = Accessors.of(method);
		assertMappingIsNotAlreadyDefined(column.getName(), propertyAccessor);
		Linkage<T> linkage = new LinkageByColumn<>(method, column);
		this.mapping.add(linkage);
		return linkage;
	}
	
	private void assertMappingIsNotAlreadyDefined(@javax.annotation.Nullable String columnName, PropertyAccessor propertyAccessor) {
		Predicate<Linkage> checker = ((Predicate<Linkage>) linkage -> {
			PropertyAccessor<T, ?> accessor = linkage.getAccessor();
			if (accessor.equals(propertyAccessor)) {
				throw new IllegalArgumentException("Mapping is already defined by the method " + accessor.getAccessor());
			}
			return true;
		}).and(linkage -> {
			if (columnName != null && columnName.equals(linkage.getColumnName())) {
				throw new IllegalArgumentException("Mapping is already defined for " + columnName);
			}
			return true;
		});
		mapping.forEach(checker::test);
	}
	
	private IFluentMappingBuilderColumnOptions<T, I> addMapping(Method method, Linkage newMapping) {
		return new MethodDispatcher()
				.redirect(ColumnOptions.class, identifierPolicy -> {
					if (FluentMappingBuilder.this.identifierAccessor != null) {
						throw new IllegalArgumentException("Identifier is already defined by " + identifierAccessor.getAccessor());
					}
					if (identifierPolicy == IdentifierPolicy.ALREADY_ASSIGNED) {
						Class<I> primaryKeyType = Reflections.propertyType(method);
						FluentMappingBuilder.this.identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(primaryKeyType);
						if (newMapping instanceof FluentMappingBuilder.LinkageByColumnName) {
							// we force primary key so it's no necessary to set it by caller
							((LinkageByColumnName) newMapping).primaryKey();
						} else if (newMapping instanceof FluentMappingBuilder.LinkageByColumn && !newMapping.isPrimaryKey()) {
							// safeguard about a missconfiguration, even if mapping would work it smells bad configuration
							throw new IllegalArgumentException("Identifier policy is assigned on a non primary key column");
						} else {
							// in case of evolution in the Linkage API
							throw new NotImplementedException(newMapping.getClass());
						}
					} else {
						throw new NotYetSupportedOperationException(identifierPolicy + " is not yet supported");
					}
					FluentMappingBuilder.this.identifierAccessor = (PropertyAccessor<T, I>) newMapping.getAccessor();
					// we return the fluent builder so user can chain with any other configuration
					return FluentMappingBuilder.this;
				})
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderColumnOptions<T, I>>) (Class) IFluentMappingBuilderColumnOptions.class);
	}
	
	@Override
	public <O extends Identified, J extends StatefullIdentifier> IFluentMappingBuilderOneToOneOptions<T, I> addOneToOne(SerializableFunction<T, O> getter,
																														Persister<O, J, ? extends Table> persister) {
		// we declare the column on our side: we do it first because it checks some rules
		add(getter);
		// we keep it
		CascadeOne<T, O, J> cascadeOne = new CascadeOne<>(getter, persister, captureLambdaMethod(getter));
		this.cascadeOnes.add(cascadeOne);
		// then we return an object that allows fluent settings over our OneToOne cascade instance
		return new MethodDispatcher()
				.redirect(OneToOneOptions.class, new OneToOneOptions() {
					@Override
					public IFluentMappingBuilderOneToOneOptions cascade(CascadeType cascadeType, CascadeType... cascadeTypes) {
						cascadeOne.addCascadeType(cascadeType);
						for (CascadeType type : cascadeTypes) {
							cascadeOne.addCascadeType(type);
						}
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public IFluentMappingBuilderOneToOneOptions mandatory() {
						cascadeOne.nullable = false;
						return null;	// we can return null because dispatcher will return proxy
					}
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderOneToOneOptions<T, I>>) (Class) IFluentMappingBuilderOneToOneOptions.class);
	}
	
	@Override
	public <O extends Identified, J extends StatefullIdentifier, C extends Set<O>> IFluentMappingBuilderOneToManyOptions<T, I, O> addOneToManySet(
			SerializableFunction<T, C> getter, Persister<O, J, ? extends Table> persister) {
		CascadeMany<T, O, J, C> cascadeMany = new CascadeMany<>(getter, persister, captureLambdaMethod(getter));
		this.cascadeManys.add(cascadeMany);
		return new MethodDispatcher()
				.redirect(OneToManyOptions.class, new OneToManyOptionsSupport<>(cascadeMany), true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderOneToManyOptions<T, I, O>>) (Class) IFluentMappingBuilderOneToManyOptions.class);
	}
	
	@Override
	public <O extends Identified, J extends StatefullIdentifier, C extends List<O>> IFluentMappingBuilderOneToManyListOptions<T, I, O> addOneToManyList(
			SerializableFunction<T, C> getter, Persister<O, J, ? extends Table> persister) {
		CascadeManyList<T, O, J> cascadeMany = new CascadeManyList<>(getter, persister, captureLambdaMethod(getter));
		this.cascadeManys.add(cascadeMany);
		return new MethodDispatcher()
				.redirect(OneToManyOptions.class, new OneToManyOptionsSupport<>(cascadeMany), true)	// true to allow "return null" in implemented methods
				.redirect(IndexableCollectionOptions.class, orderingColumn -> {
					cascadeMany.setIndexingColumn(orderingColumn);
					return null;	// we can return null because dispatcher will return proxy
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderOneToManyListOptions<T, I, O>>) (Class) IFluentMappingBuilderOneToManyListOptions.class);
	}
	
	@Override
	public <O> IFluentMappingBuilderEmbedOptions<T, I> embed(SerializableBiConsumer<T, O> setter) {
		Inset<T, O> inset = new Inset<>(setter);
		insets.add(inset);
		return embed(inset);
	}
	
	@Override
	public <O> IFluentMappingBuilderEmbedOptions<T, I> embed(SerializableFunction<T, O> getter) {
		Inset<T, O> inset = new Inset<>(getter);
		insets.add(inset);
		return embed(inset);
	}
	
	private <O> IFluentMappingBuilderEmbedOptions<T, I> embed(Inset<T, O> inset) {
		return new MethodDispatcher()
				.redirect(EmbedOptions.class, new EmbedOptions() {
					@Override
					public IFluentMappingBuilderEmbedOptions overrideName(SerializableFunction getter, String columnName) {
						inset.overrideName(getter, columnName);
						// we can't return this nor FluentMappingBuilder.this because none of them implements IFluentMappingBuilderEmbedOptions
						// so we return anything (null) and ask for returning proxy
						return null;
					}
					
					@Override
					public IFluentMappingBuilderEmbedOptions override(SerializableFunction getter, Column targetColumn) {
						inset.override(getter, targetColumn);
						// we can't return this nor FluentMappingBuilder.this because none of them implements IFluentMappingBuilderEmbedOptions
						// so we return anything (null) and ask for returning proxy
						return null;
					}
				}, true)
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderEmbedOptions<T, I>>) (Class) IFluentMappingBuilderEmbedOptions.class);
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
	
	/**
	 * Defines the versioning property of beans. This implies that Optmistic Locking will be applied on those beans.
	 * Versioning policy is supported for following types:
	 * <ul>
	 * <li>{@link Integer} : a "+1" policy will be applied, see {@link Serie#INTEGER_SERIE}</li>
	 * <li>{@link Long} : a "+1" policy will be applied, see {@link Serie#LONG_SERIE}</li>
	 * <li>{@link Date} : a "now" policy will be applied, see {@link Serie#NOW_SERIE}</li>
	 * </ul>
	 * 
	 * @param getter the funciton that gives access to the versioning property
	 * @param <C> type of the versioning property, determines versioning policy
	 * @return this
	 * @see #versionedBy(SerializableFunction, Serie)
	 */
	@Override
	public <C> IFluentMappingBuilder<T, I> versionedBy(SerializableFunction<T, C> getter) {
		Method method = captureLambdaMethod(getter);
		Serie<C> serie;
		if (Integer.class.isAssignableFrom(method.getReturnType())) {
			serie = (Serie<C>) Serie.INTEGER_SERIE;
		} else if (Long.class.isAssignableFrom(method.getReturnType())) {
			serie = (Serie<C>) Serie.LONG_SERIE;
		} else if (Date.class.isAssignableFrom(method.getReturnType())) {
			serie = (Serie<C>) Serie.NOW_SERIE;
		} else {
			throw new NotImplementedException("Type of versioned property is not implemented, please provide a "
					+ Serie.class.getSimpleName() + " for it : " + method.getReturnType());
		}
		return versionedBy(getter, method, serie);
	}
	
	@Override
	public <C> IFluentMappingBuilder<T, I> versionedBy(SerializableFunction<T, C> getter, Serie<C> serie) {
		return versionedBy(getter, captureLambdaMethod(getter), serie);
	}
	
	public <C> IFluentMappingBuilder<T, I> versionedBy(SerializableFunction<T, C> getter, Method method, Serie<C> serie) {
		optimisticLockOption = new OptimisticLockOption<>(Accessors.of(method), serie);
		add(getter);
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
					Column column;
					if (linkage instanceof LinkageByColumnName) {
						column = getTable().addColumn(linkage.getColumnName(), linkage.getColumnType());
						// assert that column binder is registered : it will throw en exception if the binder is not found
						dialect.getColumnBinderRegistry().getBinder(column);
						// setting the primary key option as asked
						if (linkage.isPrimaryKey()) {
							column.primaryKey();
						}
					} else if (linkage instanceof LinkageByColumn) {
						column = ((LinkageByColumn) linkage).getColumn();
					} else {
						throw new NotImplementedException(linkage.getClass());
					}
					columnMapping.put(linkage.getAccessor(), column);
				}
		);
		return columnMapping;
	}
	
	@Override
	public ClassMappingStrategy<T, I, Table> build(Dialect dialect) {
		return buildClassMappingStrategy(dialect);
	}
	
	@Override
	public Persister<T, I, ?> build(PersistenceContext persistenceContext) {
		ClassMappingStrategy<T, I, Table> mappingStrategy = build(persistenceContext.getDialect());
		Persister<T, I, ?> localPersister = persistenceContext.add(mappingStrategy);
		if (!cascadeOnes.isEmpty()) {
			JoinedTablesPersister<T, I, ?> joinedTablesPersister = new JoinedTablesPersister<>(persistenceContext, mappingStrategy);
			localPersister = joinedTablesPersister;
			// adding persistence flag setters on this side
			joinedTablesPersister.getPersisterListener().addInsertListener((IInsertListener<T>) SetPersistedFlagAfterInsertListener.INSTANCE);
			CascadeOneConfigurer cascadeOneConfigurer = new CascadeOneConfigurer();
			for (CascadeOne<T, ? extends Identified, ? extends StatefullIdentifier> cascadeOne : cascadeOnes) {
				cascadeOneConfigurer.appendCascade(cascadeOne, joinedTablesPersister, mappingStrategy, joinedTablesPersister,
						foreignKeyNamingStrategy);
			}
		}
		
		if (!cascadeManys.isEmpty()) {
			JoinedTablesPersister<T, I, ?> joinedTablesPersister = new JoinedTablesPersister<>(persistenceContext, mappingStrategy);
			localPersister = joinedTablesPersister;
			
			CascadeManyConfigurer cascadeManyConfigurer = new CascadeManyConfigurer();
			for(CascadeMany<T, ? extends Identified, ? extends StatefullIdentifier, ? extends Collection> cascadeMany : cascadeManys) {
				cascadeManyConfigurer.appendCascade(cascadeMany, joinedTablesPersister, foreignKeyNamingStrategy);
			}
		}
		
		Table targetTable = localPersister.getTargetTable();
		Map<String, Column<Table, Object>> columnsPerName = targetTable.mapColumnsOnName();
		Map<PropertyAccessor, Column> propertyMapping = new HashMap<>();
		for (Inset<?, ?> inset : this.insets) {
			// Building the mapping of the value-object's fields to the table
			FieldIterator fieldIterator = new FieldIterator(inset.embeddedClass);
			propertyMapping.clear();
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
				propertyMapping.put(Accessors.of(field), targetColumn);
			});
			// We simply register a specialized mapping strategy for the field into the main strategy
			EmbeddedBeanMappingStrategy beanMappingStrategy = new EmbeddedBeanMappingStrategy(inset.embeddedClass, propertyMapping);
			mappingStrategy.put(Accessors.of(inset.insetAccessor), beanMappingStrategy);
		}
		
		Nullable<VersioningStrategy> versionigStrategy = Nullable.nullable(optimisticLockOption).apply(OptimisticLockOption::getVersioningStrategy);
		if (versionigStrategy.isPresent()) {
			// we have to declare it to the mapping strategy. To do that we must find the versionning column
			Column column = localPersister.getMappingStrategy().getMainMappingStrategy().getPropertyToColumn().get(optimisticLockOption
					.propertyAccessor);
			localPersister.getMappingStrategy().addVersionedColumn(optimisticLockOption.propertyAccessor, column);
			// and don't forget to give it to the workers !
			localPersister.getUpdateExecutor().setVersioningStrategy(versionigStrategy.get());
			localPersister.getInsertExecutor().setVersioningStrategy(versionigStrategy.get());
		}
		
		return localPersister;
	}

	private ClassMappingStrategy<T, I, Table> buildClassMappingStrategy(Dialect dialect) {
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
		
		return new ClassMappingStrategy<>(persistedClass, table, (Map) columnMapping, identifierProperty, this.identifierInsertionManager);
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
		 * @param method a {@link PropertyAccessor}
		 * @param columnName an override of the default name that will be generated
		 */
		private LinkageByColumnName(Method method, String columnName) {
			this.function = Accessors.of(method);
			this.columnType = Reflections.propertyType(method);
			this.columnName = columnName == null ? Reflections.propertyName(method) : columnName;
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
	
	private static class LinkageByColumn<T> implements Linkage<T> {
		
		private final PropertyAccessor function;
		private final Column column;
		
		/**
		 * Constructor by {@link Method}. Only accessor by method is implemented (since input is from method reference).
		 * (Doing it for field accessor is simple work but not necessary)
		 * @param method a {@link PropertyAccessor}
		 * @param column an override of the default column that would have been generated
		 */
		private LinkageByColumn(Method method, Column column) {
			this(Accessors.of(method), column);
		}
		
		private LinkageByColumn(PropertyAccessor function, Column column) {
			this.function = function;
			this.column = column;
		}
		
		public <I> PropertyAccessor<T, I> getAccessor() {
			return function;
		}
		
		public String getColumnName() {
			return column.getName();
		}
		
		public Class<?> getColumnType() {
			return column.getJavaType();
		}
		
		public boolean isPrimaryKey() {
			return column.isPrimaryKey();
		}
		
		public Column getColumn() {
			return column;
		}
	}
	
	public static class CascadeOne<SRC extends Identified, O extends Identified, J extends StatefullIdentifier> {
		
		private final Function<SRC, O> targetProvider;
		private final Method member;
		private final Persister<O, J, ? extends Table> persister;
		private final Set<CascadeType> cascadeTypes = new HashSet<>();
		private Column reverseSide;
		private boolean nullable = true;
		
		private CascadeOne(Function<SRC, O> targetProvider, Persister<O, J, ? extends Table> persister, Method method) {
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
		public Persister<O, J, ?> getPersister() {
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
		
		public Column getReverseSide() {
			return reverseSide;
		}
		
		public void setReverseSide(Column reverseSide) {
			this.reverseSide = reverseSide;
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
	
	public static class SetPersistedFlagAfterInsertListener extends NoopInsertListener<Identified> {
		
		public static final SetPersistedFlagAfterInsertListener INSTANCE = new SetPersistedFlagAfterInsertListener();
		
		@Override
		public void afterInsert(Iterable<Identified> entities) {
			for (Identified t : entities) {
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
		public void beforeInsert(Iterable<T> entities) {
			for (T pawn : entities) {
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
		public void beforeUpdate(Iterable<UpdatePayload<T, ?>> payloads, boolean allColumnsStatement) {
			for (UpdatePayload<T, ?> payload : payloads) {
				T t = payload.getEntities().getLeft();
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
	
	private class OptimisticLockOption<C> {
		
		private final VersioningStrategy<Object, C> versioningStrategy;
		private final PropertyAccessor<Object, C> propertyAccessor;
		
		public OptimisticLockOption(PropertyAccessor<Object, C> propertyAccessor, Serie<C> serie) {
			this.propertyAccessor = propertyAccessor;
			this.versioningStrategy = new VersioningStrategySupport<>(propertyAccessor, serie);
		}
		
		public VersioningStrategy getVersioningStrategy() {
			return versioningStrategy;
		}
	}
	
	
	/**
	 * A small class for one-to-many options storage into a {@link CascadeMany}. Acts as a wrapper over it.
	 */
	private static class OneToManyOptionsSupport<T extends Identified, I extends StatefullIdentifier, O extends Identified>
			implements OneToManyOptions<T, I, O> {
		
		private final CascadeMany<T, O, I, ? extends Collection> cascadeMany;
		
		public OneToManyOptionsSupport(CascadeMany<T, O, I, ? extends Collection> cascadeMany) {
			this.cascadeMany = cascadeMany;
		}
		
		@Override
		public IFluentMappingBuilderOneToManyOptions<T, I, O> mappedBy(SerializableBiConsumer<O, T> reverseLink) {
			cascadeMany.setReverseSetter(reverseLink);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public IFluentMappingBuilderOneToManyOptions<T, I, O> mappedBy(SerializableFunction<O, T> reverseLink) {
			cascadeMany.setReverseGetter(reverseLink);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public IFluentMappingBuilderOneToManyOptions<T, I, O> mappedBy(Column<Table, T> reverseLink) {
			cascadeMany.setReverseColumn(reverseLink);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public IFluentMappingBuilderOneToManyOptions<T, I, O> cascade(CascadeType cascadeType, CascadeType... cascadeTypes) {
			cascadeMany.addCascadeType(cascadeType);
			for (CascadeType type : cascadeTypes) {
				cascadeMany.addCascadeType(type);
			}
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public IFluentMappingBuilderOneToManyOptions<T, I, O> deleteRemoved() {
			cascadeMany.setDeleteRemoved(true);
			return null;	// we can return null because dispatcher will return proxy
		}
	}
}
