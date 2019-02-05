package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.bean.FieldIterator;
import org.gama.lang.collection.Iterables;
import org.gama.lang.exception.NotImplementedException;
import org.gama.lang.function.Serie;
import org.gama.lang.reflect.MethodDispatcher;
import org.gama.reflection.Accessors;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.AbstractVersioningStrategy.VersioningStrategySupport;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.builder.CascadeManyList;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy;
import org.gama.stalactite.persistence.mapping.IMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdAccessor;
import org.gama.stalactite.persistence.mapping.SinglePropertyIdAccessor;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.collection.Iterables.collect;

/**
 * @author Guillaume Mary
 */
public class FluentMappingBuilder<C extends Identified, I extends StatefullIdentifier> implements IFluentMappingBuilder<C, I> {
	
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
	
	private final Class<C> persistedClass;
	
	private final Table table;
	
	private final List<Linkage> mapping = new ArrayList<>();
	
	private IdentifierInsertionManager<C, I> identifierInsertionManager;
	
	private PropertyAccessor<C, I> identifierAccessor;
	
	private final MethodReferenceCapturer methodSpy;
	
	private final List<CascadeOne<C, ? extends Identified, ? extends StatefullIdentifier>> cascadeOnes = new ArrayList<>();
	
	private final List<CascadeMany<C, ? extends Identified, ? extends StatefullIdentifier, ? extends Collection>> cascadeManys = new ArrayList<>();
	
	private final Collection<Inset<C, ?>> insets = new ArrayList<>();
	
	private ForeignKeyNamingStrategy foreignKeyNamingStrategy = ForeignKeyNamingStrategy.DEFAULT;
	
	private JoinColumnNamingStrategy columnNamingStrategy = JoinColumnNamingStrategy.DEFAULT;
	
	private AssociationTableNamingStrategy associationTableNamingStrategy = AssociationTableNamingStrategy.DEFAULT;
	
	private OptimisticLockOption optimisticLockOption;
	
	private final Map<Class<? super C>, IMappingStrategy<? super C, ?>> inheritanceMapping = new HashMap<>();
	
	/**
	 * Creates a builder to map the given class for persistence
	 *
	 * @param persistedClass the class to create a mapping for
	 * @param table the target table of the persisted class
	 */
	public FluentMappingBuilder(Class<C> persistedClass, Table table) {
		this.persistedClass = persistedClass;
		this.table = table;
		
		// Helper to capture Method behind method reference
		this.methodSpy = new MethodReferenceCapturer();
	}
	
	/**
	 * Creates a builder to map the given class on a same name table
	 * 
	 * @param persistedClass the class to create a mapping for
	 */
	public FluentMappingBuilder(Class<C> persistedClass) {
		this(persistedClass, new Table(persistedClass.getSimpleName()));
	}
	
	public Table getTable() {
		return table;
	}
	
	private Method captureLambdaMethod(SerializableFunction getter) {
		return this.methodSpy.findMethod(getter);
	}
	
	private Method captureLambdaMethod(SerializableBiConsumer setter) {
		return this.methodSpy.findMethod(setter);
	}
	
	@Override
	public <O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableBiConsumer<C, O> setter) {
		Method method = captureLambdaMethod(setter);
		return add(method, (String) null);
	}
	
	@Override
	public <O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableFunction<C, O> getter) {
		Method method = captureLambdaMethod(getter);
		return add(method, (String) null);
	}
	
	@Override
	public <O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableBiConsumer<C, O> setter, String columnName) {
		Method method = captureLambdaMethod(setter);
		return add(method, columnName);
	}
	
	@Override
	public IFluentMappingBuilderColumnOptions<C, I> add(SerializableFunction<C, ?> getter, String columnName) {
		Method method = captureLambdaMethod(getter);
		return add(method, columnName);
	}
	
	@Override
	public <O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableFunction<C, O> getter, Column<Table, O> column) {
		Method method = captureLambdaMethod(getter);
		return add(method, column);
	}
	
	private <O> IFluentMappingBuilderColumnOptions<C, I> add(Method method, Column<Table, O> column) {
		Linkage<C> newMapping = addMapping(method, column);
		return applyAdditionalOptions(method, newMapping);
	}
	
	private IFluentMappingBuilderColumnOptions<C, I> add(Method method, @javax.annotation.Nullable String columnName) {
		Linkage<C> newMapping = addMapping(method, columnName);
		return applyAdditionalOptions(method, newMapping);
	}
	
	/**
	 * @return a new Column aded to the target table, throws an exception if already mapped
	 */
	private Linkage<C> addMapping(Method method, @javax.annotation.Nullable String columnName) {
		PropertyAccessor<Object, Object> propertyAccessor = Accessors.of(method);
		assertMappingIsNotAlreadyDefined(columnName, propertyAccessor);
		String linkName = columnName;
		if (columnName == null) { 
			if (Identified.class.isAssignableFrom(Reflections.javaBeanTargetType(method))) {
				linkName = columnNamingStrategy.giveName(propertyAccessor);
			} else {
				linkName = Reflections.propertyName(method);
			}
		}
		Linkage<C>linkage = new LinkageByColumnName<>(method, linkName);
		this.mapping.add(linkage);
		return linkage;
	}
	
	/**
	 * @return a new Column aded to the target table, throws an exception if already mapped
	 */
	private <O> Linkage<C> addMapping(Method method, Column<Table, O> column) {
		PropertyAccessor<Object, Object> propertyAccessor = Accessors.of(method);
		assertMappingIsNotAlreadyDefined(column.getName(), propertyAccessor);
		Linkage<C> linkage = new LinkageByColumn<>(method, column);
		this.mapping.add(linkage);
		return linkage;
	}
	
	private void assertMappingIsNotAlreadyDefined(@javax.annotation.Nullable String columnName, PropertyAccessor propertyAccessor) {
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
	
	private IFluentMappingBuilderColumnOptions<C, I> applyAdditionalOptions(Method method, Linkage newMapping) {
		return new MethodDispatcher()
				.redirect(ColumnOptions.class, identifierPolicy -> {
					if (FluentMappingBuilder.this.identifierAccessor != null) {
						throw new IllegalArgumentException("Identifier is already defined by " + identifierAccessor.getAccessor());
					}
					if (identifierPolicy == IdentifierPolicy.ALREADY_ASSIGNED) {
						Class<I> primaryKeyType = Reflections.propertyType(method);
						FluentMappingBuilder.this.identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(primaryKeyType);
						if (newMapping instanceof LinkageByColumnName) {
							// we force primary key so it's no necessary to set it by caller
							((LinkageByColumnName) newMapping).primaryKey();
						} else if (newMapping instanceof LinkageByColumn && !newMapping.isPrimaryKey()) {
							// safeguard about a missconfiguration, even if mapping would work it smells bad configuration
							throw new IllegalArgumentException("Identifier policy is assigned on a non primary key column");
						} else {
							// in case of evolution in the Linkage API
							throw new NotImplementedException(newMapping.getClass());
						}
					} else {
						throw new NotYetSupportedOperationException(identifierPolicy + " is not yet supported");
					}
					FluentMappingBuilder.this.identifierAccessor = (PropertyAccessor<C, I>) newMapping.getAccessor();
					// we return the fluent builder so user can chain with any other configuration
					return FluentMappingBuilder.this;
				})
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderColumnOptions<C, I>>) (Class) IFluentMappingBuilderColumnOptions.class);
	}
	
	@Override
	public IFluentMappingBuilder<C, I> mapSuperClass(Class<? super C> superType, ClassMappingStrategy<? super C, ?, ?> mappingStrategy) {
		inheritanceMapping.put(superType, mappingStrategy);
		return this;
	}
	
	@Override
	public IFluentMappingBuilder<C, I> mapSuperClass(Class<? super C> superType, EmbeddedBeanMappingStrategy<? super C, ?> mappingStrategy) {
		inheritanceMapping.put(superType, mappingStrategy);
		return this;
	}
	
	@Override
	public <O extends Identified, J extends StatefullIdentifier> IFluentMappingBuilderOneToOneOptions<C, I> addOneToOne(SerializableFunction<C, O> getter,
																														Persister<O, J, ? extends Table> persister) {
		// we declare the column on our side: we do it first because it checks some rules
		add(getter);
		// we keep it
		CascadeOne<C, O, J> cascadeOne = new CascadeOne<>(getter, persister, captureLambdaMethod(getter));
		this.cascadeOnes.add(cascadeOne);
		// then we return an object that allows fluent settings over our OneToOne cascade instance
		return new MethodDispatcher()
				.redirect(OneToOneOptions.class, new OneToOneOptions() {
					@Override
					public IFluentMappingBuilderOneToOneOptions cascading(RelationshipMode relationshipMode) {
						cascadeOne.setRelationshipMode(relationshipMode);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public IFluentMappingBuilderOneToOneOptions mandatory() {
						cascadeOne.setNullable(false);
						return null;	// we can return null because dispatcher will return proxy
					}
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderOneToOneOptions<C, I>>) (Class) IFluentMappingBuilderOneToOneOptions.class);
	}
	
	@Override
	public <O extends Identified, J extends StatefullIdentifier, S extends Collection<O>> IFluentMappingBuilderOneToManyOptions<C, I, O> addOneToManySet(
			SerializableFunction<C, S> getter, Persister<O, J, ? extends Table> persister) {
		CascadeMany<C, O, J, S> cascadeMany = new CascadeMany<>(getter, persister, captureLambdaMethod(getter));
		this.cascadeManys.add(cascadeMany);
		return new MethodDispatcher()
				.redirect(OneToManyOptions.class, new OneToManyOptionsSupport<>(cascadeMany), true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderOneToManyOptions<C, I, O>>) (Class) IFluentMappingBuilderOneToManyOptions.class);
	}
	
	@Override
	public <O extends Identified, J extends StatefullIdentifier, S extends List<O>> IFluentMappingBuilderOneToManyListOptions<C, I, O> addOneToManyList(
			SerializableFunction<C, S> getter, Persister<O, J, ? extends Table> persister) {
		CascadeManyList<C, O, J, ? extends List<O>> cascadeMany = new CascadeManyList<>(getter, persister, captureLambdaMethod(getter));
		this.cascadeManys.add(cascadeMany);
		return new MethodDispatcher()
				.redirect(OneToManyOptions.class, new OneToManyOptionsSupport<>(cascadeMany), true)	// true to allow "return null" in implemented methods
				.redirect(IndexableCollectionOptions.class, orderingColumn -> {
					cascadeMany.setIndexingColumn(orderingColumn);
					return null;	// we can return null because dispatcher will return proxy
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderOneToManyListOptions<C, I, O>>) (Class) IFluentMappingBuilderOneToManyListOptions.class);
	}
	
	@Override
	public <O> IFluentMappingBuilderEmbedOptions<C, I> embed(SerializableBiConsumer<C, O> setter) {
		Inset<C, O> inset = new Inset<>(setter);
		insets.add(inset);
		return embed(inset);
	}
	
	@Override
	public <O> IFluentMappingBuilderEmbedOptions<C, I> embed(SerializableFunction<C, O> getter) {
		Inset<C, O> inset = new Inset<>(getter);
		insets.add(inset);
		return embed(inset);
	}
	
	private <O> IFluentMappingBuilderEmbedOptions<C, I> embed(Inset<C, O> inset) {
		return new MethodDispatcher()
				.redirect(EmbedWithColumnOptions.class, new EmbedWithColumnOptions() {
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
				.build((Class<IFluentMappingBuilderEmbedOptions<C, I>>) (Class) IFluentMappingBuilderEmbedOptions.class);
	}
	
	@Override
	public IFluentMappingBuilder<C, I> foreignKeyNamingStrategy(ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		return this;
	}
	
	@Override
	public IFluentMappingBuilder<C, I> joinColumnNamingStrategy(JoinColumnNamingStrategy columnNamingStrategy) {
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
	 * @param <V> type of the versioning property, determines versioning policy
	 * @return this
	 * @see #versionedBy(SerializableFunction, Serie)
	 */
	@Override
	public <V> IFluentMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter) {
		Method method = captureLambdaMethod(getter);
		Serie<V> serie;
		if (Integer.class.isAssignableFrom(method.getReturnType())) {
			serie = (Serie<V>) Serie.INTEGER_SERIE;
		} else if (Long.class.isAssignableFrom(method.getReturnType())) {
			serie = (Serie<V>) Serie.LONG_SERIE;
		} else if (Date.class.isAssignableFrom(method.getReturnType())) {
			serie = (Serie<V>) Serie.NOW_SERIE;
		} else {
			throw new NotImplementedException("Type of versioned property is not implemented, please provide a "
					+ Serie.class.getSimpleName() + " for it : " + Reflections.toString(method.getReturnType()));
		}
		return versionedBy(getter, method, serie);
	}
	
	@Override
	public <V> IFluentMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter, Serie<V> serie) {
		return versionedBy(getter, captureLambdaMethod(getter), serie);
	}
	
	public <V> IFluentMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter, Method method, Serie<V> serie) {
		optimisticLockOption = new OptimisticLockOption<>(Accessors.of(method), serie);
		add(getter);
		return this;
	}
	
	/**
	 * Creates all necessary columns on the table
	 * @param dialect necessary for some checking
	 * @return the mapping between "property" to column
	 */
	private Map<IReversibleAccessor, Column> buildMapping(Dialect dialect) {
		Map<IReversibleAccessor, Column> result = new HashMap<>();
		// first we add mapping coming from inheritance, then it can be overwritten by class mapping 
		result.putAll(buildMappingFromInheritance());
		// converting mapping field to method result
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
			result.put(linkage.getAccessor(), column);
		});
		return result;
	}
	
	private Map<IReversibleAccessor, Column> buildMappingFromInheritance() {
		Map<IReversibleAccessor, Column> result = new HashMap<>();
		inheritanceMapping.forEach((superType, mappingStrategy) -> {
			// We transfer columns and mapping of the inherited source to the current mapping
			EmbeddedBeanMappingStrategy<? super C, ?> embeddableMappingStrategy;
			if (mappingStrategy instanceof EmbeddedBeanMappingStrategy) {
				embeddableMappingStrategy = (EmbeddedBeanMappingStrategy) mappingStrategy;
			} else if (mappingStrategy instanceof ClassMappingStrategy) {
				embeddableMappingStrategy = ((ClassMappingStrategy) mappingStrategy).getMainMappingStrategy();
			} else {
				// in case of evolution in the Linkage API
				throw new NotImplementedException(mappingStrategy.getClass());
			}
			result.putAll(collectMapping(embeddableMappingStrategy));
			// Dealing with identifier
			if (mappingStrategy instanceof ClassMappingStrategy) {
				ClassMappingStrategy entityMappingStrategy = (ClassMappingStrategy) mappingStrategy;
				result.putAll(collectIdMapping(entityMappingStrategy));
				// getting identifier insertion manager, may be overwritten by class mapping
				this.identifierInsertionManager = (IdentifierInsertionManager<C, I>) entityMappingStrategy.getIdMappingStrategy().getIdentifierInsertionManager();
			}
		});
		return result;
	}
	
	private Map<IReversibleAccessor, Column> collectMapping(EmbeddedBeanMappingStrategy<? super C, ?> embeddableMappingStrategy) {
		Map<IReversibleAccessor, Column> result = new HashMap<>();
		Map<? extends IReversibleAccessor<? super C, Object>, ? extends Column<?, Object>> propertyToColumn =
			embeddableMappingStrategy.getPropertyToColumn();
		propertyToColumn.forEach((accessor, column) -> {
			Column projectedColumn = table.addColumn(column.getName(), column.getJavaType());
			// Note that we don't transfert primaryKey because it is done some lines after
			projectedColumn.setAutoGenerated(column.isAutoGenerated());
			projectedColumn.setNullable(column.isNullable());
			result.put(accessor, projectedColumn);
		});
		return result;
	}
	
	private Map<IReversibleAccessor, Column> collectIdMapping(ClassMappingStrategy entityMappingStrategy) {
		Map<IReversibleAccessor, Column> result = new HashMap<>();
		IdAccessor<? super C, ?> idAccessor = entityMappingStrategy.getIdMappingStrategy().getIdAccessor();
		if (!(idAccessor instanceof SinglePropertyIdAccessor)) {
			throw new NotYetSupportedOperationException();
		}
		IReversibleAccessor<? super C, ?> identifierAccessor = ((SinglePropertyIdAccessor<? super C, ?>) idAccessor).getIdAccessor();
		Set<Column> columns = entityMappingStrategy.getTargetTable().getPrimaryKey().getColumns();
		// Because IdAccessor is a single column one (see assertion above) we can get the only column composing the primary key
		Column primaryKey = Iterables.first(columns);
		Column projectedPrimarkey = table.addColumn(primaryKey.getName(), primaryKey.getJavaType()).primaryKey();
		projectedPrimarkey.setAutoGenerated(primaryKey.isAutoGenerated());
		result.put(identifierAccessor, projectedPrimarkey);
		return result;
	}
	
	@Override
	public Persister<C, I, ?> build(PersistenceContext persistenceContext) {
		ClassMappingStrategy<C, I, Table> mainMappingStrategy = build(persistenceContext.getDialect());
		
		// by default, result is the simple persister of the main strategy
		Persister<C, I, ?> result = persistenceContext.add(mainMappingStrategy);
		
		if (!cascadeOnes.isEmpty() || !cascadeManys.isEmpty()) {
			JoinedTablesPersister<C, I, ?> joinedTablesPersister = new JoinedTablesPersister<>(persistenceContext, mainMappingStrategy);
			result = joinedTablesPersister;
			if (!cascadeOnes.isEmpty()) {
				// adding persistence flag setters on this side
				CascadeOneConfigurer cascadeOneConfigurer = new CascadeOneConfigurer();
				for (CascadeOne<C, ? extends Identified, ? extends StatefullIdentifier> cascadeOne : cascadeOnes) {
					cascadeOneConfigurer.appendCascade(cascadeOne, joinedTablesPersister, mainMappingStrategy, joinedTablesPersister,
							foreignKeyNamingStrategy);
				}
			}
			
			if (!cascadeManys.isEmpty()) {
				CascadeManyConfigurer cascadeManyConfigurer = new CascadeManyConfigurer();
				for (CascadeMany<C, ? extends Identified, ? extends StatefullIdentifier, ? extends Collection> cascadeMany : cascadeManys) {
					cascadeManyConfigurer.appendCascade(cascadeMany, joinedTablesPersister, foreignKeyNamingStrategy, associationTableNamingStrategy,
							persistenceContext.getDialect());
				}
			}
		}
		
		Table targetTable = result.getTargetTable();
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
			EmbeddedBeanMappingStrategy beanMappingStrategy = new EmbeddedBeanMappingStrategy(inset.embeddedClass, table, propertyMapping);
			mainMappingStrategy.put(Accessors.of(inset.insetAccessor), beanMappingStrategy);
		}
		
		Nullable<VersioningStrategy> versionigStrategy = Nullable.nullable(optimisticLockOption).apply(OptimisticLockOption::getVersioningStrategy);
		if (versionigStrategy.isPresent()) {
			// we have to declare it to the mapping strategy. To do that we must find the versionning column
			Column column = result.getMappingStrategy().getMainMappingStrategy().getPropertyToColumn().get(optimisticLockOption
					.propertyAccessor);
			result.getMappingStrategy().addVersionedColumn(optimisticLockOption.propertyAccessor, column);
			// and don't forget to give it to the workers !
			result.getUpdateExecutor().setVersioningStrategy(versionigStrategy.get());
			result.getInsertExecutor().setVersioningStrategy(versionigStrategy.get());
		}
		
		return result;
	}
	
	@Override
	public ClassMappingStrategy<C, I, Table> build(Dialect dialect) {
		if (this.identifierAccessor == null) {
			SerializableBiFunction<ColumnOptions, IdentifierPolicy, IFluentMappingBuilder> identifierMethodReference = ColumnOptions::identifier;
			Method identifierSetter = this.methodSpy.findMethod(identifierMethodReference);
			throw new UnsupportedOperationException("Identifier is not defined, please add one throught " + Reflections.toString(identifierSetter));
		}
		Map<IReversibleAccessor, Column> columnMapping = buildMapping(dialect);
		Column primaryKey = columnMapping.get(this.identifierAccessor);
		if (primaryKey == null) {
			throw new UnsupportedOperationException("Table without primary key is not supported");
		} else {
			List<IReversibleAccessor> identifierAccessors = collect(columnMapping.entrySet(), e -> e.getValue().isPrimaryKey(), Entry::getKey, ArrayList::new);
			if (identifierAccessors.size() > 1) {
				throw new NotYetSupportedOperationException("Composed primary key is not yet supported");
			}
		}
		
		return new ClassMappingStrategy<>(persistedClass, table, (Map) columnMapping, identifierAccessor, this.identifierInsertionManager);
	}
	
	
	@Override
	public <T extends Table> EmbeddedBeanMappingStrategy<C, T> buildEmbeddable(Dialect dialect) {
		Map<IReversibleAccessor, Column> columnMapping = buildMapping(dialect);
		return new EmbeddedBeanMappingStrategy<>(persistedClass, table, (Map) columnMapping);
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
	
	public static class SetPersistedFlagAfterInsertListener implements InsertListener<Identified> {
		
		public static final SetPersistedFlagAfterInsertListener INSTANCE = new SetPersistedFlagAfterInsertListener();
		
		@Override
		public void afterInsert(Iterable<? extends Identified> entities) {
			for (Identified t : entities) {
				if (t.getId() instanceof PersistableIdentifier) {
					((PersistableIdentifier) t.getId()).setPersisted(true);
				}
			}
		}
	}
	
	private static class OptimisticLockOption<C> {
		
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
		public IFluentMappingBuilderOneToManyOptions<T, I, O> cascading(RelationshipMode relationshipMode) {
			cascadeMany.setRelationshipMode(relationshipMode);
			return null;	// we can return null because dispatcher will return proxy
		}
		
	}
}
