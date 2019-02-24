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

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Duo;
import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.exception.NotImplementedException;
import org.gama.lang.function.Serie;
import org.gama.lang.reflect.MethodDispatcher;
import org.gama.reflection.Accessors;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.AbstractVersioningStrategy.VersioningStrategySupport;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.Builder;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.Inset;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.Linkage;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.LinkageByColumnName;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.IFluentEmbeddableMappingBuilder.IFluentEmbeddableMappingBuilderEmbedOptions;
import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderColumnOptions;
import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderOneToManyOptions;
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
	
	private IdentifierInsertionManager<C, I> identifierInsertionManager;
	
	private PropertyAccessor<C, I> identifierAccessor;
	
	private final MethodReferenceCapturer methodSpy;
	
	private final List<CascadeOne<C, ? extends Identified, ? extends StatefullIdentifier>> cascadeOnes = new ArrayList<>();
	
	private final List<CascadeMany<C, ? extends Identified, ? extends StatefullIdentifier, ? extends Collection>> cascadeManys = new ArrayList<>();
	
	private final FluentEntityMappingConfigurationSupport fluentEntityMappingConfigurationSupport;
	
	private ForeignKeyNamingStrategy foreignKeyNamingStrategy = ForeignKeyNamingStrategy.DEFAULT;
	
	private JoinColumnNamingStrategy columnNamingStrategy = JoinColumnNamingStrategy.DEFAULT;
	
	private AssociationTableNamingStrategy associationTableNamingStrategy = AssociationTableNamingStrategy.DEFAULT;
	
	private OptimisticLockOption optimisticLockOption;
	
	private final Map<Class<? super C>, ClassMappingStrategy<? super C, ?, ?>> inheritanceMapping = new HashMap<>();
	
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
		
		this.fluentEntityMappingConfigurationSupport = new FluentEntityMappingConfigurationSupport(persistedClass);
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
	public <O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableFunction<C, O> getter, String columnName) {
		Method method = captureLambdaMethod(getter);
		return add(method, columnName);
	}
	
	@Override
	public <O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableFunction<C, O> getter, Column<Table, O> column) {
		Method method = captureLambdaMethod(getter);
		return add(method, column);
	}
	
	private <O> IFluentMappingBuilderColumnOptions<C, I> add(Method method, Column<Table, O> column) {
		Linkage<C> mapping = fluentEntityMappingConfigurationSupport.addMapping(method, column);
		return this.fluentEntityMappingConfigurationSupport.applyAdditionalOptions(method, mapping);
	}
	
	private IFluentMappingBuilderColumnOptions<C, I> add(Method method, @javax.annotation.Nullable String columnName) {
		Linkage<C> mapping = fluentEntityMappingConfigurationSupport.addMapping(method, columnName);
		return this.fluentEntityMappingConfigurationSupport.applyAdditionalOptions(method, mapping);
	}
	
	@Override
	public IFluentMappingBuilder<C, I> mapSuperClass(Class<? super C> superType, ClassMappingStrategy<? super C, ?, ?> mappingStrategy) {
		inheritanceMapping.put(superType, mappingStrategy);
		return this;
	}
	
	@Override
	public IFluentMappingBuilder<C, I> mapSuperClass(Class<? super C> superType, EmbeddedBeanMappingStrategy<? super C, ?> mappingStrategy) {
		this.fluentEntityMappingConfigurationSupport.mapSuperClass(superType, mappingStrategy);
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
	public <O> IFluentMappingBuilderEmbedOptions<C, I, O> embed(SerializableBiConsumer<C, O> setter) {
		return embed(fluentEntityMappingConfigurationSupport.embed(setter));
	}
	
	@Override
	public <O> IFluentMappingBuilderEmbedOptions<C, I, O> embed(SerializableFunction<C, O> getter) {
		return embed(fluentEntityMappingConfigurationSupport.embed(getter));
	}
	
	@Override
	public <O> IFluentEmbeddableMappingBuilder<C> embed(SerializableFunction<C, O> getter,
														EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder) {
		return null;
	}
	
	private <O> IFluentMappingBuilderEmbedOptions<C, I, O> embed(IFluentEmbeddableMappingBuilderEmbedOptions<C, O> embedSupport) {
		return new MethodDispatcher()
				.redirect(EmbedWithColumnOptions.class, new EmbedWithColumnOptions() {
					
					@Override
					public EmbedOptions innerEmbed(SerializableBiConsumer setter) {
						embedSupport.innerEmbed(setter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedOptions innerEmbed(SerializableFunction getter) {
						embedSupport.innerEmbed(getter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedOptions overrideName(SerializableFunction function, String columnName) {
						embedSupport.overrideName(function, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedWithColumnOptions exclude(SerializableBiConsumer setter) {
						embedSupport.exclude(setter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedWithColumnOptions exclude(SerializableFunction getter) {
						embedSupport.exclude(getter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public EmbedWithColumnOptions override(SerializableFunction function, Column targetColumn) {
						fluentEntityMappingConfigurationSupport.currentInset().override(function, targetColumn);
						return null;	// we can return null because dispatcher will return proxy
					}
				}, true)
				.fallbackOn(this)
				.build((Class<IFluentMappingBuilderEmbedOptions<C, I, O>>) (Class) IFluentMappingBuilderEmbedOptions.class);
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
	
	private Duo<IReversibleAccessor, Column> collectIdMapping(ClassMappingStrategy entityMappingStrategy) {
		Duo<IReversibleAccessor, Column> result = new Duo<>();
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
		result.setLeft(identifierAccessor);
		result.setRight(projectedPrimarkey);
		return result;
	}
	
	@Override
	public <T extends Table> Persister<C, I, T> build(PersistenceContext persistenceContext) {
		ClassMappingStrategy<C, I, T> mainMappingStrategy = build(persistenceContext.getDialect());
		
		// by default, result is the simple persister of the main strategy
		Persister<C, I, T> result = persistenceContext.add(mainMappingStrategy);
		
		if (!cascadeOnes.isEmpty() || !cascadeManys.isEmpty()) {
			JoinedTablesPersister<C, I, T> joinedTablesPersister = new JoinedTablesPersister<>(persistenceContext, mainMappingStrategy);
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
	public <T extends Table> ClassMappingStrategy<C, I, T> build(Dialect dialect) {
		IReversibleAccessor<C, I> localIdentifierAccessor = this.identifierAccessor;
		if (localIdentifierAccessor == null) {
			if (inheritanceMapping.size() == 0) {
				// no ClassMappingStratey in hierarchy, so we can't get an identifier from it => impossible
				SerializableBiFunction<ColumnOptions, IdentifierPolicy, IFluentMappingBuilder> identifierMethodReference = ColumnOptions::identifier;
				Method identifierSetter = this.methodSpy.findMethod(identifierMethodReference);
				throw new UnsupportedOperationException("Identifier is not defined, please add one throught " + Reflections.toString(identifierSetter));
			} else {
				// at least one parent ClassMappingStrategy exists, it necessarily defines an identifier
				for (ClassMappingStrategy<? super C, ?, ?> inheritedMappingStrategy : inheritanceMapping.values()) {
					IdAccessor<? super C, ?> idAccessor = inheritedMappingStrategy.getIdMappingStrategy().getIdAccessor();
					if (!(idAccessor instanceof SinglePropertyIdAccessor)) {
						throw new NotYetSupportedOperationException();
					}
					localIdentifierAccessor = ((SinglePropertyIdAccessor<C, I>) idAccessor).getIdAccessor();
					break;
				}
			}
		}
		
		Builder builder = fluentEntityMappingConfigurationSupport.new Builder(dialect, table) {
			
			/** Overriden to take property definition by column into account */
			@Override
			protected Column findColumn(Field field, Map<String, Column<Table, Object>> tableColumnsPerName, Inset<C, ?> configuration) {
				Column overridenColumn = ((OverridableColumnInset<C, ?>) configuration).overridenColumns.get(field);
				return Nullable.nullable(overridenColumn).orGet(() -> super.findColumn(field, tableColumnsPerName, configuration));
			}
			
			/** Overriden to take primary key into account */
			@Override
			protected Column addLinkage(Linkage linkage) {
				Column column = super.addLinkage(linkage);
				// setting the primary key option as asked
				if (linkage instanceof EntityLinkage) {	// should always be true, see FluentEntityMappingConfigurationSupport.newLinkage(..)
					if (((EntityLinkage) linkage).isPrimaryKey()) {
						column.primaryKey();
					}
				} else {
					throw new NotImplementedException(linkage.getClass());
				}
				
				return column;
			}
			
			@Override
			protected Map<IReversibleAccessor, Column> buildMappingFromInheritance() {
				Map<IReversibleAccessor, Column> superResult = super.buildMappingFromInheritance();
				inheritanceMapping.forEach((superType, mappingStrategy) -> {
					// We transfer columns and mapping of the inherited source to the current mapping
					EmbeddedBeanMappingStrategy<? super C, ?> embeddableMappingStrategy = mappingStrategy.getMainMappingStrategy();
					superResult.putAll(collectMapping(embeddableMappingStrategy));
					// Dealing with identifier
					Duo<IReversibleAccessor, Column> idMapping = collectIdMapping(mappingStrategy);
					superResult.put(idMapping.getLeft(), idMapping.getRight());
					// getting identifier insertion manager, may be overwritten by class mapping
					FluentMappingBuilder.this.identifierInsertionManager = (IdentifierInsertionManager<C, I>) mappingStrategy.getIdMappingStrategy().getIdentifierInsertionManager();
				});
				return superResult;
			}
		};
		
		Map<IReversibleAccessor, Column> columnMapping = builder.build();
		
		Column primaryKey = columnMapping.get(localIdentifierAccessor);
		if (primaryKey == null) {
			throw new UnsupportedOperationException("Table without primary key is not supported");
		} else {
			List<IReversibleAccessor> identifierAccessors = collect(columnMapping.entrySet(), e -> e.getValue().isPrimaryKey(), Entry::getKey, ArrayList::new);
			if (identifierAccessors.size() > 1) {
				throw new NotYetSupportedOperationException("Composed primary key is not yet supported");
			}
		}
		
		return new ClassMappingStrategy<>(persistedClass, table, (Map) columnMapping, localIdentifierAccessor, this.identifierInsertionManager);
	}
	
	/**
	 * Specialized version of {@link Linkage} for entity use case
	 * 
	 * @param <T>
	 */
	private interface EntityLinkage<T> extends Linkage<T> {
		
		boolean isPrimaryKey();
	}
	
	private static class EntityLinkageByColumnName<T>  extends LinkageByColumnName<T> implements EntityLinkage<T> {
		
		private boolean primaryKey;
		
		/**
		 * Constructor by {@link Method}. Only accessor by method is implemented (since input is from method reference).
		 * (Doing it for field accessor is simple work but not necessary)
		 * 
		 * @param method a {@link PropertyAccessor}
		 * @param columnName an override of the default name that will be generated
		 */
		private EntityLinkageByColumnName(Method method, String columnName) {
			super(method, columnName);
		}
		
		public boolean isPrimaryKey() {
			return primaryKey;
		}
		
		public void primaryKey() {
			this.primaryKey = true;
		}
	}
	
	private static class EntityLinkageByColumn<T> implements EntityLinkage<T> {
		
		private final PropertyAccessor function;
		private final Column column;
		
		/**
		 * Constructor by {@link Method}. Only accessor by method is implemented (since input is from method reference).
		 * (Doing it for field accessor is simple work but not necessary)
		 * @param method a {@link PropertyAccessor}
		 * @param column an override of the default column that would have been generated
		 */
		private EntityLinkageByColumn(Method method, Column column) {
			this.function = Accessors.of(method);
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
	 * Class very close to {@link FluentEmbeddableMappingConfigurationSupport}, but with dedicated methods to entity mapping such as
	 * identifier definition or configuration override by {@link Column}
	 */
	class FluentEntityMappingConfigurationSupport extends FluentEmbeddableMappingConfigurationSupport<C> {
		
		private OverridableColumnInset<C, ?> currentInset;
		
		/**
		 * Creates a builder to map the given class for persistence
		 *
		 * @param persistedClass the class to create a mapping for
		 */
		public FluentEntityMappingConfigurationSupport(Class<C> persistedClass) {
			super(persistedClass);
		}
		
		@Override
		protected <O> Inset<C, O> newInset(SerializableBiConsumer<C, O> setter) {
			this.currentInset = new OverridableColumnInset<>(setter, this);
			return (Inset<C, O>) currentInset;
		}
		
		@Override
		protected <O> Inset<C, O> newInset(SerializableFunction<C, O> getter) {
			this.currentInset = new OverridableColumnInset<>(getter, this);
			return (Inset<C, O>) currentInset;
		}
		
		/**
		 *  
		 * @return the last {@link Inset} built by {@link #newInset(SerializableFunction)} or {@link #newInset(SerializableBiConsumer)}
		 */
		@Override
		protected OverridableColumnInset<C, ?> currentInset() {
			return currentInset;
		}
		
		@Override
		protected LinkageByColumnName<C> newLinkage(Method method, String linkName) {
			return new EntityLinkageByColumnName<>(method, linkName);
		}
		
		@Override
		protected String giveLinkName(Method method) {
			if (Identified.class.isAssignableFrom(Reflections.javaBeanTargetType(method))) {
				return columnNamingStrategy.giveName(method);
			} else {
				return super.giveLinkName(method);
			}
		}
		
		/**
		 * Equivalent of {@link #addMapping(Method, String)} with a {@link Column}
		 * 
		 * @return a new Column aded to the target table, throws an exception if already mapped
		 */
		<O> Linkage<C> addMapping(Method method, Column<Table, O> column) {
			PropertyAccessor<Object, Object> propertyAccessor = Accessors.of(method);
			assertMappingIsNotAlreadyDefined(column.getName(), propertyAccessor);
			return new EntityLinkageByColumn<>(method, column);
		}
		
		private IFluentMappingBuilderColumnOptions<C, I> applyAdditionalOptions(Method method, Linkage newMapping) {
			return new MethodDispatcher()
					.redirect(ColumnOptions.class, identifierPolicy -> {
						// Please note that we don't check for any id presence in inheritance since this will override parent one (see final build()) 
						if (FluentMappingBuilder.this.identifierAccessor != null) {
							throw new IllegalArgumentException("Identifier is already defined by " + identifierAccessor.getAccessor());
						}
						if (identifierPolicy == IdentifierPolicy.ALREADY_ASSIGNED) {
							Class<I> primaryKeyType = Reflections.propertyType(method);
							FluentMappingBuilder.this.identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(primaryKeyType);
							if (newMapping instanceof EntityLinkageByColumnName) {
								// we force primary key so it's not necessary to set it by caller
								((EntityLinkageByColumnName) newMapping).primaryKey();
							} else if (newMapping instanceof EntityLinkageByColumn && !((EntityLinkageByColumn) newMapping).isPrimaryKey()) {
								// safeguard about misconfiguration, even if mapping would work it smells bad configuration
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
					.fallbackOn(FluentMappingBuilder.this)
					.build((Class<IFluentMappingBuilderColumnOptions<C, I>>) (Class) IFluentMappingBuilderColumnOptions.class);
		}
	}
	
	/**
	 * {@link Inset} with override capability for mapping definition by {@link Column}
	 *
	 * @param <SRC> the owner type
	 * @param <TRGT> the target type
	 */
	private static class OverridableColumnInset<SRC, TRGT> extends Inset<SRC, TRGT> {
		
		private final Map<Field, Column> overridenColumns = new HashMap<>();
		
		OverridableColumnInset(SerializableBiConsumer<SRC, TRGT> targetSetter, LambdaMethodUnsheller lambdaMethodUnsheller) {
			super(targetSetter, lambdaMethodUnsheller);
		}
		
		OverridableColumnInset(SerializableFunction<SRC, TRGT> targetGetter, LambdaMethodUnsheller lambdaMethodUnsheller) {
			super(targetGetter, lambdaMethodUnsheller);
		}
		
		public void override(SerializableFunction methodRef, Column column) {
			Method method = super.captureLambdaMethod(methodRef);
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
