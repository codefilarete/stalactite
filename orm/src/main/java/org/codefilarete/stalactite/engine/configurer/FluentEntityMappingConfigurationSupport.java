package org.codefilarete.stalactite.engine.configurer;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.MutatorByMethod;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointByMethodReference;
import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ColumnOptions;
import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.CompositeKeyMappingConfiguration.CompositeKeyLinkage;
import org.codefilarete.stalactite.engine.CompositeKeyMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.ElementCollectionOptions;
import org.codefilarete.stalactite.engine.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.EnumOptions;
import org.codefilarete.stalactite.engine.ExtraTablePropertyOptions;
import org.codefilarete.stalactite.engine.FluentEmbeddableMappingBuilder.FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions;
import org.codefilarete.stalactite.engine.FluentEmbeddableMappingBuilder.FluentEmbeddableMappingBuilderEnumOptions;
import org.codefilarete.stalactite.engine.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.ImportedEmbedWithColumnOptions;
import org.codefilarete.stalactite.engine.InheritanceOptions;
import org.codefilarete.stalactite.engine.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ManyToManyOptions;
import org.codefilarete.stalactite.engine.MapEntryTableNamingStrategy;
import org.codefilarete.stalactite.engine.MapOptions;
import org.codefilarete.stalactite.engine.MapOptions.KeyAsEntityMapOptions;
import org.codefilarete.stalactite.engine.MapOptions.ValueAsEntityMapOptions;
import org.codefilarete.stalactite.engine.MappingConfigurationException;
import org.codefilarete.stalactite.engine.OneToManyOptions;
import org.codefilarete.stalactite.engine.OneToOneOptions;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.PropertyOptions;
import org.codefilarete.stalactite.engine.TableNamingStrategy;
import org.codefilarete.stalactite.engine.VersioningStrategy;
import org.codefilarete.stalactite.engine.configurer.FluentEmbeddableMappingConfigurationSupport.LinkageSupport;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.map.MapRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelation;
import org.codefilarete.stalactite.engine.runtime.AbstractVersioningStrategy.VersioningStrategySupport;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.exception.NotImplementedException;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.codefilarete.tool.function.Serie;
import org.codefilarete.tool.function.TriFunction;
import org.codefilarete.tool.reflect.MethodDispatcher;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static org.codefilarete.tool.Reflections.propertyName;

/**
 * A class that stores configuration made through a {@link FluentEntityMappingBuilder}
 * 
 * @author Guillaume Mary
 */
public class FluentEntityMappingConfigurationSupport<C, I> implements FluentEntityMappingBuilder<C, I>, EntityMappingConfiguration<C, I> {
	
	private final Class<C> classToPersist;
	
	@javax.annotation.Nullable
	private final Table<?> targetTable;
	
	private TableNamingStrategy tableNamingStrategy = TableNamingStrategy.DEFAULT;
	
	private KeyMapping<C, I> keyMapping;
	
	private final MethodReferenceCapturer methodSpy;
	
	private final List<OneToOneRelation<C, Object, Object>> oneToOneRelations = new ArrayList<>();
	
	private final List<OneToManyRelation<C, ?, ?, ? extends Collection>> oneToManyRelations = new ArrayList<>();
	
	private final List<ManyToManyRelation<C, ?, ?, ? extends Collection, ? extends Collection>> manyToManyRelations = new ArrayList<>();
	
	private final List<ElementCollectionRelation<C, ?, ? extends Collection>> elementCollections = new ArrayList<>();
	
	private final List<MapRelation<C, ?, ?, ? extends Map>> maps = new ArrayList<>();
	
	private final EntityDecoratedEmbeddableConfigurationSupport<C, I> propertiesMappingConfigurationDelegate;
	
	private ForeignKeyNamingStrategy foreignKeyNamingStrategy = ForeignKeyNamingStrategy.DEFAULT;
	
	private JoinColumnNamingStrategy joinColumnNamingStrategy = JoinColumnNamingStrategy.JOIN_DEFAULT;
	
	private ColumnNamingStrategy indexColumnNamingStrategy;
	
	private AssociationTableNamingStrategy associationTableNamingStrategy = AssociationTableNamingStrategy.DEFAULT;
	
	private ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy = ElementCollectionTableNamingStrategy.DEFAULT;
	
	private MapEntryTableNamingStrategy mapEntryTableNamingStrategy = MapEntryTableNamingStrategy.DEFAULT;
	
	private OptimisticLockOption optimisticLockOption;
	
	private InheritanceConfigurationSupport<? super C, I> inheritanceConfiguration;
	
	private PolymorphismPolicy<C> polymorphismPolicy;
	
	private EntityFactoryProviderSupport<C, Table> entityFactoryProvider;
	
	/**
	 * Creates a builder to map the given class for persistence
	 *
	 * @param classToPersist the class to create a mapping for
	 */
	public FluentEntityMappingConfigurationSupport(Class<C> classToPersist) {
		this(classToPersist, (Table<?>) null);
	}
	
	public FluentEntityMappingConfigurationSupport(Class<C> classToPersist, String targetTableName) {
		this(classToPersist, new Table<>(targetTableName));
	}
	
	public FluentEntityMappingConfigurationSupport(Class<C> classToPersist, @javax.annotation.Nullable Table<?> targetTable) {
		this.classToPersist = classToPersist;
		this.targetTable = targetTable;
		
		// Helper to capture Method behind method reference
		this.methodSpy = new MethodReferenceCapturer();
		
		this.propertiesMappingConfigurationDelegate = new EntityDecoratedEmbeddableConfigurationSupport<>(this, classToPersist);
	}
	
	@javax.annotation.Nullable
	public Table<?> getTable() {
		return targetTable;
	}
	
	@Override
	public Class<C> getEntityType() {
		return classToPersist;
	}
	
	@Override
	public EntityFactoryProvider<C, Table> getEntityFactoryProvider() {
		return entityFactoryProvider;
	}
	
	@Override
	public TableNamingStrategy getTableNamingStrategy() {
		return tableNamingStrategy;
	}
	
	@Override
	public ColumnNamingStrategy getColumnNamingStrategy() {
		return propertiesMappingConfigurationDelegate.getColumnNamingStrategy();
	}
	
	@Override
	public JoinColumnNamingStrategy getJoinColumnNamingStrategy() {
		return joinColumnNamingStrategy;
	}
	
	@Override
	public ColumnNamingStrategy getIndexColumnNamingStrategy() {
		return indexColumnNamingStrategy;
	}
	
	private Method captureMethod(SerializableFunction getter) {
		return this.methodSpy.findMethod(getter);
	}
	
	private Method captureMethod(SerializableBiConsumer setter) {
		return this.methodSpy.findMethod(setter);
	}
	
	@Override
	public KeyMapping<C, I> getKeyMapping() {
		return keyMapping;
	}
	
	@Override
	public EmbeddableMappingConfiguration<C> getPropertiesMapping() {
		return propertiesMappingConfigurationDelegate;
	}
	
	@Override
	public VersioningStrategy getOptimisticLockOption() {
		return Nullable.nullable(this.optimisticLockOption).map(OptimisticLockOption::getVersioningStrategy).get();
	}
	
	@Override
	public <TRGT, TRGTID> List<OneToOneRelation<C, TRGT, TRGTID>> getOneToOnes() {
		return (List) oneToOneRelations;
	}
	
	@Override
	public <TRGT, TRGTID> List<OneToManyRelation<C, TRGT, TRGTID, Collection<TRGT>>> getOneToManys() {
		return (List) oneToManyRelations;
	}
	
	@Override
	public <TRGT, TRGTID> List<ManyToManyRelation<C, TRGT, TRGTID, Collection<TRGT>, Collection<C>>> getManyToManyRelations() {
		return (List) manyToManyRelations;
	}
	
	@Override
	public <TRGT> List<ElementCollectionRelation<C, TRGT, ? extends Collection<TRGT>>> getElementCollections() {
		return (List) elementCollections;
	}
	
	@Override
	public List<MapRelation<C, ?, ?, ? extends Map>> getMaps() {
		return maps;
	}
	
	@javax.annotation.Nullable
	@Override
	public InheritanceConfiguration<? super C, I> getInheritanceConfiguration() {
		return inheritanceConfiguration;
	}
	
	@Override
	public ForeignKeyNamingStrategy getForeignKeyNamingStrategy() {
		return this.foreignKeyNamingStrategy;
	}
	
	@Override
	public AssociationTableNamingStrategy getAssociationTableNamingStrategy() {
		return this.associationTableNamingStrategy;
	}
	
	@Override
	public ElementCollectionTableNamingStrategy getElementCollectionTableNamingStrategy() {
		return this.elementCollectionTableNamingStrategy;
	}
	
	@Override
	public MapEntryTableNamingStrategy getEntryMapTableNamingStrategy() {
		return this.mapEntryTableNamingStrategy;
	}
	
	@Override
	public EntityMappingConfiguration<C, I> getConfiguration() {
		return this;
	}
	
	@Override
	public PolymorphismPolicy<C> getPolymorphismPolicy() {
		return polymorphismPolicy;
	}
	
	@Override
	public FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableFunction<C, I> getter, IdentifierPolicy<I> identifierPolicy) {
		SingleKeyLinkageSupport<C, I> mapping = propertiesMappingConfigurationDelegate.addKeyMapping(getter, identifierPolicy);
		return this.propertiesMappingConfigurationDelegate.wrapWithKeyOptions(mapping);
	}
	
	@Override
	public <T extends Table> FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableFunction<C, I> getter, IdentifierPolicy<I> identifierPolicy,
																			   Column<T, I> column) {
		SingleKeyLinkageSupport<C, I> mapping = propertiesMappingConfigurationDelegate.addKeyMapping(getter, identifierPolicy, column);
		return this.propertiesMappingConfigurationDelegate.wrapWithKeyOptions(mapping);
	}
	
	@Override
	public FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableFunction<C, I> getter, IdentifierPolicy<I> identifierPolicy,
																			   String columnName) {
		SingleKeyLinkageSupport<C, I> mapping = propertiesMappingConfigurationDelegate.addKeyMapping(getter, identifierPolicy, columnName);
		return this.propertiesMappingConfigurationDelegate.wrapWithKeyOptions(mapping);
	}
	
	@Override
	public FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableBiConsumer<C, I> setter, IdentifierPolicy<I> identifierPolicy) {
		SingleKeyLinkageSupport<C, I> mapping = propertiesMappingConfigurationDelegate.addKeyMapping(setter, identifierPolicy);
		return this.propertiesMappingConfigurationDelegate.wrapWithKeyOptions(mapping);
	}
	
	@Override
	public <T extends Table> FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableBiConsumer<C, I> setter, IdentifierPolicy<I> identifierPolicy, Column<T, I> column) {
		SingleKeyLinkageSupport<C, I> mapping = propertiesMappingConfigurationDelegate.addKeyMapping(setter, identifierPolicy, column);
		return this.propertiesMappingConfigurationDelegate.wrapWithKeyOptions(mapping);
	}
	
	@Override
	public FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableBiConsumer<C, I> setter, IdentifierPolicy<I> identifierPolicy, String columnName) {
		SingleKeyLinkageSupport<C, I> mapping = propertiesMappingConfigurationDelegate.addKeyMapping(setter, identifierPolicy, columnName);
		return this.propertiesMappingConfigurationDelegate.wrapWithKeyOptions(mapping);
	}
	
	@Override
	public FluentEntityMappingBuilderCompositeKeyOptions<C, I> mapCompositeKey(SerializableFunction<C, I> getter,
																			   CompositeKeyMappingConfigurationProvider<I> compositeKeyMappingBuilder,
																			   Consumer<C> markAsPersistedFunction,
																			   Function<C, Boolean> isPersistedFunction) {
		return this.propertiesMappingConfigurationDelegate.wrapWithKeyOptions(
				propertiesMappingConfigurationDelegate.addCompositeKeyMapping(
						Accessors.accessor(getter),
						compositeKeyMappingBuilder,
						markAsPersistedFunction,
						isPersistedFunction));
	}
	
	@Override
	public FluentEntityMappingBuilderCompositeKeyOptions<C, I> mapCompositeKey(SerializableBiConsumer<C, I> setter,
																			   CompositeKeyMappingConfigurationProvider<I> compositeKeyMappingBuilder,
																			   Consumer<C> markAsPersistedFunction,
																			   Function<C, Boolean> isPersistedFunction) {
		return this.propertiesMappingConfigurationDelegate.wrapWithKeyOptions(
				propertiesMappingConfigurationDelegate.addCompositeKeyMapping(
						Accessors.mutator(setter),
						compositeKeyMappingBuilder,
						markAsPersistedFunction,
						isPersistedFunction));
	}
	
	@Override
	public <O> FluentMappingBuilderPropertyOptions<C, I, O> map(SerializableBiConsumer<C, O> setter) {
		LinkageSupport<C, O> mapping = propertiesMappingConfigurationDelegate.addMapping(setter);
		return this.propertiesMappingConfigurationDelegate.wrapWithAdditionalPropertyOptions(mapping);
	}
	
	@Override
	public <O> FluentMappingBuilderPropertyOptions<C, I, O> map(SerializableFunction<C, O> getter) {
		LinkageSupport<C, O> mapping = propertiesMappingConfigurationDelegate.addMapping(getter);
		return this.propertiesMappingConfigurationDelegate.wrapWithAdditionalPropertyOptions(mapping);
	}
	
	@Override
	public <E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I, E> mapEnum(SerializableBiConsumer<C, E> setter) {
		LinkageSupport<C, E> linkage = propertiesMappingConfigurationDelegate.addMapping(setter);
		return wrapEnumOptions(propertiesMappingConfigurationDelegate.wrapWithEnumOptions(linkage));
	}
	
	@Override
	public <E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I, E> mapEnum(SerializableFunction<C, E> getter) {
		LinkageSupport<C, E> linkage = propertiesMappingConfigurationDelegate.addMapping(getter);
		return wrapEnumOptions(propertiesMappingConfigurationDelegate.wrapWithEnumOptions(linkage));
	}
	
	private <E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I, E> wrapEnumOptions(FluentEmbeddableMappingBuilderEnumOptions<C, E> enumOptionsHandler) {
		// we redirect all EnumOptions methods to the instance that can handle them, returning the dispatcher on these methods so one can chain
		// with some other methods, any methods out of EnumOptions are redirected to "this" because it can handle them.
		return new MethodDispatcher()
				.redirect(EnumOptions.class, enumOptionsHandler, true)
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderEnumOptions<C, I, E>>) (Class) FluentMappingBuilderEnumOptions.class);
	}
	
	@Override
	public <K, V, M extends Map<K, V>> FluentMappingBuilderMapOptions<C, I, K, V, M> mapMap(SerializableFunction<C, M> getter, Class<K> keyType, Class<V> valueType) {
		MapRelation<C, K, V, M> mapRelation = new MapRelation<>(getter, keyType, valueType);
		this.maps.add(mapRelation);
		return wrapWithMapOptions(mapRelation);
	}
	
	@Override
	public <K, V, M extends Map<K, V>> FluentMappingBuilderMapOptions<C, I, K, V, M> mapMap(SerializableBiConsumer<C, M> setter, Class<K> keyType, Class<V> valueType) {
		MapRelation<C, K, V, M> mapRelation = new MapRelation<>(setter, keyType, valueType);
		this.maps.add(mapRelation);
		return wrapWithMapOptions(mapRelation);
	}
	
	private <K, V, M extends Map<K, V>> FluentMappingBuilderMapOptions<C, I, K, V, M> wrapWithMapOptions(MapRelation<C, K, V, M> mapRelation) {
		Holder<FluentMappingBuilderMapOptions> proxyHolder = new Holder<>();
		FluentMappingBuilderMapOptions<C, I, K, V, M> result = new MethodReferenceDispatcher()
				.redirect(MapOptions.class, new MapOptions() {
					
					@Override
					public MapOptions withReverseJoinColumn(String columnName) {
						mapRelation.setReverseColumnName(columnName);
						return null;
					}
					
					@Override
					public MapOptions withKeyColumn(String columnName) {
						mapRelation.setKeyColumnName(columnName);
						return null;
					}
					
					@Override
					public MapOptions withValueColumn(String columnName) {
						mapRelation.setValueColumnName(columnName);
						return null;
					}
					
					@Override
					public MapOptions withMapFactory(Supplier mapFactory) {
						mapRelation.setMapFactory(mapFactory);
						return null;
					}
					
					@Override
					public MapOptions withTable(String tableName) {
						mapRelation.setTargetTableName(tableName);
						return null;
					}
					
					@Override
					public MapOptions withTable(Table table) {
						mapRelation.setTargetTable(table);
						return null;
					}
					
					@Override
					public KeyAsEntityMapOptions withKeyMapping(EntityMappingConfigurationProvider mappingConfigurationProvider) {
						// This method is not call because it is overwritten by a dedicated redirect(..) call below
						// mapRelation.setKeyConfigurationProvider(mappingConfigurationProvider);
						return null;
					}
					
					@Override
					public MapOptions withKeyMapping(EmbeddableMappingConfigurationProvider mappingConfigurationProvider) {
						mapRelation.setKeyConfigurationProvider(mappingConfigurationProvider);
						return null;
					}
					
					@Override
					public ValueAsEntityMapOptions withValueMapping(EntityMappingConfigurationProvider mappingConfigurationProvider) {
						// This method is not call because it is overwritten by a dedicated redirect(..) call below
						// mapRelation.setValueConfigurationProvider(mappingConfigurationProvider);
						return null;
					}
					
					@Override
					public MapOptions withValueMapping(EmbeddableMappingConfigurationProvider mappingConfigurationProvider) {
						mapRelation.setValueConfigurationProvider(mappingConfigurationProvider);
						return null;
					}
					
					@Override
					public MapOptions overrideKeyColumnName(SerializableFunction getter, String columnName) {
						mapRelation.overrideKeyName(getter, columnName);
						return null;
					}
					
					@Override
					public MapOptions overrideKeyColumnName(SerializableBiConsumer setter, String columnName) {
						mapRelation.overrideKeyName(setter, columnName);
						return null;
					}
					
					@Override
					public MapOptions overrideValueColumnName(SerializableFunction getter, String columnName) {
						mapRelation.overrideValueName(getter, columnName);
						return null;
					}
					
					@Override
					public MapOptions overrideValueColumnName(SerializableBiConsumer setter, String columnName) {
						mapRelation.overrideValueName(setter, columnName);
						return null;
					}
					
					@Override
					public MapOptions fetchSeparately() {
						mapRelation.fetchSeparately();
						return null;
					}
				}, true)
				// This will overwrite withKeyMapping(EntityMappingConfigurationProvider) capture to return a proxy
				// that will let us configure cascading of the relation
				.redirect((SerializableBiFunction<MapOptions, EntityMappingConfigurationProvider, KeyAsEntityMapOptions>) MapOptions::withKeyMapping,
						entityMappingConfigurationProvider -> {
							mapRelation.setKeyConfigurationProvider(entityMappingConfigurationProvider);
							return new MethodReferenceDispatcher()
									.redirect(KeyAsEntityMapOptions.class, relationMode -> {
										mapRelation.setKeyEntityRelationMode(relationMode);
										return null;
									}, true)
									.fallbackOn(proxyHolder.get())
									.build((Class<FluentMappingBuilderKeyAsEntityMapOptions<C, I, K, V, M>>) (Class) FluentMappingBuilderKeyAsEntityMapOptions.class);
						})
				.redirect((SerializableBiFunction<MapOptions, EntityMappingConfigurationProvider, ValueAsEntityMapOptions>) MapOptions::withValueMapping,
						entityMappingConfigurationProvider -> {
							mapRelation.setValueConfigurationProvider(entityMappingConfigurationProvider);
							return new MethodReferenceDispatcher()
									.redirect(ValueAsEntityMapOptions.class, relationMode -> {
										mapRelation.setValueEntityRelationMode(relationMode);
										return null;
									}, true)
									.fallbackOn(proxyHolder.get())
									.build((Class<FluentMappingBuilderValueAsEntityMapOptions<C, I, K, V, M>>) (Class) FluentMappingBuilderValueAsEntityMapOptions.class);
						})
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderMapOptions<C, I, K, V, M>>) (Class) FluentMappingBuilderMapOptions.class);
		proxyHolder.set(result);
		return result;
	}
	
	@Override
	public <O, S extends Collection<O>> FluentMappingBuilderElementCollectionOptions<C, I, O, S> mapCollection(SerializableFunction<C, S> getter,
																											   Class<O> componentType) {
		ElementCollectionRelation<C, O, S> elementCollectionRelation = new ElementCollectionRelation<>(getter, componentType,
				propertiesMappingConfigurationDelegate, null);
		elementCollections.add(elementCollectionRelation);
		return wrapWithElementCollectionOptions(elementCollectionRelation);
	}
	
	@Override
	public <O, S extends Collection<O>> FluentMappingBuilderElementCollectionOptions<C, I, O, S> mapCollection(SerializableBiConsumer<C, S> setter,
																											   Class<O> componentType) {
		ElementCollectionRelation<C, O, S> elementCollectionRelation = new ElementCollectionRelation<>(setter, componentType, null);
		elementCollections.add(elementCollectionRelation);
		return wrapWithElementCollectionOptions(elementCollectionRelation);
	}
	
	private <O, S extends Collection<O>> FluentMappingBuilderElementCollectionOptions<C, I, O, S> wrapWithElementCollectionOptions(
			ElementCollectionRelation<C, O, S> elementCollectionRelation) {
		return new MethodReferenceDispatcher()
				.redirect((SerializableBiFunction<FluentMappingBuilderElementCollectionOptions, String, FluentMappingBuilderElementCollectionOptions>)
								FluentMappingBuilderElementCollectionOptions::override,
						elementCollectionRelation::overrideColumnName)
				.redirect(ElementCollectionOptions.class, wrapAsOptions(elementCollectionRelation), true)
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderElementCollectionOptions<C, I, O, S>>) (Class) FluentMappingBuilderElementCollectionOptions.class);
	}
	
	@Override
	public <O, S extends Collection<O>> FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> mapCollection(SerializableFunction<C, S> getter,
																														  Class<O> componentType,
																														  EmbeddableMappingConfigurationProvider<O> embeddableConfiguration) {
		ElementCollectionRelation<C, O, S> elementCollectionRelation = new ElementCollectionRelation<>(getter, componentType,
				propertiesMappingConfigurationDelegate,
				embeddableConfiguration);
		elementCollections.add(elementCollectionRelation);
		return wrapWithElementCollectionImportOptions(elementCollectionRelation);
	}
	
	@Override
	public <O, S extends Collection<O>> FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> mapCollection(SerializableBiConsumer<C, S> setter,
																														  Class<O> componentType,
																														  EmbeddableMappingConfigurationProvider<O> embeddableConfiguration) {
		ElementCollectionRelation<C, O, S> elementCollectionRelation = new ElementCollectionRelation<>(setter, componentType, embeddableConfiguration);
		elementCollections.add(elementCollectionRelation);
		return wrapWithElementCollectionImportOptions(elementCollectionRelation);
	}
	
	private <O, S extends Collection<O>> FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> wrapWithElementCollectionImportOptions(
			ElementCollectionRelation<C, O, S> elementCollectionRelation) {
		return new MethodReferenceDispatcher()
				.redirect((SerializableTriFunction<FluentMappingBuilderElementCollectionImportEmbedOptions, SerializableFunction, String, FluentMappingBuilderElementCollectionImportEmbedOptions>)
								FluentMappingBuilderElementCollectionImportEmbedOptions::overrideName,
						(BiConsumer<SerializableFunction, String>) elementCollectionRelation::overrideName)
				.redirect(ElementCollectionOptions.class, wrapAsOptions(elementCollectionRelation), true)
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S>>) (Class) FluentMappingBuilderElementCollectionImportEmbedOptions.class);
	}
	
	private <O, S extends Collection<O>> ElementCollectionOptions<C, O, S> wrapAsOptions(ElementCollectionRelation<C, O, S> elementCollectionRelation) {
		return new ElementCollectionOptions<C, O, S>() {
			
			@Override
			public ElementCollectionOptions<C, O, S> withCollectionFactory(Supplier<? extends S> collectionFactory) {
				elementCollectionRelation.setCollectionFactory(collectionFactory);
				return null;
			}
			
			@Override
			public FluentMappingBuilderElementCollectionOptions<C, I, O, S> withReverseJoinColumn(String name) {
				elementCollectionRelation.setReverseColumnName(name);
				return null;
			}
			
			@Override
			public ElementCollectionOptions<C, O, S> withTable(Table table) {
				elementCollectionRelation.setTargetTable(table);
				return null;
			}
			
			@Override
			public ElementCollectionOptions<C, O, S> withTable(String tableName) {
				elementCollectionRelation.setTargetTableName(tableName);
				return null;
			}
			
		};
	}
	
	@Override
	public FluentMappingBuilderInheritanceOptions<C, I> mapSuperClass(EntityMappingConfigurationProvider<? super C, I> mappingConfiguration) {
		inheritanceConfiguration = new InheritanceConfigurationSupport<>(mappingConfiguration.getConfiguration());
		return new MethodReferenceDispatcher()
				.redirect((SerializableFunction<InheritanceOptions, InheritanceOptions>) InheritanceOptions::withJoinedTable,
						() -> this.inheritanceConfiguration.joinTable = true)
				.redirect((SerializableBiFunction<InheritanceOptions, Table, InheritanceOptions>) InheritanceOptions::withJoinedTable,
						t -> { this.inheritanceConfiguration.joinTable = true; this.inheritanceConfiguration.table = t;})
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderInheritanceOptions<C, I>>) (Class) FluentMappingBuilderInheritanceOptions.class);
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> mapSuperClass(EmbeddableMappingConfigurationProvider<? super C> superMappingConfiguration) {
		this.propertiesMappingConfigurationDelegate.mapSuperClass(superMappingConfiguration);
		return this;
	}
	
	@Override
	public <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T, O> mapOneToOne(
			SerializableBiConsumer<C, O> setter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			T table) {
		// we keep close to user demand: we keep its method reference ...
		Mutator<C, O> mutatorByMethodReference = Accessors.mutatorByMethodReference(setter);
		// ... but we can't do it for accessor, so we use the most equivalent manner: an accessor based on setter method (fallback to property if not present)
		Accessor<C, O> accessor = new MutatorByMethod<C, O>(captureMethod(setter)).toAccessor();
		return mapOneToOne(accessor, mutatorByMethodReference, mappingConfiguration, table);
	}
	
	@Override
	public <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T, O> mapOneToOne(
			SerializableFunction<C, O> getter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			T table) {
		// we keep close to user demand: we keep its method reference ...
		AccessorByMethodReference<C, O> accessorByMethodReference = Accessors.accessorByMethodReference(getter);
		// ... but we can't do it for mutator, so we use the most equivalent manner: a mutator based on getter method (fallback to property if not present)
		Mutator<C, O> mutator = new AccessorByMethod<C, O>(captureMethod(getter)).toMutator();
		return mapOneToOne(accessorByMethodReference, mutator, mappingConfiguration, table);
	}
	
	private <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T, O> mapOneToOne(
			Accessor<C, O> accessor,
			Mutator<C, O> mutator,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			T table) {
		OneToOneRelation<C, O, J> oneToOneRelation = new OneToOneRelation<>(
				new PropertyAccessor<>(accessor, mutator),
				() -> this.polymorphismPolicy instanceof PolymorphismPolicy.TablePerClassPolymorphism,
				mappingConfiguration,
				table);
		this.oneToOneRelations.add((OneToOneRelation<C, Object, Object>) oneToOneRelation);
		return wrapForAdditionalOptions(oneToOneRelation);
	}
	
	private <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T, O> wrapForAdditionalOptions(final OneToOneRelation<C, O, J> oneToOneRelation) {
		// then we return an object that allows fluent settings over our OneToOne cascade instance
		return new MethodDispatcher()
				.redirect(OneToOneOptions.class, new OneToOneOptions() {
					@Override
					public OneToOneOptions cascading(RelationMode relationMode) {
						oneToOneRelation.setRelationMode(relationMode);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions mandatory() {
						oneToOneRelation.setNullable(false);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions mappedBy(SerializableFunction reverseLink) {
						oneToOneRelation.setReverseGetter(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions mappedBy(SerializableBiConsumer reverseLink) {
						oneToOneRelation.setReverseSetter(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions mappedBy(Column reverseLink) {
						oneToOneRelation.setReverseColumn(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions fetchSeparately() {
						oneToOneRelation.fetchSeparately();
						return null;	// we can return null because dispatcher will return proxy
					}
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderOneToOneOptions<C, I, T, O>>) (Class) FluentMappingBuilderOneToOneOptions.class);
	}
	
	@Override
	public <O, J, S extends Collection<O>, T extends Table> FluentMappingBuilderOneToManyOptions<C, I, O, S> mapOneToMany(
			SerializableFunction<C, S> getter,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		
		AccessorByMethodReference<C, S> getterReference = Accessors.accessorByMethodReference(getter);
		ReversibleAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				getterReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<C, S>(captureMethod(getter)).toMutator());
		return mapOneToMany(propertyAccessor, getterReference, mappingConfiguration, table);
	}
	
	@Override
	public <O, J, S extends Collection<O>, T extends Table> FluentMappingBuilderOneToManyOptions<C, I, O, S> mapOneToMany(
			SerializableBiConsumer<C, S> setter,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		
		MutatorByMethodReference<C, S> setterReference = Accessors.mutatorByMethodReference(setter);
		PropertyAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				Accessors.accessor(setterReference.getDeclaringClass(), propertyName(setterReference.getMethodName())),
				setterReference
		);
		return mapOneToMany(propertyAccessor, setterReference, mappingConfiguration, table);
	}
	
	private <O, J, S extends Collection<O>> FluentMappingBuilderOneToManyOptions<C, I, O, S> mapOneToMany(
			ReversibleAccessor<C, S> propertyAccessor,
			ValueAccessPointByMethodReference methodReference,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration,
			@javax.annotation.Nullable Table table) {
		OneToManyRelation<C, O, J, S> oneToManyRelation = new OneToManyRelation<>(
				propertyAccessor,
				methodReference,
				() -> this.polymorphismPolicy instanceof PolymorphismPolicy.TablePerClassPolymorphism,
				mappingConfiguration,
				table);
		this.oneToManyRelations.add(oneToManyRelation);
		return new MethodDispatcher()
				.redirect(OneToManyOptions.class, new OneToManyOptionsSupport<>(oneToManyRelation), true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderOneToManyOptions<C, I, O, S>>) (Class) FluentMappingBuilderOneToManyOptions.class);
	}
	
	@Override
	public <O, J, S1 extends Set<O>, S2 extends Set<C>, T extends Table>
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2>
	mapManyToMany(SerializableFunction<C, S1> getter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration, @javax.annotation.Nullable T table) {
		AccessorByMethodReference<C, S1> getterReference = Accessors.accessorByMethodReference(getter);
		ReversibleAccessor<C, S1> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				getterReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<C, S1>(captureMethod(getter)).toMutator());
		return mapManyToManySet(propertyAccessor, getterReference, mappingConfiguration, table);
	}
	
	@Override
	public <O, J, S1 extends Set<O>, S2 extends Set<C>, T extends Table>
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2>
	mapManyToMany(SerializableBiConsumer<C, S1> setter,
				  EntityMappingConfigurationProvider<? super O, J> mappingConfiguration,
				  @javax.annotation.Nullable T table) {
		MutatorByMethodReference<C, S1> setterReference = Accessors.mutatorByMethodReference(setter);
		PropertyAccessor<C, S1> propertyAccessor = new PropertyAccessor<>(
				Accessors.accessor(setterReference.getDeclaringClass(), propertyName(setterReference.getMethodName())),
				setterReference
		);
		return mapManyToManySet(propertyAccessor, setterReference, mappingConfiguration, table);
	}
	
	private <O, J, S1 extends Set<O>, S2 extends Set<C>, T extends Table> FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> mapManyToManySet(
			ReversibleAccessor<C, S1> propertyAccessor,
			ValueAccessPointByMethodReference methodReference,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		ManyToManyRelation<C, O, J, S1, S2> manyToManyRelation = new ManyToManyRelation<>(propertyAccessor, methodReference, this, mappingConfiguration, table);
		this.manyToManyRelations.add(manyToManyRelation);
		return new MethodDispatcher()
				.redirect(ManyToManyOptions.class, new ManyToManyOptionsSupport<>(manyToManyRelation), true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2>>) (Class) FluentMappingBuilderManyToManyOptions.class);
	}
	
	@Override
	public <O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableFunction<C, O> getter,
																									 EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder) {
		return embed(propertiesMappingConfigurationDelegate.embed(getter, embeddableMappingBuilder));
	}
	
	@Override
	public <O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableBiConsumer<C, O> setter,
																									 EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder) {
		return embed(propertiesMappingConfigurationDelegate.embed(setter, embeddableMappingBuilder));
	}
	
	private <O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> support) {
		return new MethodDispatcher()
				.redirect(ImportedEmbedWithColumnOptions.class, new ImportedEmbedWithColumnOptions() {
					@Override
					public ImportedEmbedWithColumnOptions overrideName(SerializableBiConsumer setter, String columnName) {
						support.overrideName(setter, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ImportedEmbedWithColumnOptions overrideName(SerializableFunction getter, String columnName) {
						support.overrideName(getter, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ImportedEmbedWithColumnOptions override(SerializableBiConsumer setter, Column targetColumn) {
						propertiesMappingConfigurationDelegate.currentInset().override(setter, targetColumn);
						return null;
					}
					
					@Override
					public ImportedEmbedWithColumnOptions override(SerializableFunction getter, Column targetColumn) {
						propertiesMappingConfigurationDelegate.currentInset().override(getter, targetColumn);
						return null;
					}
					
					@Override
					public ImportedEmbedWithColumnOptions exclude(SerializableBiConsumer setter) {
						support.exclude(setter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ImportedEmbedWithColumnOptions exclude(SerializableFunction getter) {
						support.exclude(getter);
						return null;	// we can return null because dispatcher will return proxy
					}
				}, true)
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O>>) (Class) FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions.class);
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> withElementCollectionTableNaming(ElementCollectionTableNamingStrategy tableNamingStrategy) {
		this.elementCollectionTableNamingStrategy = tableNamingStrategy;
		return this;
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> withMapEntryTableNaming(MapEntryTableNamingStrategy tableNamingStrategy) {
		this.mapEntryTableNamingStrategy = tableNamingStrategy;
		return this;
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> withForeignKeyNaming(ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		return this;
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> withTableNaming(TableNamingStrategy tableNamingStrategy) {
		this.tableNamingStrategy = tableNamingStrategy;
		return this;
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> withColumnNaming(ColumnNamingStrategy columnNamingStrategy) {
		this.propertiesMappingConfigurationDelegate.withColumnNaming(columnNamingStrategy);
		return this;
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> withJoinColumnNaming(JoinColumnNamingStrategy columnNamingStrategy) {
		this.joinColumnNamingStrategy = columnNamingStrategy;
		return this;
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> withIndexColumnNamingStrategy(ColumnNamingStrategy columnNamingStrategy) {
		this.indexColumnNamingStrategy = columnNamingStrategy;
		return this;
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> withAssociationTableNaming(AssociationTableNamingStrategy associationTableNamingStrategy) {
		this.associationTableNamingStrategy = associationTableNamingStrategy;
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
	public <V> FluentEntityMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter) {
		AccessorByMethodReference methodReference = Accessors.accessorByMethodReference(getter);
		Serie<V> serie;
		if (Integer.class.isAssignableFrom(methodReference.getPropertyType())) {
			serie = (Serie<V>) Serie.INTEGER_SERIE;
		} else if (Long.class.isAssignableFrom(methodReference.getPropertyType())) {
			serie = (Serie<V>) Serie.LONG_SERIE;
		} else if (Date.class.isAssignableFrom(methodReference.getPropertyType())) {
			serie = (Serie<V>) Serie.NOW_SERIE;
		} else {
			throw new NotImplementedException("Type of versioned property is not implemented, please provide a "
					+ Serie.class.getSimpleName() + " for it : " + Reflections.toString(methodReference.getPropertyType()));
		}
		return versionedBy(getter, methodReference, serie);
	}
	
	@Override
	public <V> FluentEntityMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter, Serie<V> serie) {
		return versionedBy(getter, new AccessorByMethodReference<>(getter), serie);
	}
	
	private <V> FluentEntityMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter, AccessorByMethodReference methodReference, Serie<V> serie) {
		optimisticLockOption = new OptimisticLockOption<>(methodReference, serie);
		map(getter);
		return this;
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> mapPolymorphism(PolymorphismPolicy<C> polymorphismPolicy) {
		this.polymorphismPolicy = polymorphismPolicy;
		return this;
	}
	
	@Override
	public ConfiguredRelationalPersister<C, I> build(PersistenceContext persistenceContext) {
		PersisterBuilderImpl<C, I> persisterBuilder = new PersisterBuilderImpl<>(this.getConfiguration());
		return persisterBuilder.build(persistenceContext);
	}
	
	/**
	 * Class very close to {@link FluentEmbeddableMappingConfigurationSupport}, but with dedicated methods to entity mapping such as
	 * identifier definition or configuration override by {@link Column}
	 */
	static class EntityDecoratedEmbeddableConfigurationSupport<C, I> extends FluentEmbeddableMappingConfigurationSupport<C> {
		
		private final FluentEntityMappingConfigurationSupport<C, I> entityConfigurationSupport;
		
		/**
		 * Creates a builder to map the given class for persistence
		 *
		 * @param persistedClass the class to create a mapping for
		 */
		public EntityDecoratedEmbeddableConfigurationSupport(FluentEntityMappingConfigurationSupport<C, I> entityConfigurationSupport, Class<C> persistedClass) {
			super(persistedClass);
			this.entityConfigurationSupport = entityConfigurationSupport;
		}
		
		<E> LinkageSupport<C, E> addMapping(SerializableBiConsumer<C, E> setter) {
			LinkageSupport<C, E> newLinkage = new LinkageSupport<>(setter);
			mapping.add(newLinkage);
			return newLinkage;
		}
		
		<E> LinkageSupport<C, E> addMapping(SerializableFunction<C, E> getter) {
			LinkageSupport<C, E> newLinkage = new LinkageSupport<>(getter);
			mapping.add(newLinkage);
			return newLinkage;
		}
		
		private <O> FluentMappingBuilderPropertyOptions<C, I, O> wrapWithAdditionalPropertyOptions(LinkageSupport<C, O> newMapping) {
			return new MethodDispatcher()
					.redirect(ColumnOptions.class, new ColumnOptions<O>() {
						@Override
						public ColumnOptions<O> mandatory() {
							newMapping.setNullable(false);
							return null;
						}
						
						@Override
						public ColumnOptions<O> setByConstructor() {
							newMapping.setByConstructor();
							return null;
						}
						
						@Override
						public ColumnOptions<O> readonly() {
							newMapping.readonly();
							return null;
						}
						
						@Override
						public ColumnOptions<O> columnName(String name) {
							newMapping.setColumnOptions(new ColumnLinkageOptionsByName(name));
							return null;
						}
						
						@Override
						public ColumnOptions<O> column(Column<? extends Table, ? extends O> column) {
							newMapping.setColumnOptions(new ColumnLinkageOptionsByColumn(column));
							return null;
						}
						
						@Override
						public ColumnOptions<O> fieldName(String name) {
							Field field = Reflections.findField(EntityDecoratedEmbeddableConfigurationSupport.this.entityConfigurationSupport.classToPersist, name);
							if (field == null) {
								throw new MappingConfigurationException(("Field " + name
										+ " was not found in " + Reflections.toString(EntityDecoratedEmbeddableConfigurationSupport.this.entityConfigurationSupport.classToPersist)));
							}
							newMapping.setField(field);
							return null;
						}
						
						@Override
						public ColumnOptions<O> readConverter(Converter<O, O> converter) {
							newMapping.setReadConverter(converter);
							return null;
						}
						
						@Override
						public ColumnOptions<O> writeConverter(Converter<O, O> converter) {
							newMapping.setWriteConverter(converter);
							return null;
						}
						
						@Override
						public <V> PropertyOptions<O> sqlBinder(ParameterBinder<V> parameterBinder) {
							newMapping.setParameterBinder(parameterBinder);
							return null;
						}
					}, true)
					.redirect(ExtraTablePropertyOptions.class, name -> {
						newMapping.setExtraTableName(name);
						return null;
					}, true)
					.fallbackOn(entityConfigurationSupport)
					.build((Class<FluentMappingBuilderPropertyOptions<C, I, O>>) (Class) FluentMappingBuilderPropertyOptions.class);
		}
		
		SingleKeyLinkageSupport<C, I> addKeyMapping(SerializableFunction<C, I> getter, IdentifierPolicy<I> identifierPolicy) {
			return addKeyMapping(Accessors.accessor(getter), identifierPolicy);
		}
		
		SingleKeyLinkageSupport<C, I> addKeyMapping(SerializableFunction<C, I> getter, IdentifierPolicy<I> identifierPolicy, Column<?, I> column) {
			SingleKeyLinkageSupport<C, I> linkage = addKeyMapping(Accessors.accessor(getter), identifierPolicy);
			linkage.setColumnOptions(new ColumnLinkageOptionsByColumn(column));
			return linkage;
		}
		
		SingleKeyLinkageSupport<C, I> addKeyMapping(SerializableFunction<C, I> getter, IdentifierPolicy<I> identifierPolicy, String columnName) {
			SingleKeyLinkageSupport<C, I> linkage = addKeyMapping(Accessors.accessor(getter), identifierPolicy);
			linkage.setColumnOptions(new ColumnLinkageOptionsByName(columnName));
			return linkage;
		}
		
		SingleKeyLinkageSupport<C, I> addKeyMapping(SerializableBiConsumer<C, I> setter, IdentifierPolicy<I> identifierPolicy) {
			return addKeyMapping(Accessors.mutator(setter), identifierPolicy);
		}
		
		SingleKeyLinkageSupport<C, I> addKeyMapping(SerializableBiConsumer<C, I> setter, IdentifierPolicy<I> identifierPolicy, Column<?, I> column) {
			SingleKeyLinkageSupport<C, I> linkage = addKeyMapping(Accessors.mutator(setter), identifierPolicy);
			linkage.setColumnOptions(new ColumnLinkageOptionsByColumn(column));
			return linkage;
		}
		
		SingleKeyLinkageSupport<C, I> addKeyMapping(SerializableBiConsumer<C, I> setter, IdentifierPolicy<I> identifierPolicy, String columnName) {
			SingleKeyLinkageSupport<C, I> linkage = addKeyMapping(Accessors.mutator(setter), identifierPolicy);
			linkage.setColumnOptions(new ColumnLinkageOptionsByName(columnName));
			return linkage;
		}
		
		/**
		 * 
		 * @param propertyAccessor
		 * @param identifierPolicy
		 * @return
		 */
		private SingleKeyLinkageSupport<C, I> addKeyMapping(ReversibleAccessor<C, I> propertyAccessor, IdentifierPolicy<I> identifierPolicy) {
			
			// Please note that we don't check for any id presence in inheritance since this will override parent one (see final build()) 
			if (entityConfigurationSupport.keyMapping != null) {
				throw new IllegalArgumentException("Identifier is already defined by " + AccessorDefinition.toString(entityConfigurationSupport.keyMapping.getAccessor()));
			}
			SingleKeyLinkageSupport<C, I> newLinkage = new SingleKeyLinkageSupport<>(propertyAccessor, identifierPolicy);
			entityConfigurationSupport.keyMapping = newLinkage;
			return newLinkage;
		}
		
		/**
		 *
		 * @param propertyAccessor
		 * @return
		 */
		private CompositeKeyLinkageSupport<C, I> addCompositeKeyMapping(ReversibleAccessor<C, I> propertyAccessor,
																		CompositeKeyMappingConfigurationProvider<I> compositeKeyMappingBuilder,
																		Consumer<C> markAsPersistedFunction,
																		Function<C, Boolean> isPersistedFunction) {
			
			// Please note that we don't check for any id presence in inheritance since this will override parent one (see final build()) 
			if (entityConfigurationSupport.keyMapping != null) {
				throw new IllegalArgumentException("Identifier is already defined by " + AccessorDefinition.toString(entityConfigurationSupport.keyMapping.getAccessor()));
			}
			CompositeKeyLinkageSupport<C, I> newLinkage = new CompositeKeyLinkageSupport<>(propertyAccessor, compositeKeyMappingBuilder, markAsPersistedFunction, isPersistedFunction);
			entityConfigurationSupport.keyMapping = newLinkage;
			return newLinkage;
		}
		
		private FluentEntityMappingBuilderKeyOptions<C, I> wrapWithKeyOptions(SingleKeyLinkageSupport<C, I> keyMapping) {
			return new MethodDispatcher()
					.redirect(KeyOptions.class, new KeyOptions<C, I>() {
						
						@Override
						public KeyOptions<C, I> usingConstructor(Supplier<C> factory) {
							entityConfigurationSupport.entityFactoryProvider = new EntityFactoryProviderSupport<>(table -> row -> factory.get(), false);
							return null;
						}
						
						@Override
						public KeyOptions<C, I> usingConstructor(Function<? super I, C> factory) {
							keyMapping.setByConstructor();
							entityConfigurationSupport.entityFactoryProvider = new EntityFactoryProviderSupport<>(table -> {
								Column<?, I> primaryKey = (Column<?, I>) Iterables.first(((Table<?>) table).getPrimaryKey().getColumns());
								return row -> factory.apply((I) row.get(primaryKey));
							}, true);
							return null;
						}
						
						@Override
						public <T extends Table<T>> KeyOptions<C, I> usingConstructor(Function<? super I, C> factory, Column<T, I> input) {
							keyMapping.setColumnOptions(new ColumnLinkageOptionsByColumn(input));
							keyMapping.setByConstructor();
							entityConfigurationSupport.entityFactoryProvider = new EntityFactoryProviderSupport<>(table -> row -> factory.apply((I) row.get(input)), true);
							return null;
						}
						
						@Override
						public KeyOptions<C, I> usingConstructor(Function<? super I, C> factory, String columnName) {
							keyMapping.setColumnOptions(new ColumnLinkageOptionsByName(columnName));
							keyMapping.setByConstructor();
							entityConfigurationSupport.entityFactoryProvider = new EntityFactoryProviderSupport<>(table -> row -> factory.apply((I) row.get(table.getColumn(columnName))), true);
							return null;
						}
						
						@Override
						public <X, T extends Table<T>> KeyOptions<C, I> usingConstructor(BiFunction<? super I, X, C> factory,
																						 Column<T, I> input1,
																						 Column<T, X> input2) {
							keyMapping.setColumnOptions(new ColumnLinkageOptionsByColumn(input1));
							keyMapping.setByConstructor();
							entityConfigurationSupport.entityFactoryProvider = new EntityFactoryProviderSupport<>(
									table -> row -> factory.apply(
											(I) row.get(input1),
											(X) row.get(input2)),
									true);
							return null;
						}
						
						@Override
						public <X> KeyOptions<C, I> usingConstructor(BiFunction<? super I, X, C> factory,
																	 String columnName1,
																	 String columnName2) {
							keyMapping.setColumnOptions(new ColumnLinkageOptionsByName(columnName1));
							keyMapping.setByConstructor();
							entityConfigurationSupport.entityFactoryProvider = new EntityFactoryProviderSupport<>(
									table -> row -> factory.apply(
											(I) row.get(table.getColumn(columnName1)),
											(X) row.get(table.getColumn(columnName2))),
									true);
							return null;
						}
						
						
						@Override
						public <X, Y, T extends Table<T>> KeyOptions<C, I> usingConstructor(TriFunction<? super I, X, Y, C> factory,
																							Column<T, I> input1,
																							Column<T, X> input2,
																							Column<T, Y> input3) {
							keyMapping.setColumnOptions(new ColumnLinkageOptionsByColumn(input1));
							keyMapping.setByConstructor();
							entityConfigurationSupport.entityFactoryProvider = new EntityFactoryProviderSupport<>(
									table -> row -> factory.apply(
											(I) row.get(input1),
											(X) row.get(input2),
											(Y) row.get(input3)),
									true);
							return null;
						}
						
						@Override
						public <X, Y> KeyOptions<C, I> usingConstructor(TriFunction<? super I, X, Y, C> factory,
																		String columnName1,
																		String columnName2,
																		String columnName3) {
							keyMapping.setColumnOptions(new ColumnLinkageOptionsByName(columnName1));
							keyMapping.setByConstructor();
							entityConfigurationSupport.entityFactoryProvider = new EntityFactoryProviderSupport<>(
									table -> row -> factory.apply(
											(I) row.get(table.getColumn(columnName1)),
											(X) row.get(table.getColumn(columnName2)),
											(Y) row.get(table.getColumn(columnName3))),
									true);
							return null;
						}
						
						@Override
						public KeyOptions<C, I> usingFactory(Function<Function<Column<?, ?>, ?>, C> factory) {
							keyMapping.setByConstructor();
							entityConfigurationSupport.entityFactoryProvider = new EntityFactoryProviderSupport<>(table -> row -> (C) factory.apply(row::get), true);
							return null;
						}
					}, true)
					.fallbackOn(entityConfigurationSupport)
					.build((Class<FluentEntityMappingBuilderKeyOptions<C, I>>) (Class) FluentEntityMappingBuilderKeyOptions.class);
		}
		
		private FluentEntityMappingBuilderCompositeKeyOptions<C, I> wrapWithKeyOptions(CompositeKeyLinkageSupport<C, I> keyMapping) {
			return new MethodDispatcher()
					.redirect(CompositeKeyOptions.class, new CompositeKeyOptions<C, I>() {
						
					}, true)
					.fallbackOn(entityConfigurationSupport)
					.build((Class<FluentEntityMappingBuilderCompositeKeyOptions<C, I>>) (Class) FluentEntityMappingBuilderCompositeKeyOptions.class);
		}
	}
	
	private static class OptimisticLockOption<C> {
		
		private final VersioningStrategy<Object, C> versioningStrategy;
		
		public OptimisticLockOption(AccessorByMethodReference<Object, C> versionAccessor, Serie<C> serie) {
			this.versioningStrategy = new VersioningStrategySupport<>(new PropertyAccessor<>(
					versionAccessor,
					Accessors.mutator(versionAccessor.getDeclaringClass(), propertyName(versionAccessor.getMethodName()), versionAccessor.getPropertyType())
			), serie);
		}
		
		public VersioningStrategy getVersioningStrategy() {
			return versioningStrategy;
		}
	}
	
	/**
	 * A small class for one-to-many options storage into a {@link OneToManyRelation}. Acts as a wrapper over it.
	 */
	static class OneToManyOptionsSupport<C, I, O, S extends Collection<O>>
			implements OneToManyOptions<C, I, O, S> {
		
		private final OneToManyRelation<C, O, I, S> oneToManyRelation;
		
		public OneToManyOptionsSupport(OneToManyRelation<C, O, I, S> oneToManyRelation) {
			this.oneToManyRelation = oneToManyRelation;
		}
		
		@Override
		public FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(SerializableBiConsumer<O, ? super C> reverseLink) {
			oneToManyRelation.setReverseSetter(reverseLink);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(SerializableFunction<O, ? super C> reverseLink) {
			oneToManyRelation.setReverseGetter(reverseLink);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(Column<Table, ?> reverseLink) {
			oneToManyRelation.setReverseColumn(reverseLink);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public OneToManyOptions<C, I, O, S> reverselySetBy(SerializableBiConsumer<O, C> reverseLink) {
			oneToManyRelation.setReverseLink(reverseLink);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public FluentMappingBuilderOneToManyOptions<C, I, O, S> initializeWith(Supplier<S> collectionFactory) {
			oneToManyRelation.setCollectionFactory(collectionFactory);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public FluentMappingBuilderOneToManyOptions<C, I, O, S> cascading(RelationMode relationMode) {
			oneToManyRelation.setRelationMode(relationMode);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public FluentMappingBuilderOneToManyOptions<C, I, O, S> fetchSeparately() {
			oneToManyRelation.fetchSeparately();
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public FluentMappingBuilderOneToManyOptions<C, I, O, S> indexedBy(Column<?, Integer> orderingColumn) {
			oneToManyRelation.setIndexingColumn(orderingColumn);
			return null;
		}
		
		@Override
		public FluentMappingBuilderOneToManyOptions<C, I, O, S> indexedBy(String columnName) {
			oneToManyRelation.setIndexingColumnName(columnName);
			return null;
		}
		
		@Override
		public FluentMappingBuilderOneToManyOptions<C, I, O, S> indexed() {
			oneToManyRelation.ordered();
			return null;
		}
	}
	
	/**
	 * A small class for one-to-many options storage into a {@link OneToManyRelation}. Acts as a wrapper over it.
	 */
	static class ManyToManyOptionsSupport<C, I, O, S1 extends Collection<O>, S2 extends Collection<C>>
			implements ManyToManyOptions<C, I, O, S1, S2> {
		
		private final ManyToManyRelation<C, O, I, S1, S2> manyToManyRelation;
		
		public ManyToManyOptionsSupport(ManyToManyRelation<C, O, I, S1, S2> manyToManyRelation) {
			this.manyToManyRelation = manyToManyRelation;
		}
		
		@Override
		public FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> initializeWith(Supplier<S1> collectionFactory) {
			manyToManyRelation.setCollectionFactory(collectionFactory);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> reverselySetBy(SerializableBiConsumer<O, C> reverseLink) {
			manyToManyRelation.getMappedByConfiguration().setReverseCombiner(reverseLink);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> reverseCollection(SerializableFunction<O, S2> collectionAccessor) {
			manyToManyRelation.getMappedByConfiguration().setReverseCollectionAccessor(collectionAccessor);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> reverseCollection(SerializableBiConsumer<O, S2> collectionMutator) {
			manyToManyRelation.getMappedByConfiguration().setReverseCollectionMutator(collectionMutator);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> reverselyInitializeWith(Supplier<S2> collectionFactory) {
			manyToManyRelation.getMappedByConfiguration().setReverseCollectionFactory(collectionFactory);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> cascading(RelationMode relationMode) {
			manyToManyRelation.setRelationMode(relationMode);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> fetchSeparately() {
			manyToManyRelation.fetchSeparately();
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> indexedBy(String columnName) {
			manyToManyRelation.setIndexingColumnName(columnName);
			return null;
		}
		
		@Override
		public FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> indexed() {
			manyToManyRelation.ordered();
			return null;
		}
	}
	
	/**
	 * Stores information of {@link InheritanceConfiguration}
	 * 
	 * @param <E> entity type
	 * @param <I> identifier type
	 */
	static class InheritanceConfigurationSupport<E, I> implements InheritanceConfiguration<E, I> {
		
		private final EntityMappingConfiguration<E, I> configuration;
		
		private boolean joinTable = false;
		
		private Table table;
		
		InheritanceConfigurationSupport(EntityMappingConfiguration<E, I> configuration) {
			this.configuration = configuration;
		}
		
		@Override
		public EntityMappingConfiguration<E, I> getConfiguration() {
			return configuration;
		}
		
		@Override
		public boolean isJoinTable() {
			return this.joinTable;
		}
		
		@Override
		public Table getTable() {
			return this.table;
		}
	}
	
	/**
	 * Storage for single key mapping definition. See {@link #mapKey(SerializableFunction, IdentifierPolicy)} methods.
	 */
	protected static class SingleKeyLinkageSupport<C, I> implements KeyMapping<C, I>, SingleKeyMapping<C, I> {
		
		private final ReversibleAccessor<C, I> accessor;
		
		private final IdentifierPolicy<I> identifierPolicy;
		
		@javax.annotation.Nullable
		private ColumnLinkageOptions columnOptions;
		
		private boolean setByConstructor;
		
		public SingleKeyLinkageSupport(ReversibleAccessor<C, I> accessor, IdentifierPolicy<I> identifierPolicy) {
			this.accessor = accessor;
			this.identifierPolicy = identifierPolicy;
		}
		
		@Override
		public IdentifierPolicy<I> getIdentifierPolicy() {
			return identifierPolicy;
		}
		
		@Override
		public ReversibleAccessor<C, I> getAccessor() {
			return accessor;
		}
		
		@javax.annotation.Nullable
		public ColumnLinkageOptions getColumnOptions() {
			return columnOptions;
		}
		
		public void setColumnOptions(ColumnLinkageOptions columnOptions) {
			this.columnOptions = columnOptions;
		}
		
		public void setByConstructor() {
			this.setByConstructor = true;
		}
		
		@Override
		public boolean isSetByConstructor() {
			return setByConstructor;
		}
	}
	
	/**
	 * Storage for composite key mapping definition. See {@link FluentEntityMappingBuilder#mapCompositeKey(SerializableFunction, CompositeKeyMappingConfigurationProvider, Consumer, Function)} methods.
	 */
	protected static class CompositeKeyLinkageSupport<C, I> implements KeyMapping<C, I>, CompositeKeyMapping<C, I> {
		
		private final ReversibleAccessor<C, I> accessor;
		private final CompositeKeyMappingConfigurationProvider<I> compositeKeyMappingBuilder;
		private final Consumer<C> markAsPersistedFunction;
		private final Function<C, Boolean> isPersistedFunction;
		
		@javax.annotation.Nullable
		private CompositeKeyLinkageOptions columnOptions;
		
		private boolean setByConstructor;
		
		public CompositeKeyLinkageSupport(ReversibleAccessor<C, I> accessor,
										  CompositeKeyMappingConfigurationProvider<I> compositeKeyMappingBuilder,
										  Consumer<C> markAsPersistedFunction,
										  Function<C, Boolean> isPersistedFunction) {
			this.accessor = accessor;
			this.compositeKeyMappingBuilder = compositeKeyMappingBuilder;
			this.markAsPersistedFunction = markAsPersistedFunction;
			this.isPersistedFunction = isPersistedFunction;
		}
		
		public CompositeKeyMappingConfigurationProvider<I> getCompositeKeyMappingBuilder() {
			return compositeKeyMappingBuilder;
		}
		
		@Override
		public ReversibleAccessor<C, I> getAccessor() {
			return accessor;
		}
		
		public void setColumnOptions(CompositeKeyLinkageOptions columnOptions) {
			this.columnOptions = columnOptions;
		}
		
		public void setByConstructor() {
			this.setByConstructor = true;
		}
		
		@Override
		public boolean isSetByConstructor() {
			return setByConstructor;
		}
		
		public List<CompositeKeyLinkage> getPropertiesMapping() {
			return compositeKeyMappingBuilder.getConfiguration().getPropertiesMapping();
		}
		
		@Override
		public Consumer<C> getMarkAsPersistedFunction() {
			return markAsPersistedFunction;
		}
		
		@Override
		public Function<C, Boolean> getIsPersistedFunction() {
			return isPersistedFunction;
		}
		
		@javax.annotation.Nullable
		@Override
		public CompositeKeyLinkageOptions getColumnsOptions() {
			return columnOptions;
		}
	}
	
	static class EntityFactoryProviderSupport<C, T extends Table> implements EntityFactoryProvider<C, T> {
		
		private final Function<Table, Function<ColumnedRow, C>> factory;
		
		private final boolean setIdentifier;
		
		EntityFactoryProviderSupport(Function<Table, Function<ColumnedRow, C>> factory, boolean setIdentifier) {
			this.factory = factory;
			this.setIdentifier = setIdentifier;
		}
		
		@Override
		public Function<ColumnedRow, C> giveEntityFactory(T table) {
			return factory.apply(table);
		}
		
		@Override
		public boolean isIdentifierSetByFactory() {
			return setIdentifier;
		}
	}
	
}
