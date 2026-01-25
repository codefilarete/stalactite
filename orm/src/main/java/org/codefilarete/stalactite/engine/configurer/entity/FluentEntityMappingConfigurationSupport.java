package org.codefilarete.stalactite.engine.configurer.entity;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.MutatorByMethod;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.InheritanceOptions;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilder.FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions;
import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilder.FluentEmbeddableMappingBuilderEnumOptions;
import org.codefilarete.stalactite.dsl.embeddable.ImportedEmbedWithColumnOptions;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.entity.FluentMappingBuilderManyToManyOptions;
import org.codefilarete.stalactite.dsl.entity.FluentMappingBuilderManyToOneOptions;
import org.codefilarete.stalactite.dsl.entity.FluentMappingBuilderOneToManyMappedByOptions;
import org.codefilarete.stalactite.dsl.entity.FluentMappingBuilderOneToManyOptions;
import org.codefilarete.stalactite.dsl.entity.FluentMappingBuilderOneToOneOptions;
import org.codefilarete.stalactite.dsl.entity.OptimisticLockOption;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.dsl.key.CompositeKeyMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.key.FluentEntityMappingBuilderCompositeKeyOptions;
import org.codefilarete.stalactite.dsl.key.FluentEntityMappingBuilderKeyOptions;
import org.codefilarete.stalactite.dsl.naming.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.MapEntryTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.UniqueConstraintNamingStrategy;
import org.codefilarete.stalactite.dsl.property.ElementCollectionOptions;
import org.codefilarete.stalactite.dsl.property.EmbeddableCollectionOptions;
import org.codefilarete.stalactite.dsl.property.EnumOptions;
import org.codefilarete.stalactite.dsl.property.MapOptions;
import org.codefilarete.stalactite.dsl.property.MapOptions.EmbeddableInMapOptions;
import org.codefilarete.stalactite.dsl.property.MapOptions.EntityInMapOptions;
import org.codefilarete.stalactite.dsl.relation.ManyToManyOptions;
import org.codefilarete.stalactite.dsl.relation.ManyToOneOptions;
import org.codefilarete.stalactite.dsl.relation.OneToManyEntityOptions;
import org.codefilarete.stalactite.dsl.relation.OneToOneEntityOptions;
import org.codefilarete.stalactite.dsl.relation.OneToOneOptions;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.configurer.builder.DefaultPersisterBuilder;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.embeddable.LinkageSupport;
import org.codefilarete.stalactite.engine.configurer.manyToOne.ManyToOneRelation;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.map.MapRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelation;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.function.Serie;
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
	private Table<?> targetTable;
	
	private TableNamingStrategy tableNamingStrategy = TableNamingStrategy.DEFAULT;
	
	private KeyMapping<C, I> keyMapping;
	
	private final MethodReferenceCapturer methodSpy;
	
	private final List<OneToOneRelation<C, ?, ?>> oneToOneRelations = new ArrayList<>();
	
	private final List<OneToManyRelation<C, ?, ?, ? extends Collection>> oneToManyRelations = new ArrayList<>();
	
	private final List<ManyToManyRelation<C, ?, ?, ? extends Collection, ? extends Collection>> manyToManyRelations = new ArrayList<>();
	
	private final List<ManyToOneRelation<C, ?, ?, Collection<C>>> manyToOneRelations = new ArrayList<>();
	
	private final List<ElementCollectionRelation<C, ?, ? extends Collection>> elementCollections = new ArrayList<>();
	
	private final List<MapRelation<C, ?, ?, ? extends Map>> maps = new ArrayList<>();
	
	private final EntityDecoratedEmbeddableConfigurationSupport<C, I> propertiesMappingConfigurationDelegate;
	
	private ForeignKeyNamingStrategy foreignKeyNamingStrategy = ForeignKeyNamingStrategy.DEFAULT;
	
	private UniqueConstraintNamingStrategy uniqueConstraintNamingStrategy;
	
	private JoinColumnNamingStrategy joinColumnNamingStrategy = JoinColumnNamingStrategy.JOIN_DEFAULT;
	
	private ColumnNamingStrategy indexColumnNamingStrategy;
	
	private AssociationTableNamingStrategy associationTableNamingStrategy = AssociationTableNamingStrategy.DEFAULT;
	
	private ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy = ElementCollectionTableNamingStrategy.DEFAULT;
	
	private MapEntryTableNamingStrategy mapEntryTableNamingStrategy = MapEntryTableNamingStrategy.DEFAULT;
	
	@Nullable
	private OptimisticLockOption<C, ?> optimisticLockOption;
	
	private InheritanceConfigurationSupport<? super C, I> inheritanceConfiguration;
	
	private PolymorphismPolicy<C> polymorphismPolicy;
	
	private EntityFactoryProviderSupport<C, Table> entityFactoryProvider;
	
	/**
	 * Creates a builder to map the given class for persistence
	 *
	 * @param classToPersist the class to create a mapping for
	 */
	public FluentEntityMappingConfigurationSupport(Class<C> classToPersist) {
		this.classToPersist = classToPersist;
		
		// Helper to capture Method behind method reference
		this.methodSpy = new MethodReferenceCapturer();
		
		this.propertiesMappingConfigurationDelegate = new EntityDecoratedEmbeddableConfigurationSupport<>(this, classToPersist);
	}
	
	public void setKeyMapping(KeyMapping<C, I> keyMapping) {
		this.keyMapping = keyMapping;
	}
	
	public void setEntityFactoryProvider(EntityFactoryProviderSupport<C, Table> entityFactoryProvider) {
		this.entityFactoryProvider = entityFactoryProvider;
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
	
	private Method captureLambdaMethod(SerializableFunction getter) {
		return this.methodSpy.findMethod(getter);
	}
	
	private Method captureLambdaMethod(SerializableBiConsumer setter) {
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
	
	@Nullable
	@Override
	public OptimisticLockOption<C, ?> getOptimisticLockOption() {
		return this.optimisticLockOption;
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
	public <TRGT, TRGTID> List<ManyToManyRelation<C, TRGT, TRGTID, Collection<TRGT>, Collection<C>>> getManyToManys() {
		return (List) manyToManyRelations;
	}
	
	@Override
	public <TRGT, TRGTID> List<ManyToOneRelation<C, TRGT, TRGTID, Collection<C>>> getManyToOnes() {
		return (List) manyToOneRelations;
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
	
	@javax.annotation.Nullable
	@Override
	public UniqueConstraintNamingStrategy getUniqueConstraintNamingStrategy() {
		return uniqueConstraintNamingStrategy;
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
	public <O> FluentMappingBuilderPropertyOptions<C, I, O> map(SerializableFunction<C, O> getter) {
		LinkageSupport<C, O> mapping = propertiesMappingConfigurationDelegate.addMapping(getter);
		return this.propertiesMappingConfigurationDelegate.wrapWithAdditionalPropertyOptions(mapping);
	}
	
	@Override
	public <O> FluentMappingBuilderPropertyOptions<C, I, O> map(SerializableBiConsumer<C, O> setter) {
		LinkageSupport<C, O> mapping = propertiesMappingConfigurationDelegate.addMapping(setter);
		return this.propertiesMappingConfigurationDelegate.wrapWithAdditionalPropertyOptions(mapping);
	}
	
	@Override
	public <O> FluentMappingBuilderPropertyOptions<C, I, O> map(String fieldName) {
		LinkageSupport<C, O> mapping = propertiesMappingConfigurationDelegate.addMapping(fieldName);
		return this.propertiesMappingConfigurationDelegate.wrapWithAdditionalPropertyOptions(mapping);
	}
	
	@Override
	public <E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I, E> mapEnum(SerializableFunction<C, E> getter) {
		LinkageSupport<C, E> linkage = propertiesMappingConfigurationDelegate.addMapping(getter);
		return wrapEnumOptions(propertiesMappingConfigurationDelegate.wrapWithEnumOptions(linkage));
	}
	
	@Override
	public <E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I, E> mapEnum(SerializableBiConsumer<C, E> setter) {
		LinkageSupport<C, E> linkage = propertiesMappingConfigurationDelegate.addMapping(setter);
		return wrapEnumOptions(propertiesMappingConfigurationDelegate.wrapWithEnumOptions(linkage));
	}
	
	@Override
	public <E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I, E> mapEnum(String fieldName) {
		LinkageSupport<C, E> linkage = propertiesMappingConfigurationDelegate.addMapping(fieldName);
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
				.redirect(MapOptions.class, new MapOptions<K, V, M>() {
					
					@Override
					public MapOptions<K, V, M> reverseJoinColumn(String columnName) {
						mapRelation.setReverseColumnName(columnName);
						return null;
					}
					
					@Override
					public MapOptions<K, V, M> keyColumn(String columnName) {
						mapRelation.setKeyColumnName(columnName);
						return null;
					}
					
					@Override
					public MapOptions<K, V, M> keySize(Size columnSize) {
						mapRelation.setKeyColumnSize(columnSize);
						return null;
					}
					
					@Override
					public MapOptions<K, V, M> valueColumn(String columnName) {
						mapRelation.setValueColumnName(columnName);
						return null;
					}
					
					@Override
					public MapOptions<K, V, M> valueSize(Size columnSize) {
						mapRelation.setValueColumnSize(columnSize);
						return null;
					}
					
					@Override
					public MapOptions<K, V, M> initializeWith(Supplier<? extends M> mapFactory) {
						mapRelation.setMapFactory(mapFactory);
						return null;
					}
					
					@Override
					public MapOptions<K, V, M> onTable(String tableName) {
						mapRelation.setTargetTableName(tableName);
						return null;
					}
					
					@Override
					public MapOptions<K, V, M> onTable(Table table) {
						mapRelation.setTargetTable(table);
						return null;
					}
					
					@Override
					public EntityInMapOptions<K, V, M> withKeyMapping(EntityMappingConfigurationProvider<K, ?> mappingConfigurationProvider) {
						// This method is not call because it is overwritten by a dedicated redirect(..) call below
						// mapRelation.setKeyConfigurationProvider(mappingConfigurationProvider);
						return null;
					}
					
					@Override
					public EmbeddableInMapOptions<K> withKeyMapping(EmbeddableMappingConfigurationProvider<K> mappingConfigurationProvider) {
						// This method is not call because it is overwritten by a dedicated redirect(..) call below
						// mapRelation.setKeyConfigurationProvider(mappingConfigurationProvider);
						return null;
					}
					
					@Override
					public EntityInMapOptions<K, V, M> withValueMapping(EntityMappingConfigurationProvider<V, ?> mappingConfigurationProvider) {
						// This method is not call because it is overwritten by a dedicated redirect(..) call below
						// mapRelation.setValueConfigurationProvider(mappingConfigurationProvider);
						return null;
					}
					
					@Override
					public EmbeddableInMapOptions<V> withValueMapping(EmbeddableMappingConfigurationProvider<V> mappingConfigurationProvider) {
						// This method is not call because it is overwritten by a dedicated redirect(..) call below
						// mapRelation.setValueConfigurationProvider(mappingConfigurationProvider);
						return null;
					}
					
					@Override
					public MapOptions<K, V, M> fetchSeparately() {
						mapRelation.fetchSeparately();
						return null;
					}
				}, true)
				// This will overwrite withKeyMapping(EntityMappingConfigurationProvider) capture to return a proxy
				// that will let us configure cascading of the relation
				.redirect((SerializableBiFunction<MapOptions<K, V, M>, EntityMappingConfigurationProvider<K, ?>, EntityInMapOptions<K, V, M>>) MapOptions::withKeyMapping,
						entityMappingConfigurationProvider -> {
							mapRelation.setKeyConfigurationProvider(entityMappingConfigurationProvider);
							return new MethodReferenceDispatcher()
									.redirect(EntityInMapOptions.class, relationMode -> {
										mapRelation.setKeyEntityRelationMode(relationMode);
										return null;
									}, true)
									.fallbackOn(proxyHolder)
									.build((Class<FluentMappingBuilderEntityInMapOptions<C, I, K, V, M>>) (Class) FluentMappingBuilderEntityInMapOptions.class);
						})
				.redirect((SerializableBiFunction<MapOptions<K, V, M>, EmbeddableMappingConfigurationProvider<K>, EmbeddableInMapOptions<K>>) MapOptions::withKeyMapping,
						entityMappingConfigurationProvider -> {
							mapRelation.setKeyConfigurationProvider(entityMappingConfigurationProvider);
							return new MethodReferenceDispatcher()
									.redirect(EmbeddableInMapOptions.class, new EmbeddableInMapOptions<K>() {
										
										@Override
										public EmbeddableInMapOptions<K> overrideName(SerializableFunction<K, ?> getter, String columnName) {
											mapRelation.overrideKeyName(getter, columnName);
											return null;
										}
										
										@Override
										public EmbeddableInMapOptions<K> overrideName(SerializableBiConsumer<K, ?> setter, String columnName) {
											mapRelation.overrideKeyName(setter, columnName);
											return null;
										}
										
										@Override
										public EmbeddableInMapOptions<K> overrideSize(SerializableFunction<K, ?> getter, Size columnSize) {
											mapRelation.overrideKeySize(getter, columnSize);
											return null;
										}
										
										@Override
										public EmbeddableInMapOptions<K> overrideSize(SerializableBiConsumer<K, ?> setter, Size columnSize) {
											mapRelation.overrideKeySize(setter, columnSize);
											return null;
										}
									}, true)
									.fallbackOn(proxyHolder)
									.build((Class<FluentMappingBuilderEmbeddableInMapOptions<C, I, K, V, M, K>>) (Class) FluentMappingBuilderEmbeddableInMapOptions.class);
						})
				.redirect((SerializableBiFunction<MapOptions<K, V, M>, EntityMappingConfigurationProvider<V, ?>, EntityInMapOptions<K, V, M>>) MapOptions::withValueMapping,
						entityMappingConfigurationProvider -> {
							mapRelation.setValueConfigurationProvider(entityMappingConfigurationProvider);
							return new MethodReferenceDispatcher()
									.redirect(EntityInMapOptions.class, relationMode -> {
										mapRelation.setValueEntityRelationMode(relationMode);
										return null;
									}, true)
									.fallbackOn(proxyHolder)
									.build((Class<FluentMappingBuilderEntityInMapOptions<C, I, K, V, M>>) (Class) FluentMappingBuilderEntityInMapOptions.class);
						})
				.redirect((SerializableBiFunction<MapOptions<K, V, M>, EmbeddableMappingConfigurationProvider<V>, EmbeddableInMapOptions<V>>) MapOptions::withValueMapping,
						entityMappingConfigurationProvider -> {
							mapRelation.setValueConfigurationProvider(entityMappingConfigurationProvider);
							return new MethodReferenceDispatcher()
									.redirect(EmbeddableInMapOptions.class, new EmbeddableInMapOptions<V>() {
										
										@Override
										public EmbeddableInMapOptions<V> overrideName(SerializableFunction<V, ?> getter, String columnName) {
											mapRelation.overrideValueName(getter, columnName);
											return null;
										}
										
										@Override
										public EmbeddableInMapOptions<V> overrideName(SerializableBiConsumer<V, ?> setter, String columnName) {
											mapRelation.overrideValueName(setter, columnName);
											return null;
										}
										
										@Override
										public EmbeddableInMapOptions<V> overrideSize(SerializableFunction<V, ?> getter, Size columnSize) {
											mapRelation.overrideValueSize(getter, columnSize);
											return null;
										}
										
										@Override
										public EmbeddableInMapOptions<V> overrideSize(SerializableBiConsumer<V, ?> setter, Size columnSize) {
											mapRelation.overrideValueSize(setter, columnSize);
											return null;
										}
									}, true)
									.fallbackOn(proxyHolder)
									.build((Class<FluentMappingBuilderEmbeddableInMapOptions<C, I, K, V, M, V>>) (Class) FluentMappingBuilderEmbeddableInMapOptions.class);
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
				.redirect(EmbeddableCollectionOptions.class, new EmbeddableCollectionOptions<C, O, S>() {
					
					@Override
					public <IN> EmbeddableCollectionOptions<C, O, S> overrideName(SerializableFunction<O, IN> getter, String columnName) {
						elementCollectionRelation.overrideName(getter, columnName);
						return null;
					}
					
					@Override
					public <IN> EmbeddableCollectionOptions<C, O, S> overrideName(SerializableBiConsumer<O, IN> setter, String columnName) {
						elementCollectionRelation.overrideName(setter, columnName);
						return null;
					}
					
					@Override
					public <IN> EmbeddableCollectionOptions<C, O, S> overrideSize(SerializableFunction<O, IN> getter, Size columnSize) {
						elementCollectionRelation.overrideSize(getter, columnSize);
						return null;
					}
					
					@Override
					public <IN> EmbeddableCollectionOptions<C, O, S> overrideSize(SerializableBiConsumer<O, IN> setter, Size columnSize) {
						elementCollectionRelation.overrideSize(setter, columnSize);
						return null;
					}
					
					@Override
					public EmbeddableCollectionOptions<C, O, S> initializeWith(Supplier<? extends S> collectionFactory) {
						elementCollectionRelation.setCollectionFactory(collectionFactory);
						return null;
					}
					
					@Override
					public EmbeddableCollectionOptions<C, O, S> reverseJoinColumn(String name) {
						elementCollectionRelation.setReverseColumnName(name);
						return null;
					}
					
					@Override
					public EmbeddableCollectionOptions<C, O, S> indexed() {
						elementCollectionRelation.ordered();
						return null;
					}
					
					@Override
					public EmbeddableCollectionOptions<C, O, S> indexedBy(String columnName) {
						elementCollectionRelation.setIndexingColumnName(columnName);
						return null;
					}
					
					@Override
					public EmbeddableCollectionOptions<C, O, S> onTable(Table table) {
						elementCollectionRelation.setTargetTable(table);
						return null;
					}
					
					@Override
					public EmbeddableCollectionOptions<C, O, S> onTable(String tableName) {
						elementCollectionRelation.setTargetTableName(tableName);
						return null;
					}
				}, true)
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S>>) (Class) FluentMappingBuilderElementCollectionImportEmbedOptions.class);
	}
	
	private <O, S extends Collection<O>> ElementCollectionOptions<C, O, S> wrapAsOptions(ElementCollectionRelation<C, O, S> elementCollectionRelation) {
		return new ElementCollectionOptions<C, O, S>() {
			
			@Override
			public ElementCollectionOptions<C, O, S> initializeWith(Supplier<? extends S> collectionFactory) {
				elementCollectionRelation.setCollectionFactory(collectionFactory);
				return null;
			}
			
			@Override
			public ElementCollectionOptions<C, O, S> elementColumnName(String columnName) {
				elementCollectionRelation.setElementColumnName(columnName);
				return null;
			}
			
			@Override
			public ElementCollectionOptions<C, O, S> elementColumnSize(Size columnSize) {
				elementCollectionRelation.setElementColumnSize(columnSize);
				return null;
			}
			
			@Override
			public FluentMappingBuilderElementCollectionOptions<C, I, O, S> reverseJoinColumn(String name) {
				elementCollectionRelation.setReverseColumnName(name);
				return null;
			}
			
			@Override
			public FluentMappingBuilderElementCollectionOptions<C, I, O, S> indexed() {
				elementCollectionRelation.ordered();
				return null;
			}
			
			@Override
			public FluentMappingBuilderElementCollectionOptions<C, I, O, S> indexedBy(String columnName) {
				elementCollectionRelation.setIndexingColumnName(columnName);
				return null;
			}
			
			@Override
			public ElementCollectionOptions<C, O, S> onTable(Table table) {
				elementCollectionRelation.setTargetTable(table);
				return null;
			}
			
			@Override
			public ElementCollectionOptions<C, O, S> onTable(String tableName) {
				elementCollectionRelation.setTargetTableName(tableName);
				return null;
			}
			
		};
	}
	
	@Override
	public FluentMappingBuilderInheritanceOptions<C, I> mapSuperClass(EntityMappingConfigurationProvider<? super C, I> mappingConfiguration) {
		inheritanceConfiguration = new InheritanceConfigurationSupport<>(mappingConfiguration.getConfiguration());
		return new MethodReferenceDispatcher()
				.redirect(InheritanceOptions.class, new InheritanceOptions() {
					@Override
					public InheritanceOptions withJoinedTable() {
						inheritanceConfiguration.setJoinTable(true);
						return null;
					}

					@Override
					public InheritanceOptions withJoinedTable(Table parentTable) {
						inheritanceConfiguration.setJoinTable(true);
						inheritanceConfiguration.setTable(parentTable);
						return null;
					}
				}, true)
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderInheritanceOptions<C, I>>) (Class) FluentMappingBuilderInheritanceOptions.class);
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> mapSuperClass(EmbeddableMappingConfigurationProvider<? super C> superMappingConfiguration) {
		this.propertiesMappingConfigurationDelegate.mapSuperClass(superMappingConfiguration);
		return this;
	}
	
	@Override
	public <O, J> FluentMappingBuilderOneToOneOptions<C, I, O> mapOneToOne(
			SerializableBiConsumer<C, O> setter,
			EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration) {
		// we keep close to user demand: we keep its method reference ...
		Mutator<C, O> mutatorByMethodReference = Accessors.mutatorByMethodReference(setter);
		// ... but we can't do it for accessor, so we use the most equivalent manner: an accessor based on setter method (fallback to property if not present)
		Accessor<C, O> accessor = new MutatorByMethod<C, O>(captureLambdaMethod(setter)).toAccessor();
		return mapOneToOne(accessor, mutatorByMethodReference, mappingConfiguration);
	}
	
	@Override
	public <O, J> FluentMappingBuilderOneToOneOptions<C, I, O> mapOneToOne(
			SerializableFunction<C, O> getter,
			EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration) {
		// we keep close to user demand: we keep its method reference ...
		AccessorByMethodReference<C, O> accessorByMethodReference = Accessors.accessorByMethodReference(getter);
		// ... but we can't do it for mutator, so we use the most equivalent manner: a mutator based on getter method (fallback to property if not present)
		Mutator<C, O> mutator = new AccessorByMethod<C, O>(captureLambdaMethod(getter)).toMutator();
		return mapOneToOne(accessorByMethodReference, mutator, mappingConfiguration);
	}
	
	private <O, J> FluentMappingBuilderOneToOneOptions<C, I, O> mapOneToOne(
			Accessor<C, O> accessor,
			Mutator<C, O> mutator,
			EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration) {
		OneToOneRelation<C, O, J> oneToOneRelation = new OneToOneRelation<>(
				new PropertyAccessor<>(accessor, mutator),
				() -> this.polymorphismPolicy instanceof PolymorphismPolicy.TablePerClassPolymorphism,
				mappingConfiguration);
		this.oneToOneRelations.add((OneToOneRelation<C, Object, Object>) oneToOneRelation);
		return wrapForAdditionalOptions(oneToOneRelation);
	}
	
	private <O, J> FluentMappingBuilderOneToOneOptions<C, I, O> wrapForAdditionalOptions(OneToOneRelation<C, O, J> oneToOneRelation) {
		// then we return an object that allows fluent settings over our OneToOne cascade instance
		return new MethodDispatcher()
				.redirect(OneToOneEntityOptions.class, new OneToOneEntityOptions<C, J, O>() {
					@Override
					public OneToOneOptions<C, O> cascading(RelationMode relationMode) {
						oneToOneRelation.setRelationMode(relationMode);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneEntityOptions<C, J, O> mandatory() {
						oneToOneRelation.setNullable(false);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneEntityOptions<C, J, O> mappedBy(SerializableFunction<? super O, C> reverseLink) {
						oneToOneRelation.setReverseGetter(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneEntityOptions<C, J, O> mappedBy(SerializableBiConsumer<? super O, C> reverseLink) {
						oneToOneRelation.setReverseSetter(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneEntityOptions<C, J, O> reverseJoinColumn(Column<?, J> reverseLink) {
						oneToOneRelation.setReverseColumn(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneEntityOptions<C, J, O> reverseJoinColumn(String reverseColumnName) {
						oneToOneRelation.setReverseColumn(reverseColumnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneEntityOptions<C, J, O> fetchSeparately() {
						oneToOneRelation.fetchSeparately();
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions<C, O> columnName(String columnName) {
						oneToOneRelation.setColumnName(columnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneEntityOptions<C, J, O> unique() {
						oneToOneRelation.setUnique(true);
						return null;	// we can return null because dispatcher will return proxy
					}
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderOneToOneOptions<C, I, O>>) (Class) FluentMappingBuilderOneToOneOptions.class);
	}
	
	@Override
	public <O, J, S extends Collection<O>> FluentMappingBuilderOneToManyOptions<C, I, O, S> mapOneToMany(
			SerializableFunction<C, S> getter,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration) {
		
		AccessorByMethodReference<C, S> getterReference = Accessors.accessorByMethodReference(getter);
		ReversibleAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				getterReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<C, S>(captureLambdaMethod(getter)).toMutator());
		return mapOneToMany(propertyAccessor, mappingConfiguration);
	}
	
	@Override
	public <O, J, S extends Collection<O>> FluentMappingBuilderOneToManyOptions<C, I, O, S> mapOneToMany(
			SerializableBiConsumer<C, S> setter,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration) {
		
		MutatorByMethodReference<C, S> setterReference = Accessors.mutatorByMethodReference(setter);
		PropertyAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				Accessors.accessor(setterReference.getDeclaringClass(), propertyName(setterReference.getMethodName())),
				setterReference
		);
		return mapOneToMany(propertyAccessor, mappingConfiguration);
	}
	
	private <TRGT, TRGTID, S extends Collection<TRGT>> FluentMappingBuilderOneToManyOptions<C, I, TRGT, S> mapOneToMany(
			ReversibleAccessor<C, S> propertyAccessor,
			EntityMappingConfigurationProvider<? super TRGT, TRGTID> mappingConfiguration) {
		OneToManyRelation<C, TRGT, TRGTID, S> oneToManyRelation = new OneToManyRelation<>(
				propertyAccessor,
				() -> this.polymorphismPolicy instanceof PolymorphismPolicy.TablePerClassPolymorphism,
				mappingConfiguration);
		this.oneToManyRelations.add(oneToManyRelation);
		OneToManyOptionsSupport<C, I, TRGT, S, TRGTID> optionsSupport = new OneToManyOptionsSupport<>(oneToManyRelation);
		// Code below is a bit complicated due to mandatory() method after mappedBy(..) ones: we must provide a support for mandatory() after them
		// which also allow to call default oneToMany options that are available on the main proxy. Therefore we have to use Holder objects
		// to built the complex global proxy
		Holder<FluentMappingBuilderOneToManyOptions<C, I, TRGT, S>> result = new Holder<>();
		FluentMappingBuilderOneToManyMappedByOptions<C, I, TRGT, S> reverseAsMandatorySupport = new MethodReferenceDispatcher()
				.redirect(
						(SerializableFunction<FluentMappingBuilderOneToManyMappedByOptions<C, I, TRGT, S>, FluentMappingBuilderOneToManyMappedByOptions<C, I, TRGT, S>>) FluentMappingBuilderOneToManyMappedByOptions::mandatory,
						() -> oneToManyRelation.setReverseAsMandatory(true))
				.fallbackOn(result)	// for all other methods, methods are called on the main proxy
				.build((Class<FluentMappingBuilderOneToManyMappedByOptions<C, I, TRGT, S>>) (Class) FluentMappingBuilderOneToManyMappedByOptions.class);
		FluentMappingBuilderOneToManyOptions<C, I, TRGT, S> build = new MethodReferenceDispatcher()
				.redirect(OneToManyEntityOptions.class, optionsSupport, true)
				.redirect(
						(SerializableBiFunction<FluentMappingBuilderOneToManyOptions<C, I, TRGT, S>, SerializableBiConsumer<TRGT, ? super C>, FluentMappingBuilderOneToManyMappedByOptions<C, I, TRGT, S>>) FluentMappingBuilderOneToManyOptions::mappedBy,
						(consumer) -> {
							optionsSupport.mappedBy(consumer);
							return reverseAsMandatorySupport;	// to let user call mandatory() special option
						})
				.redirect(
						(SerializableBiFunction<FluentMappingBuilderOneToManyOptions<C, I, TRGT, S>, SerializableFunction<TRGT, ? super C>, FluentMappingBuilderOneToManyMappedByOptions<C, I, TRGT, S>>) FluentMappingBuilderOneToManyOptions::mappedBy,
						(consumer) -> {
							optionsSupport.mappedBy(consumer);
							return reverseAsMandatorySupport;	// to let user call mandatory() special option
						})
				.redirect(
						(SerializableBiFunction<FluentMappingBuilderOneToManyOptions<C, I, TRGT, S>, Column<?, I>, FluentMappingBuilderOneToManyMappedByOptions<C, I, TRGT, S>>) FluentMappingBuilderOneToManyOptions::mappedBy,
						(consumer) -> {
							optionsSupport.mappedBy(consumer);
							return reverseAsMandatorySupport;	// to let user call mandatory() special option
						})
				.redirect(
						(SerializableBiFunction<FluentMappingBuilderOneToManyOptions<C, I, TRGT, S>, String, FluentMappingBuilderOneToManyMappedByOptions<C, I, TRGT, S>>) FluentMappingBuilderOneToManyOptions::reverseJoinColumn,
						(consumer) -> {
							optionsSupport.reverseJoinColumn(consumer);
							return reverseAsMandatorySupport;	// to let user call mandatory() special option
						})
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderOneToManyOptions<C, I, TRGT, S>>) (Class) FluentMappingBuilderOneToManyOptions.class);
		result.set(build);
		return build;
	}
	
	@Override
	public <O, J, S1 extends Collection<O>, S2 extends Collection<C>>
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2>
	mapManyToMany(SerializableFunction<C, S1> getter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration) {
		AccessorByMethodReference<C, S1> getterReference = Accessors.accessorByMethodReference(getter);
		ReversibleAccessor<C, S1> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				getterReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<C, S1>(captureLambdaMethod(getter)).toMutator());
		return mapManyToMany(propertyAccessor, mappingConfiguration);
	}
	
	@Override
	public <O, J, S1 extends Collection<O>, S2 extends Collection<C>>
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2>
	mapManyToMany(SerializableBiConsumer<C, S1> setter,
				  EntityMappingConfigurationProvider<? super O, J> mappingConfiguration) {
		MutatorByMethodReference<C, S1> setterReference = Accessors.mutatorByMethodReference(setter);
		PropertyAccessor<C, S1> propertyAccessor = new PropertyAccessor<>(
				Accessors.accessor(setterReference.getDeclaringClass(), propertyName(setterReference.getMethodName())),
				setterReference
		);
		return mapManyToMany(propertyAccessor, mappingConfiguration);
	}
	
	private <O, J, S1 extends Collection<O>, S2 extends Collection<C>, T extends Table> FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> mapManyToMany(
			ReversibleAccessor<C, S1> propertyAccessor,
			EntityMappingConfigurationProvider<? super O, J> mappingConfiguration) {
		ManyToManyRelation<C, O, J, S1, S2> manyToManyRelation = new ManyToManyRelation<>(
				propertyAccessor,
				() -> this.polymorphismPolicy instanceof PolymorphismPolicy.TablePerClassPolymorphism,
				mappingConfiguration);
		this.manyToManyRelations.add(manyToManyRelation);
		return new MethodDispatcher()
				.redirect(ManyToManyOptions.class, new ManyToManyOptionsSupport<>(manyToManyRelation), true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2>>) (Class) FluentMappingBuilderManyToManyOptions.class);
	}
	
	@Override
	public <O, J, S extends Collection<C>> FluentMappingBuilderManyToOneOptions<C, I, O, S> mapManyToOne(
			SerializableBiConsumer<C, O> setter,
			EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration) {
		// we keep close to user demand: we keep its method reference ...
		Mutator<C, O> mutatorByMethodReference = Accessors.mutatorByMethodReference(setter);
		// ... but we can't do it for accessor, so we use the most equivalent manner: an accessor based on setter method (fallback to property if not present)
		Accessor<C, O> accessor = new MutatorByMethod<C, O>(captureLambdaMethod(setter)).toAccessor();
		return mapManyToOne(accessor, mutatorByMethodReference, mappingConfiguration);
	}
	
	@Override
	public <O, J, S extends Collection<C>> FluentMappingBuilderManyToOneOptions<C, I, O, S> mapManyToOne(
			SerializableFunction<C, O> getter,
			EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration) {
		// we keep close to user demand: we keep its method reference ...
		AccessorByMethodReference<C, O> accessorByMethodReference = Accessors.accessorByMethodReference(getter);
		// ... but we can't do it for mutator, so we use the most equivalent manner: a mutator based on getter method (fallback to property if not present)
		Mutator<C, O> mutator = new AccessorByMethod<C, O>(captureLambdaMethod(getter)).toMutator();
		return mapManyToOne(accessorByMethodReference, mutator, mappingConfiguration);
	}
	
	private <O, J, S extends Collection<C>> FluentMappingBuilderManyToOneOptions<C, I, O, S> mapManyToOne(
			Accessor<C, O> accessor,
			Mutator<C, O> mutator,
			EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration) {
		ManyToOneRelation<C, O, J, S> manyToOneRelation = new ManyToOneRelation<>(
				new PropertyAccessor<>(accessor, mutator),
				() -> this.polymorphismPolicy instanceof PolymorphismPolicy.TablePerClassPolymorphism,
				mappingConfiguration);
		this.manyToOneRelations.add((ManyToOneRelation<C, Object, Object, Collection<C>>) manyToOneRelation);
		return wrapForAdditionalOptions(manyToOneRelation);
	}
	
	private <O, J, S extends Collection<C>> FluentMappingBuilderManyToOneOptions<C, I, O, S> wrapForAdditionalOptions(ManyToOneRelation<C, O, J, S> manyToOneRelation) {
		// then we return an object that allows fluent settings over our OneToOne cascade instance
		return new MethodDispatcher()
				.redirect(ManyToOneOptions.class, new ManyToOneOptions<C, O, S>() {
					@Override
					public ManyToOneOptions<C, O, S> cascading(RelationMode relationMode) {
						manyToOneRelation.setRelationMode(relationMode);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ManyToOneOptions<C, O, S> mandatory() {
						manyToOneRelation.setNullable(false);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ManyToOneOptions<C, O, S> reverselySetBy(SerializableBiConsumer<O, C> reverseLink) {
						manyToOneRelation.getMappedByConfiguration().setCombiner(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ManyToOneOptions<C, O, S> reverseCollection(SerializableFunction<O, S> collectionAccessor) {
						manyToOneRelation.getMappedByConfiguration().setAccessor(collectionAccessor);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ManyToOneOptions<C, O, S> reverseCollection(SerializableBiConsumer<O, S> collectionMutator) {
						manyToOneRelation.getMappedByConfiguration().setMutator(collectionMutator);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ManyToOneOptions<C, O, S> reverselyInitializeWith(Supplier<S> collectionFactory) {
						manyToOneRelation.getMappedByConfiguration().setFactory(collectionFactory);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ManyToOneOptions<C, O, S> fetchSeparately() {
						manyToOneRelation.fetchSeparately();
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ManyToOneOptions<C, O, S> columnName(String columnName) {
						manyToOneRelation.setColumnName(columnName);
						return null;
					}
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderManyToOneOptions<C, I, O, S>>) (Class) FluentMappingBuilderManyToOneOptions.class);
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
				.redirect(ImportedEmbedWithColumnOptions.class, new ImportedEmbedWithColumnOptions<O>() {
					@Override
					public <IN> ImportedEmbedWithColumnOptions<O> overrideName(SerializableBiConsumer<O, IN> setter, String columnName) {
						support.overrideName(setter, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public <IN> ImportedEmbedWithColumnOptions<O> overrideName(SerializableFunction<O, IN> getter, String columnName) {
						support.overrideName(getter, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public <IN> ImportedEmbedWithColumnOptions<O> overrideSize(SerializableBiConsumer<O, IN> setter, Size columnSize) {
						support.overrideSize(setter, columnSize);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public <IN> ImportedEmbedWithColumnOptions<O> overrideSize(SerializableFunction<O, IN> getter, Size columnSize) {
						support.overrideSize(getter, columnSize);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public <IN> ImportedEmbedWithColumnOptions<O> override(SerializableBiConsumer<O, IN> setter, Column<? extends Table, IN> targetColumn) {
						propertiesMappingConfigurationDelegate.currentInset().override(setter, targetColumn);
						return null;
					}
					
					@Override
					public <IN> ImportedEmbedWithColumnOptions<O> override(SerializableFunction<O, IN> getter, Column<? extends Table, IN> targetColumn) {
						propertiesMappingConfigurationDelegate.currentInset().override(getter, targetColumn);
						return null;
					}
					
					@Override
					public <IN> ImportedEmbedWithColumnOptions<O> exclude(SerializableBiConsumer<O, IN> setter) {
						support.exclude(setter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public <IN> ImportedEmbedWithColumnOptions<O> exclude(SerializableFunction<O, IN> getter) {
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
	public FluentEntityMappingBuilder<C, I> withUniqueConstraintNaming(UniqueConstraintNamingStrategy uniqueConstraintNamingStrategy) {
		this.uniqueConstraintNamingStrategy = uniqueConstraintNamingStrategy;
		return this;
	}

	@Override
	public FluentEntityMappingBuilder<C, I> withJoinColumnNaming(JoinColumnNamingStrategy columnNamingStrategy) {
		this.joinColumnNamingStrategy = columnNamingStrategy;
		return this;
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> withIndexColumnNaming(ColumnNamingStrategy columnNamingStrategy) {
		this.indexColumnNamingStrategy = columnNamingStrategy;
		return this;
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> withAssociationTableNaming(AssociationTableNamingStrategy associationTableNamingStrategy) {
		this.associationTableNamingStrategy = associationTableNamingStrategy;
		return this;
	}
	
	@Override
	public <V> FluentEntityMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter) {
		optimisticLockOption = new OptimisticLockOption<>(getter, null);
		return this;
	}
	
	@Override
	public <V> FluentEntityMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter, Serie<V> serie) {
		optimisticLockOption = new OptimisticLockOption<>(getter, serie);
		return this;
	}
	
	@Override
	public <V> FluentEntityMappingBuilder<C, I> versionedBy(SerializableBiConsumer<C, V> setter) {
		optimisticLockOption = new OptimisticLockOption<>(setter, null);
		return this;
	}
	
	@Override
	public <V> FluentEntityMappingBuilder<C, I> versionedBy(SerializableBiConsumer<C, V> setter, Serie<V> serie) {
		optimisticLockOption = new OptimisticLockOption<>(setter, serie);
		return this;
	}
	
	@Override
	public <V> FluentEntityMappingBuilder<C, I> versionedBy(String fieldName) {
		return versionedBy(fieldName, null);
	}
	
	@Override
	public <V> FluentEntityMappingBuilder<C, I> versionedBy(String fieldName, Serie<V> serie) {
		optimisticLockOption = new OptimisticLockOption<>(classToPersist, fieldName, serie);
		return this;
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> onTable(String tableName) {
		return onTable(new Table<>(tableName));
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> onTable(Table table) {
		this.targetTable = table;
		return this;
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> mapPolymorphism(PolymorphismPolicy<C> polymorphismPolicy) {
		this.polymorphismPolicy = polymorphismPolicy;
		return this;
	}
	
	@Override
	public ConfiguredRelationalPersister<C, I> build(PersistenceContext persistenceContext) {
		DefaultPersisterBuilder persisterBuilder = new DefaultPersisterBuilder(persistenceContext);
		ConfiguredRelationalPersister<C, I> persister = persisterBuilder.build(this.getConfiguration());
		persistenceContext.getPersisterRegistry().addPersister(persister);
		return persister;
	}
}
