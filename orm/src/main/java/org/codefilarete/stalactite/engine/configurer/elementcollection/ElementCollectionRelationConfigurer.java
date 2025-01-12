package org.codefilarete.stalactite.engine.configurer.elementcollection;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorChain.ValueInitializerOnNullValue;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.cascade.AfterInsertCollectionCascader;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.AfterUpdateTrigger;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.DeleteTargetEntitiesBeforeDeleteCascader;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.EmbeddedClassMapping;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.PairIterator;

import static org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder.BeanMappingConfiguration.Linkage;
import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.bean.Objects.preventNull;

/**
 * Class that configures element-collection mapping
 * 
 * @author Guillaume Mary
 */
public class ElementCollectionRelationConfigurer<SRC, TRGT, ID, C extends Collection<TRGT>> {
	
	private static final AccessorDefinition ELEMENT_RECORD_ID_ACCESSOR_DEFINITION = AccessorDefinition.giveDefinition(new AccessorByMethodReference<>(ElementRecord<Object, Object>::getId));
	
	private final ElementCollectionRelation<SRC, TRGT, C> linkage;
	private final ConfiguredRelationalPersister<SRC, ID> sourcePersister;
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private final ColumnNamingStrategy columnNamingStrategy;
	private final ElementCollectionTableNamingStrategy tableNamingStrategy;
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public ElementCollectionRelationConfigurer(ElementCollectionRelation<SRC, TRGT, C> linkage,
											   ConfiguredRelationalPersister<SRC, ID> sourcePersister,
											   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
											   ColumnNamingStrategy columnNamingStrategy,
											   ElementCollectionTableNamingStrategy tableNamingStrategy,
											   Dialect dialect,
											   ConnectionConfiguration connectionConfiguration) {
		this.linkage = linkage;
		this.sourcePersister = sourcePersister;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		this.columnNamingStrategy = columnNamingStrategy;
		this.tableNamingStrategy = tableNamingStrategy;
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	public <T extends Table<T>, TARGETTABLE extends Table<TARGETTABLE>> SimpleRelationalEntityPersister<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>, TARGETTABLE>
	configure() {
		
		AccessorDefinition collectionProviderDefinition = AccessorDefinition.giveDefinition(linkage.getCollectionProvider());
		// schema configuration
		PrimaryKey<T, ID> sourcePK = sourcePersister.<T>getMapping().getTargetTable().getPrimaryKey();
		
		String tableName = nullable(linkage.getTargetTableName()).getOr(() -> tableNamingStrategy.giveName(collectionProviderDefinition));
		// Note that table will participate to DDL while cascading selection thanks to its join on foreignKey 
		TARGETTABLE targetTable = (TARGETTABLE) nullable(linkage.getTargetTable()).getOr(() -> new Table(tableName));
		Map<Column<T, ?>, Column<TARGETTABLE, ?>> primaryKeyForeignColumnMapping = new HashMap<>();
		Column<TARGETTABLE, ID> reverseColumn = (Column) linkage.getReverseColumn();
		Key<TARGETTABLE, ID> reverseKey = nullable(reverseColumn).map(Key::ofSingleColumn)
				.getOr(() -> {
					KeyBuilder<TARGETTABLE, ID> result = Key.from(targetTable);
					sourcePK.getColumns().forEach(col -> {
						String reverseColumnName = nullable(linkage.getReverseColumnName()).getOr(() ->
								columnNamingStrategy.giveName(ELEMENT_RECORD_ID_ACCESSOR_DEFINITION));
						Column<TARGETTABLE, ?> reverseCol = targetTable.addColumn(reverseColumnName, col.getJavaType())
								.primaryKey();
						primaryKeyForeignColumnMapping.put(col, reverseCol);
						result.addColumn(reverseCol);
					});
					return result.build();
				});
		ForeignKey<TARGETTABLE, T, ID> reverseForeignKey = targetTable.addForeignKey(this.foreignKeyNamingStrategy::giveName, reverseKey, sourcePK);
		registerColumnBinder(reverseForeignKey, sourcePK);	// because sourcePk binder might have been overloaded by column so we need to adjust to it
		
		EmbeddableMappingConfiguration<TRGT> embeddableConfiguration =
				nullable(linkage.getEmbeddableConfigurationProvider()).map(EmbeddableMappingConfigurationProvider::getConfiguration).get();
		ClassMapping<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>, TARGETTABLE> elementRecordMapping;
		IdentifierAssembler<ID, T> sourceIdentifierAssembler = sourcePersister.getMapping().getIdMapping().getIdentifierAssembler();
		if (embeddableConfiguration == null) {
			String columnName = nullable(linkage.getElementColumnName())
					.getOr(() -> columnNamingStrategy.giveName(collectionProviderDefinition));
			Column<TARGETTABLE, TRGT> elementColumn = targetTable.addColumn(columnName, linkage.getComponentType());
			// adding constraint only if Collection is a Set (because Sets don't allow duplicates) 
			if (collectionProviderDefinition.getMemberType().isAssignableFrom(Set.class)) {
				elementColumn.primaryKey();
			}
			elementRecordMapping = new ElementRecordMapping<>(targetTable, elementColumn, sourceIdentifierAssembler, primaryKeyForeignColumnMapping);
		} else {
			// a special configuration was given, we compute a EmbeddedClassMapping from it
			BeanMappingBuilder<TRGT, TARGETTABLE> elementCollectionMappingBuilder = new BeanMappingBuilder<>(embeddableConfiguration, targetTable,
					dialect.getColumnBinderRegistry(), new ColumnNameProvider(columnNamingStrategy) {
				@Override
				protected String giveColumnName(Linkage pawn) {
					return nullable(linkage.getOverriddenColumnNames().get(pawn.getAccessor()))
							.getOr(() -> super.giveColumnName(pawn));
				}
			});
			Map<ReversibleAccessor<TRGT, Object>, Column<TARGETTABLE, Object>> columnMapping = elementCollectionMappingBuilder.build().getMapping();
			
			Map<ReversibleAccessor<ElementRecord<TRGT, ID>, Object>, Column<TARGETTABLE, Object>> projectedColumnMap = new HashMap<>();
			columnMapping.forEach((propertyAccessor, column) -> {
				AccessorChain<ElementRecord<TRGT, ID>, Object> accessorChain = AccessorChain.chainNullSafe(Arrays.asList(ElementRecord.ELEMENT_ACCESSOR, propertyAccessor), (accessor, valueType) -> {
					if (accessor == ElementRecord.ELEMENT_ACCESSOR) {
						// on getElement(), bean type can't be deduced by reflection due to generic type erasure : default mechanism returns Object
						// so we have to specify our bean type, else a simple Object is instantiated which throws a ClassCastException further
						return Reflections.newInstance(embeddableConfiguration.getBeanType());
					} else {
						// default mechanism
						return ValueInitializerOnNullValue.newInstance(accessor, valueType);
					}
				});
				
				projectedColumnMap.put(accessorChain, column);
				column.primaryKey();
			});
			
			Class<ElementRecord<TRGT, ID>> elementRecordClass = (Class) ElementRecord.class;
			EmbeddedClassMapping<ElementRecord<TRGT, ID>, TARGETTABLE> elementRecordMappingStrategy = new EmbeddedClassMapping<>(elementRecordClass, targetTable, projectedColumnMap);
			elementRecordMapping = new ElementRecordMapping<>(targetTable, elementRecordMappingStrategy, sourceIdentifierAssembler, primaryKeyForeignColumnMapping);
		}
			
		// Note that table will be added to schema thanks to select cascade because join is added to source persister
		SimpleRelationalEntityPersister<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>, TARGETTABLE> elementRecordPersister =
				new ElementRecordPersister<>(elementRecordMapping, dialect, connectionConfiguration);
		
		// insert management
		Accessor<SRC, C> collectionAccessor = linkage.getCollectionProvider();
		addInsertCascade(sourcePersister, elementRecordPersister, collectionAccessor);
		
		// update management
		addUpdateCascade(sourcePersister, elementRecordPersister, collectionAccessor);
		
		// delete management (we provide persisted instances so they are perceived as deletable)
		addDeleteCascade(sourcePersister, elementRecordPersister, collectionAccessor);
		
		// select management
		Supplier<C> collectionFactory = preventNull(
				linkage.getCollectionFactory(),
				BeanRelationFixer.giveCollectionFactory((Class<C>) collectionProviderDefinition.getMemberType()));
		addSelectCascade(sourcePersister, elementRecordPersister, sourcePK, reverseForeignKey,
				linkage.getCollectionProvider().toMutator()::set, collectionAccessor::get,
				collectionFactory);
		
		return elementRecordPersister;
	}
	
	private void registerColumnBinder(ForeignKey<?, ?, ID> reverseColumn, PrimaryKey<?, ID> sourcePK) {
		PairIterator<? extends Column<?, ?>, ? extends Column<?, ?>> pairIterator = new PairIterator<>(reverseColumn.getColumns(), sourcePK.getColumns());
		pairIterator.forEachRemaining(col -> {
			dialect.getColumnBinderRegistry().register(col.getLeft(), dialect.getColumnBinderRegistry().getBinder(col.getRight()));
			dialect.getSqlTypeRegistry().put(col.getLeft(), dialect.getSqlTypeRegistry().getTypeName(col.getRight()));
		});
	}
	
	private void addInsertCascade(ConfiguredRelationalPersister<SRC, ID> sourcePersister,
								  EntityPersister<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>> wrapperPersister,
								  Accessor<SRC, C> collectionAccessor) {
		Function<SRC, Collection<ElementRecord<TRGT, ID>>> collectionProviderForInsert = collectionProvider(
			collectionAccessor,
			sourcePersister.getMapping(),
			false);
		
		sourcePersister.addInsertListener(new TargetInstancesInsertCascader(wrapperPersister, collectionProviderForInsert));
	}
	
	private void addUpdateCascade(ConfiguredRelationalPersister<SRC, ID> sourcePersister,
								  EntityPersister<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>> elementRecordPersister,
								  Accessor<SRC, C> collectionAccessor) {
		Function<SRC, Collection<ElementRecord<TRGT, ID>>> collectionProviderAsPersistedInstances = collectionProvider(
			collectionAccessor,
			sourcePersister.getMapping(),
			true);
		
		BiConsumer<Duo<SRC, SRC>, Boolean> collectionUpdater = new CollectionUpdater<SRC, ElementRecord<TRGT, ID>, Collection<ElementRecord<TRGT, ID>>>(
				collectionProviderAsPersistedInstances,
				elementRecordPersister,
				(o, i) -> { /* no reverse setter because we store only raw values */ },
				true,
				// we base our id policy on a particular identifier because Id is all the same for ElementCollection (it is source bean id)
				ElementRecord::footprint) {
			
			/**
			 * Overridden to force insertion of added entities because as a difference with default behavior (parent class), collection elements are
			 * not entities, so they can't be moved from a collection to another, hence they don't need to be updated, therefore there's no need to
			 * use {@code getElementPersister().persist(..)} mechanism. Even more : it is counterproductive (meaning false) because
			 * {@code persist(..)} uses {@code update(..)} when entities are considered already persisted (not {@code isNew()}), which is always the
			 * case for new {@link ElementRecord}
			 */
			@Override
			protected void insertTargets(UpdateContext updateContext) {
				elementRecordPersister.insert(updateContext.getAddedElements());
			}
		};
		
		sourcePersister.addUpdateListener(new AfterUpdateTrigger<>(collectionUpdater));
	}
	
	private void addDeleteCascade(ConfiguredRelationalPersister<SRC, ID> sourcePersister,
								  EntityPersister<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>> wrapperPersister,
								  Accessor<SRC, C> collectionAccessor) {
		Function<SRC, Collection<ElementRecord<TRGT, ID>>> collectionProviderAsPersistedInstances = collectionProvider(
			collectionAccessor,
			sourcePersister.getMapping(),
			true);
		
		sourcePersister.addDeleteListener(new DeleteTargetEntitiesBeforeDeleteCascader<>(wrapperPersister, collectionProviderAsPersistedInstances));
	}
	
	private void addSelectCascade(ConfiguredRelationalPersister<SRC, ID> sourcePersister,
								  RelationalEntityPersister<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>> elementRecordPersister,
								  PrimaryKey<?, ID> sourcePK,
								  ForeignKey<?, ?, ID> elementRecordToSourceForeignKey,
								  BiConsumer<SRC, C> collectionSetter,
								  Function<SRC, C> collectionGetter,
								  Supplier<C> collectionFactory) {
		// a particular collection fixer that gets raw values (elements) from ElementRecord
		// because elementRecordPersister manages ElementRecord, so it gives them as input of the relation,
		// hence an adaption is needed to "convert" it
		BeanRelationFixer<SRC, ElementRecord<TRGT, ID>> relationFixer = BeanRelationFixer.ofAdapter(
				collectionSetter,
				collectionGetter,
				collectionFactory,
				(bean, input, collection) -> collection.add(input.getElement()));	// element value is taken from ElementRecord
		
		elementRecordPersister.joinAsMany(sourcePersister, sourcePK, elementRecordToSourceForeignKey, relationFixer, null, EntityJoinTree.ROOT_STRATEGY_NAME, true, false);
	}
	
	private Function<SRC, Collection<ElementRecord<TRGT, ID>>> collectionProvider(Accessor<SRC, C> collectionAccessor,
																				  IdAccessor<SRC, ID> idAccessor,
																				  boolean markAsPersisted) {
		return src -> Iterables.collect(Nullable.nullable(collectionAccessor.get(src)).getOr(() -> (C) Collections.emptyList()),
										trgt -> new ElementRecord<>(idAccessor.getId(src), trgt).setPersisted(markAsPersisted),
										HashSet::new);
	}
	
	private static class TargetInstancesInsertCascader<SRC, TRGT, ID> extends AfterInsertCollectionCascader<SRC, ElementRecord<TRGT, ID>> {
		
		private final Function<SRC, ? extends Collection<ElementRecord<TRGT, ID>>> collectionGetter;
		
		public TargetInstancesInsertCascader(EntityPersister<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>> targetPersister, Function<SRC, ? extends Collection<ElementRecord<TRGT, ID>>> collectionGetter) {
			super(targetPersister);
			this.collectionGetter = collectionGetter;
		}
		
		@Override
		protected void postTargetInsert(Iterable<? extends ElementRecord<TRGT, ID>> entities) {
			// Nothing to do. Identified#isPersisted flag should be fixed by target persister
		}
		
		@Override
		protected Collection<ElementRecord<TRGT, ID>> getTargets(SRC source) {
			return collectionGetter.apply(source);
		}
	}
	
	public static class ElementRecordPersister<TRGT, ID, T extends Table<T>> extends SimpleRelationalEntityPersister<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>, T> {
		
		public ElementRecordPersister(ClassMapping<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>, T> elementRecordMapping, Dialect dialect, ConnectionConfiguration connectionConfiguration) {
			super(elementRecordMapping, dialect, connectionConfiguration);
		}
		
		@Override
		public Column getColumn(List<? extends ValueAccessPoint<?>> accessorChain) {
			return Iterables.first(getMapping().getSelectableColumns());
		}
	}
	
}
