package org.codefilarete.stalactite.engine.configurer.elementcollection;

import java.util.ArrayList;
import java.util.Collection;
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
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.IndexNamingStrategy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.cascade.AfterInsertCollectionCascader;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableLinkage;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMappingBuilder;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.AfterUpdateTrigger;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.DeleteTargetEntitiesBeforeDeleteCascader;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.mapping.EmbeddedClassMapping;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.Size;
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

import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.bean.Objects.preventNull;

/**
 * Class that configures element-collection mapping
 * 
 * @author Guillaume Mary
 */
public class ElementCollectionRelationConfigurer<SRC, TRGT, I, C extends Collection<TRGT>> {
	
	private static final AccessorDefinition ELEMENT_RECORD_ID_ACCESSOR_DEFINITION = AccessorDefinition.giveDefinition(new AccessorByMethodReference<>(ElementRecord<Object, Object>::getId));
	private static final AccessorDefinition ELEMENT_RECORD_INDEX_ACCESSOR_DEFINITION = AccessorDefinition.giveDefinition(IndexedElementRecord.INDEX_ACCESSOR);
	
	private final ConfiguredRelationalPersister<SRC, I> sourcePersister;
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private final ColumnNamingStrategy columnNamingStrategy;
	private final ColumnNamingStrategy indexColumnNamingStrategy;
	private final ElementCollectionTableNamingStrategy tableNamingStrategy;
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	private final IndexNamingStrategy indexNamingStrategy;
	
	public ElementCollectionRelationConfigurer(ConfiguredRelationalPersister<SRC, I> sourcePersister,
											   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
											   ColumnNamingStrategy columnNamingStrategy,
											   ColumnNamingStrategy indexColumnNamingStrategy,
											   ElementCollectionTableNamingStrategy tableNamingStrategy,
											   Dialect dialect,
											   ConnectionConfiguration connectionConfiguration,
											   IndexNamingStrategy indexNamingStrategy) {
		this.sourcePersister = sourcePersister;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		this.columnNamingStrategy = columnNamingStrategy;
		this.indexColumnNamingStrategy = indexColumnNamingStrategy;
		this.tableNamingStrategy = tableNamingStrategy;
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.indexNamingStrategy = indexNamingStrategy;
	}
	
	public <SRCTABLE extends Table<SRCTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>> void
	configure(ElementCollectionRelation<SRC, TRGT, C> elementCollectionRelation) {
		AccessorDefinition collectionProviderDefinition = AccessorDefinition.giveDefinition(elementCollectionRelation.getCollectionAccessor());
		// schema configuration
		PrimaryKey<SRCTABLE, I> sourcePK = sourcePersister.<SRCTABLE>getMapping().getTargetTable().getPrimaryKey();
		
		ElementCollectionMapping<SRCTABLE, COLLECTIONTABLE, ElementRecord<TRGT, I>> elementCollectionMapping = buildCollectionTableMapping(elementCollectionRelation, collectionProviderDefinition, sourcePK);
		
		// Note that table will be added to schema thanks to select cascade because join is added to source persister
		ElementRecordPersister<TRGT, I, COLLECTIONTABLE> collectionPersister =
				new ElementRecordPersister<>(elementCollectionMapping.elementRecordMapping, dialect, connectionConfiguration);
		
		// insert management
		Accessor<SRC, C> collectionAccessor = elementCollectionRelation.getCollectionAccessor();
		addInsertCascade(sourcePersister, collectionPersister, elementCollectionMapping.collectionProvider(collectionAccessor, sourcePersister.getMapping(), false));
		
		// update management
		addUpdateCascade(sourcePersister, collectionPersister, elementCollectionMapping.collectionProvider(collectionAccessor, sourcePersister.getMapping(), true));
		
		// delete management (we provide persisted instances so they are perceived as deletable)
		addDeleteCascade(sourcePersister, collectionPersister, elementCollectionMapping.collectionProvider(collectionAccessor, sourcePersister.getMapping(), true));
		
		// select management
		Supplier<C> collectionFactory = preventNull(
				elementCollectionRelation.getCollectionFactory(),
				Reflections.giveCollectionFactory((Class<C>) collectionProviderDefinition.getMemberType()));
		addSelectCascade(sourcePersister, collectionPersister, sourcePK, elementCollectionMapping.reverseForeignKey,
				elementCollectionRelation.getCollectionAccessor().toMutator()::set, collectionAccessor,
				collectionFactory);
	}
	
	private <SRCTABLE extends Table<SRCTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>, ER extends ElementRecord<TRGT, I>> ElementCollectionMapping<SRCTABLE, COLLECTIONTABLE, ER>
	buildCollectionTableMapping(ElementCollectionRelation<SRC, TRGT, C> collectionRelation, AccessorDefinition collectionProviderDefinition, PrimaryKey<SRCTABLE, I> sourcePK) {
		String tableName = nullable(collectionRelation.getTargetTableName()).getOr(() -> {
			String generatedTableName = tableNamingStrategy.giveName(collectionProviderDefinition);
			// we replace dot character by underscore one to take embedded relation properties into account: their accessor is an AccessorChain
			// which is printed with dots by AccessorDefinition
			return generatedTableName.replace('.', '_');
		});
		// Note that table will participate to DDL while cascading selection thanks to its join on foreignKey 
		COLLECTIONTABLE targetTable = (COLLECTIONTABLE) nullable(collectionRelation.getTargetTable()).getOr(() -> new Table(tableName));
		Map<Column<SRCTABLE, ?>, Column<COLLECTIONTABLE, ?>> primaryKeyForeignColumnMapping = new HashMap<>();
		Column<COLLECTIONTABLE, I> reverseColumn = (Column) collectionRelation.getReverseColumn();
		Key<COLLECTIONTABLE, I> reverseKey = nullable(reverseColumn).map(Key::ofSingleColumn)
				.getOr(() -> {
					KeyBuilder<COLLECTIONTABLE, I> result = Key.from(targetTable);
					sourcePK.getColumns().forEach(col -> {
						String reverseColumnName = nullable(collectionRelation.getReverseColumnName()).getOr(() ->
								columnNamingStrategy.giveName(ELEMENT_RECORD_ID_ACCESSOR_DEFINITION));
						Column<COLLECTIONTABLE, ?> reverseCol = targetTable.addColumn(reverseColumnName, col.getJavaType())
								.primaryKey();
						primaryKeyForeignColumnMapping.put(col, reverseCol);
						result.addColumn(reverseCol);
					});
					return result.build();
				});
		ForeignKey<COLLECTIONTABLE, SRCTABLE, I> reverseForeignKey = targetTable.addForeignKey(this.foreignKeyNamingStrategy::giveName, reverseKey, sourcePK);
		registerColumnBinder(reverseForeignKey, sourcePK);	// because sourcePk binder might have been overloaded by column so we need to adjust to it
		
		EmbeddableMappingConfiguration<TRGT> embeddableConfiguration =
				nullable(collectionRelation.getEmbeddableConfigurationProvider()).map(EmbeddableMappingConfigurationProvider::getConfiguration).get();
		DefaultEntityMapping<ER, ER, COLLECTIONTABLE> elementRecordMapping = null;
		IdentifierAssembler<I, SRCTABLE> sourceIdentifierAssembler = sourcePersister.getMapping().getIdMapping().getIdentifierAssembler();
		if (embeddableConfiguration == null) {
			String columnName = nullable(collectionRelation.getElementColumnName())
					.getOr(() -> columnNamingStrategy.giveName(collectionProviderDefinition));
			Column<COLLECTIONTABLE, TRGT> elementColumn = targetTable.addColumn(columnName, collectionRelation.getComponentType(), collectionRelation.getElementColumnSize());
			if (collectionRelation.isOrdered()) {
				String indexingColumnName = nullable(collectionRelation.getIndexingColumnName()).getOr(() -> indexColumnNamingStrategy.giveName(ELEMENT_RECORD_INDEX_ACCESSOR_DEFINITION));
				Column<COLLECTIONTABLE, Integer> indexColumn = targetTable.addColumn(indexingColumnName, Integer.class);
				// adding a constraint on the index column, not on the element one (like for Set), to allow duplicates
				indexColumn.primaryKey();
				elementRecordMapping = (DefaultEntityMapping<ER, ER, COLLECTIONTABLE>) new IndexedElementRecordMapping<>(targetTable, elementColumn, indexColumn, sourceIdentifierAssembler, primaryKeyForeignColumnMapping);
			} else {
				// adding a constraint on the element column because Sets don't allow duplicates
				elementColumn.primaryKey();
				elementRecordMapping = (DefaultEntityMapping<ER, ER, COLLECTIONTABLE>) new ElementRecordMapping<>(targetTable, elementColumn, sourceIdentifierAssembler, primaryKeyForeignColumnMapping);
			}
		} else {
			// a special configuration was given, we compute a EmbeddedClassMapping from it
			EmbeddableMappingBuilder<TRGT, COLLECTIONTABLE> elementCollectionMappingBuilder = new EmbeddableMappingBuilder<TRGT, COLLECTIONTABLE>(embeddableConfiguration, targetTable,
					dialect.getColumnBinderRegistry(), columnNamingStrategy, indexNamingStrategy) {
				@Override
				protected <O> String determineColumnName(EmbeddableLinkage<TRGT, O> linkage, @javax.annotation.Nullable String overriddenColumName) {
					return super.determineColumnName(linkage, collectionRelation.getOverriddenColumnNames().get(linkage.getAccessor()));
				}
				
				@Override
				protected <O> Size determineColumnSize(EmbeddableLinkage<TRGT, O> linkage, @javax.annotation.Nullable Size overriddenColumSize) {
					return super.determineColumnSize(linkage, collectionRelation.getOverriddenColumnSizes().get(linkage.getAccessor()));
				}
			};
			Map<ReversibleAccessor<TRGT, Object>, Column<COLLECTIONTABLE, Object>> columnMapping = elementCollectionMappingBuilder.build().getMapping();
			
			Map<ReversibleAccessor<ElementRecord<TRGT, I>, Object>, Column<COLLECTIONTABLE, Object>> projectedColumnMap = new HashMap<>();
			columnMapping.forEach((propertyAccessor, column) -> {
				AccessorChain<ElementRecord<TRGT, I>, Object> accessorChain = AccessorChain.fromAccessorsWithNullSafe(Arrays.asList(ElementRecord.ELEMENT_ACCESSOR, propertyAccessor), (accessor, valueType) -> {
					if (accessor == ElementRecord.ELEMENT_ACCESSOR) {
						// on getElement(), bean type can't be deduced by reflection due to generic type erasure : default mechanism returns Object
						// so we have to specify our bean type, else a simple Object is instantiated which throws a ClassCastException further
						return Reflections.newInstance(embeddableConfiguration.getBeanType());
					} else {
						// default mechanism
						return Reflections.newInstance(valueType);
					}
				});
				
				projectedColumnMap.put(accessorChain, column);
				column.primaryKey();
			});
			
			Class<ElementRecord<TRGT, I>> elementRecordClass = (Class) ElementRecord.class;
			EmbeddedClassMapping<ElementRecord<TRGT, I>, COLLECTIONTABLE> elementRecordMappingStrategy = new EmbeddedClassMapping<>(elementRecordClass, targetTable, projectedColumnMap);
			elementRecordMapping = (DefaultEntityMapping<ER, ER, COLLECTIONTABLE>) new ElementRecordMapping<>(targetTable, elementRecordMappingStrategy, sourceIdentifierAssembler, primaryKeyForeignColumnMapping);
		}
		// Returns collection mapping based on collection type
		if (Set.class.isAssignableFrom(collectionProviderDefinition.getMemberType())) {
			return new ElementCollectionMapping<>(reverseForeignKey, elementRecordMapping);
		} else if (List.class.isAssignableFrom(collectionProviderDefinition.getMemberType())) {
			return (ElementCollectionMapping) new IndexedElementCollectionMapping<>(reverseForeignKey, (DefaultEntityMapping<IndexedElementRecord<TRGT, I>, IndexedElementRecord<TRGT, I>, COLLECTIONTABLE>) elementRecordMapping);
		}
		return null;
	}
	
	private class ElementCollectionMapping<SRCTABLE extends Table<SRCTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>, ER extends ElementRecord<TRGT, I>> {
		public final ForeignKey<COLLECTIONTABLE, SRCTABLE, I> reverseForeignKey;
		public final DefaultEntityMapping<ER, ER, COLLECTIONTABLE> elementRecordMapping;
		
		public ElementCollectionMapping(ForeignKey<COLLECTIONTABLE, SRCTABLE, I> reverseForeignKey, DefaultEntityMapping<ER, ER, COLLECTIONTABLE> elementRecordMapping) {
			this.reverseForeignKey = reverseForeignKey;
			this.elementRecordMapping = elementRecordMapping;
		}
		
		protected Function<SRC, Collection<ElementRecord<TRGT, I>>> collectionProvider(Accessor<SRC, C> collectionAccessor,
																					 IdAccessor<SRC, I> idAccessor,
																					 boolean markAsPersisted) {
			return src -> Iterables.collect(Nullable.nullable(collectionAccessor.get(src)).getOr(() -> (C) new ArrayList<>()),
					trgt -> new ElementRecord<>(idAccessor.getId(src), trgt).setPersisted(markAsPersisted),
					HashSet::new);
		}
	}
	
	private class IndexedElementCollectionMapping<SRCTABLE extends Table<SRCTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>, ER extends IndexedElementRecord<TRGT, I>> extends ElementCollectionMapping<SRCTABLE, COLLECTIONTABLE, ER> {
		
		public IndexedElementCollectionMapping(ForeignKey<COLLECTIONTABLE, SRCTABLE, I> reverseForeignKey, DefaultEntityMapping<ER, ER, COLLECTIONTABLE> elementRecordMapping) {
			super(reverseForeignKey, elementRecordMapping);
		}
		
		@Override
		protected Function<SRC, Collection<ElementRecord<TRGT, I>>> collectionProvider(Accessor<SRC, C> collectionAccessor,
																					   IdAccessor<SRC, I> idAccessor,
																					   boolean markAsPersisted) {
			return src -> {
				C collection = nullable(collectionAccessor.get(src)).getOr(() -> (C) new ArrayList<>());
				return Iterables.collect(collection,
						trgt -> new IndexedElementRecord<>(idAccessor.getId(src), trgt, Iterables.indexOf(collection, trgt)).setPersisted(markAsPersisted),
						HashSet::new);
			};
		}
	}
	
	private void registerColumnBinder(ForeignKey<?, ?, I> reverseColumn, PrimaryKey<?, I> sourcePK) {
		PairIterator<? extends Column<?, ?>, ? extends Column<?, ?>> pairIterator = new PairIterator<>(reverseColumn.getColumns(), sourcePK.getColumns());
		pairIterator.forEachRemaining(col -> {
			dialect.getColumnBinderRegistry().register(col.getLeft(), dialect.getColumnBinderRegistry().getBinder(col.getRight()));
			dialect.getSqlTypeRegistry().put(col.getLeft(), dialect.getSqlTypeRegistry().getTypeName(col.getRight()));
		});
	}
	
	private void addInsertCascade(ConfiguredRelationalPersister<SRC, I> sourcePersister,
								  EntityPersister<ElementRecord<TRGT, I>, ElementRecord<TRGT, I>> wrapperPersister,
								  Function<SRC, Collection<ElementRecord<TRGT, I>>> collectionProviderForInsert) {
		sourcePersister.addInsertListener(new TargetInstancesInsertCascader<>(wrapperPersister, collectionProviderForInsert));
	}
	
	private void addUpdateCascade(ConfiguredRelationalPersister<SRC, I> sourcePersister,
								  EntityPersister<ElementRecord<TRGT, I>, ElementRecord<TRGT, I>> elementRecordPersister,
								  Function<SRC, Collection<ElementRecord<TRGT, I>>> collectionProviderAsPersistedInstances) {
		BiConsumer<Duo<SRC, SRC>, Boolean> collectionUpdater = new CollectionUpdater<SRC, ElementRecord<TRGT, I>, Collection<ElementRecord<TRGT, I>>>(
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
	
	private void addDeleteCascade(ConfiguredRelationalPersister<SRC, I> sourcePersister,
								  EntityPersister<ElementRecord<TRGT, I>, ElementRecord<TRGT, I>> wrapperPersister,
								  Function<SRC, Collection<ElementRecord<TRGT, I>>> collectionProviderAsPersistedInstances) {
		sourcePersister.addDeleteListener(new DeleteTargetEntitiesBeforeDeleteCascader<>(wrapperPersister, collectionProviderAsPersistedInstances));
	}
	
	private String addSelectCascade(RelationalEntityPersister<SRC, I> sourcePersister,
									RelationalEntityPersister<ElementRecord<TRGT, I>, ElementRecord<TRGT, I>> elementRecordPersister,
									PrimaryKey<?, I> sourcePK,
									ForeignKey<?, ?, I> elementRecordToSourceForeignKey,
									BiConsumer<SRC, C> collectionSetter,
									Accessor<SRC, C> collectionGetter,
									Supplier<C> collectionFactory) {
		// a particular collection fixer that gets raw values (elements) from ElementRecord
		// because elementRecordPersister manages ElementRecord, so it gives them as input of the relation,
		// hence an adaption is needed to "convert" it.
		// Note that this code is wrongly typed: the relationFixer should be of <SRC, C> to access the property, whereas it is typed with
		// ElementRecord<TRGT, I> to fulfill the adapter argument. There's a kind of magic here that make it works (generics type erasure, and wrong
		// ofAdapter(..) type deduction by compiler to match the relationFixer variable.
		BeanRelationFixer<SRC, ElementRecord<TRGT, I>> relationFixer = BeanRelationFixer.ofAdapter(
				collectionSetter,
				collectionGetter::get,
				collectionFactory,
				(bean, input, collection) -> collection.add(input.getElement()));	// element value is taken from ElementRecord
		
		return elementRecordPersister.joinAsMany(EntityJoinTree.ROOT_JOIN_NAME, sourcePersister, collectionGetter, sourcePK, elementRecordToSourceForeignKey, relationFixer, null, true, false);
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
		
		public ElementRecordPersister(DefaultEntityMapping<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>, T> elementRecordMapping, Dialect dialect, ConnectionConfiguration connectionConfiguration) {
			super(elementRecordMapping, dialect, connectionConfiguration);
		}
	}
}
