package org.codefilarete.stalactite.engine.configurer.elementcollection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.naming.IndexNamingStrategy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.cascade.AfterInsertCollectionCascader;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMappingBuilder;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.ColumnNameProvider;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableLinkage;
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
	
	private final ConfiguredRelationalPersister<SRC, I> sourcePersister;
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private final ColumnNamingStrategy columnNamingStrategy;
	private final ElementCollectionTableNamingStrategy tableNamingStrategy;
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	private final IndexNamingStrategy indexNamingStrategy;
	
	public ElementCollectionRelationConfigurer(ConfiguredRelationalPersister<SRC, I> sourcePersister,
											   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
											   ColumnNamingStrategy columnNamingStrategy,
											   ElementCollectionTableNamingStrategy tableNamingStrategy,
											   Dialect dialect,
											   ConnectionConfiguration connectionConfiguration,
											   IndexNamingStrategy indexNamingStrategy) {
		this.sourcePersister = sourcePersister;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		this.columnNamingStrategy = columnNamingStrategy;
		this.tableNamingStrategy = tableNamingStrategy;
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.indexNamingStrategy = indexNamingStrategy;
	}
	
	public <SRCTABLE extends Table<SRCTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>> void
	configure(ElementCollectionRelation<SRC, TRGT, C> elementCollectionRelation) {
		AccessorDefinition collectionProviderDefinition = AccessorDefinition.giveDefinition(elementCollectionRelation.getCollectionProvider());
		// schema configuration
		PrimaryKey<SRCTABLE, I> sourcePK = sourcePersister.<SRCTABLE>getMapping().getTargetTable().getPrimaryKey();
		
		ElementCollectionMapping<SRCTABLE, COLLECTIONTABLE> elementCollectionMapping = buildCollectionTableMapping(elementCollectionRelation, collectionProviderDefinition, sourcePK);
		
		// Note that table will be added to schema thanks to select cascade because join is added to source persister
		ElementRecordPersister<TRGT, I, COLLECTIONTABLE> collectionPersister =
				new ElementRecordPersister<>(elementCollectionMapping.elementRecordMapping, dialect, connectionConfiguration);
		
		// insert management
		Accessor<SRC, C> collectionAccessor = elementCollectionRelation.getCollectionProvider();
		addInsertCascade(sourcePersister, collectionPersister, collectionAccessor);
		
		// update management
		addUpdateCascade(sourcePersister, collectionPersister, collectionAccessor);
		
		// delete management (we provide persisted instances so they are perceived as deletable)
		addDeleteCascade(sourcePersister, collectionPersister, collectionAccessor);
		
		// select management
		Supplier<C> collectionFactory = preventNull(
				elementCollectionRelation.getCollectionFactory(),
				BeanRelationFixer.giveCollectionFactory((Class<C>) collectionProviderDefinition.getMemberType()));
		addSelectCascade(sourcePersister, collectionPersister, sourcePK, elementCollectionMapping.reverseForeignKey,
				elementCollectionRelation.getCollectionProvider().toMutator()::set, collectionAccessor,
				collectionFactory);
	}
	
	private <SRCTABLE extends Table<SRCTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>> ElementCollectionMapping<SRCTABLE, COLLECTIONTABLE>
	buildCollectionTableMapping(ElementCollectionRelation<SRC, TRGT, C> linkage, AccessorDefinition collectionProviderDefinition, PrimaryKey<SRCTABLE, I> sourcePK) {
		String tableName = nullable(linkage.getTargetTableName()).getOr(() -> tableNamingStrategy.giveName(collectionProviderDefinition));
		// Note that table will participate to DDL while cascading selection thanks to its join on foreignKey 
		COLLECTIONTABLE targetTable = (COLLECTIONTABLE) nullable(linkage.getTargetTable()).getOr(() -> new Table(tableName));
		Map<Column<SRCTABLE, ?>, Column<COLLECTIONTABLE, ?>> primaryKeyForeignColumnMapping = new HashMap<>();
		Column<COLLECTIONTABLE, I> reverseColumn = (Column) linkage.getReverseColumn();
		Key<COLLECTIONTABLE, I> reverseKey = nullable(reverseColumn).map(Key::ofSingleColumn)
				.getOr(() -> {
					KeyBuilder<COLLECTIONTABLE, I> result = Key.from(targetTable);
					sourcePK.getColumns().forEach(col -> {
						String reverseColumnName = nullable(linkage.getReverseColumnName()).getOr(() ->
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
				nullable(linkage.getEmbeddableConfigurationProvider()).map(EmbeddableMappingConfigurationProvider::getConfiguration).get();
		DefaultEntityMapping<ElementRecord<TRGT, I>, ElementRecord<TRGT, I>, COLLECTIONTABLE> elementRecordMapping;
		IdentifierAssembler<I, SRCTABLE> sourceIdentifierAssembler = sourcePersister.getMapping().getIdMapping().getIdentifierAssembler();
		if (embeddableConfiguration == null) {
			String columnName = nullable(linkage.getElementColumnName())
					.getOr(() -> columnNamingStrategy.giveName(collectionProviderDefinition));
			Column<COLLECTIONTABLE, TRGT> elementColumn = targetTable.addColumn(columnName, linkage.getComponentType());
			// adding constraint only if Collection is a Set (because Sets don't allow duplicates) 
			if (collectionProviderDefinition.getMemberType().isAssignableFrom(Set.class)) {
				elementColumn.primaryKey();
			}
			elementRecordMapping = new ElementRecordMapping<>(targetTable, elementColumn, sourceIdentifierAssembler, primaryKeyForeignColumnMapping);
		} else {
			// a special configuration was given, we compute a EmbeddedClassMapping from it
			EmbeddableMappingBuilder<TRGT, COLLECTIONTABLE> elementCollectionMappingBuilder = new EmbeddableMappingBuilder<>(embeddableConfiguration, targetTable,
					dialect.getColumnBinderRegistry(), new ColumnNameProvider(columnNamingStrategy) {
				@Override
				protected String giveColumnName(EmbeddableLinkage pawn) {
					return nullable(linkage.getOverriddenColumnNames().get(pawn.getAccessor()))
							.getOr(() -> super.giveColumnName(pawn));
				}
			}, indexNamingStrategy);
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
						return ValueInitializerOnNullValue.newInstance(accessor, valueType);
					}
				});
				
				projectedColumnMap.put(accessorChain, column);
				column.primaryKey();
			});
			
			Class<ElementRecord<TRGT, I>> elementRecordClass = (Class) ElementRecord.class;
			EmbeddedClassMapping<ElementRecord<TRGT, I>, COLLECTIONTABLE> elementRecordMappingStrategy = new EmbeddedClassMapping<>(elementRecordClass, targetTable, projectedColumnMap);
			elementRecordMapping = new ElementRecordMapping<>(targetTable, elementRecordMappingStrategy, sourceIdentifierAssembler, primaryKeyForeignColumnMapping);
		}
		return new ElementCollectionMapping<>(reverseForeignKey, elementRecordMapping);
	}
	
	private class ElementCollectionMapping<SRCTABLE extends Table<SRCTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>> {
		public final ForeignKey<COLLECTIONTABLE, SRCTABLE, I> reverseForeignKey;
		public final DefaultEntityMapping<ElementRecord<TRGT, I>, ElementRecord<TRGT, I>, COLLECTIONTABLE> elementRecordMapping;
		
		public ElementCollectionMapping(ForeignKey<COLLECTIONTABLE, SRCTABLE, I> reverseForeignKey, DefaultEntityMapping<ElementRecord<TRGT, I>, ElementRecord<TRGT, I>, COLLECTIONTABLE> elementRecordMapping) {
			this.reverseForeignKey = reverseForeignKey;
			this.elementRecordMapping = elementRecordMapping;
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
								  Accessor<SRC, C> collectionAccessor) {
		Function<SRC, Collection<ElementRecord<TRGT, I>>> collectionProviderForInsert = collectionProvider(
			collectionAccessor,
			sourcePersister.getMapping(),
			false);
		
		sourcePersister.addInsertListener(new TargetInstancesInsertCascader(wrapperPersister, collectionProviderForInsert));
	}
	
	private void addUpdateCascade(ConfiguredRelationalPersister<SRC, I> sourcePersister,
								  EntityPersister<ElementRecord<TRGT, I>, ElementRecord<TRGT, I>> elementRecordPersister,
								  Accessor<SRC, C> collectionAccessor) {
		Function<SRC, Collection<ElementRecord<TRGT, I>>> collectionProviderAsPersistedInstances = collectionProvider(
			collectionAccessor,
			sourcePersister.getMapping(),
			true);
		
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
								  Accessor<SRC, C> collectionAccessor) {
		Function<SRC, Collection<ElementRecord<TRGT, I>>> collectionProviderAsPersistedInstances = collectionProvider(
			collectionAccessor,
			sourcePersister.getMapping(),
			true);
		
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
	
	private Function<SRC, Collection<ElementRecord<TRGT, I>>> collectionProvider(Accessor<SRC, C> collectionAccessor,
																				 IdAccessor<SRC, I> idAccessor,
																				 boolean markAsPersisted) {
		return src -> Iterables.collect(Nullable.nullable(collectionAccessor.get(src)).getOr(() -> (C) new ArrayList<>()),
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
		
		public ElementRecordPersister(DefaultEntityMapping<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>, T> elementRecordMapping, Dialect dialect, ConnectionConfiguration connectionConfiguration) {
			super(elementRecordMapping, dialect, connectionConfiguration);
		}
	}
}
