package org.codefilarete.stalactite.engine.configurer.elementcollection;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorChain.ValueInitializerOnNullValue;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfiguration.Linkage;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.cascade.AfterInsertCollectionCascader;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.codefilarete.stalactite.engine.diff.AbstractDiff;
import org.codefilarete.stalactite.engine.diff.CollectionDiffer;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.DeleteTargetEntitiesBeforeDeleteCascader;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.TargetInstancesUpdateCascader;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.EmbeddedClassMapping;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.collection.PairIterator;

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
	
	public <T extends Table<T>, TARGETTABLE extends Table<TARGETTABLE>> void configure() {
		
		AccessorDefinition collectionProviderDefinition = AccessorDefinition.giveDefinition(linkage.getCollectionProvider());
		// schema configuration
		PrimaryKey<T, ID> sourcePK = sourcePersister.<T>getMapping().getTargetTable().getPrimaryKey();
		
		String tableName = nullable(linkage.getTargetTableName()).getOr(() -> tableNamingStrategy.giveName(collectionProviderDefinition));
		TARGETTABLE targetTable = (TARGETTABLE) nullable(linkage.getTargetTable()).getOr(() -> new Table(tableName));
		Map<Column<T, Object>, Column<TARGETTABLE, Object>> primaryKeyForeignColumnMapping = new HashMap<>();
		Key<TARGETTABLE, ID> reverseKey = nullable((Column<TARGETTABLE, ID>) (Column) linkage.getReverseColumn()).map(Key::ofSingleColumn)
				.getOr(() -> {
					KeyBuilder<TARGETTABLE, ID> result = Key.from(targetTable);
					sourcePK.getColumns().forEach(col -> {
						String reverseColumnName = nullable(linkage.getReverseColumnName()).getOr(() ->
								columnNamingStrategy.giveName(ELEMENT_RECORD_ID_ACCESSOR_DEFINITION));
						Column<TARGETTABLE, Object> reverseCol = targetTable.addColumn(reverseColumnName, col.getJavaType())
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
		ClassMapping<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>, TARGETTABLE> elementRecordStrategy;
		IdentifierAssembler<ID, T> sourceIdentifierAssembler = sourcePersister.getMapping().getIdMapping().getIdentifierAssembler();
		if (embeddableConfiguration == null) {
			String columnName = nullable(linkage.getElementColumnName())
					.getOr(() -> columnNamingStrategy.giveName(collectionProviderDefinition));
			Column<TARGETTABLE, TRGT> elementColumn = targetTable.addColumn(columnName, linkage.getComponentType());
			// adding constraint only if Collection is a Set (because Sets don't allow duplicates) 
			if (collectionProviderDefinition.getMemberType().isAssignableFrom(Set.class)) {
				elementColumn.primaryKey();
			}
			elementRecordStrategy = new ElementRecordMapping<>(targetTable, elementColumn, sourceIdentifierAssembler, primaryKeyForeignColumnMapping);
		} else {
			// a special configuration was given, we compute a EmbeddedClassMapping from it
			BeanMappingBuilder<TRGT, TARGETTABLE> elementCollectionMappingBuilder = new BeanMappingBuilder<>();
			Map<ReversibleAccessor<TRGT, Object>, Column<TARGETTABLE, Object>> columnMapping = elementCollectionMappingBuilder.build(embeddableConfiguration, targetTable,
					dialect.getColumnBinderRegistry(), new ColumnNameProvider(columnNamingStrategy) {
						@Override
						protected String giveColumnName(Linkage pawn) {
							return nullable(linkage.getOverriddenColumnNames().get(pawn.getAccessor()))
									.getOr(() -> super.giveColumnName(pawn));
						}
					});
			
			Map<ReversibleAccessor<ElementRecord<TRGT, ID>, Object>, Column<TARGETTABLE, Object>> projectedColumnMap = new HashMap<>();
			columnMapping.forEach((propertyAccessor, column) -> {
				AccessorChain<ElementRecord<TRGT, ID>, Object> accessorChain = AccessorChain.forModel(Arrays.asList(ElementRecord.ELEMENT_ACCESSOR, propertyAccessor), (accessor, valueType) -> {
					if (accessor == ElementRecord.ELEMENT_ACCESSOR) {
						// on getElement(), bean type can't be deduced by reflection due to generic type erasure : default mechanism returns Object
						// so we have to specify our bean type, else a simple Object is instantiated which throws a ClassCastException further
						return embeddableConfiguration.getBeanType();
					} else {
						// default mechanism
						return ValueInitializerOnNullValue.giveValueType(accessor, valueType);
					}
				});
				
				projectedColumnMap.put(accessorChain, column);
				column.primaryKey();
			});
			
			Class<ElementRecord<TRGT, ID>> elementRecordClass = (Class) ElementRecord.class;
			EmbeddedClassMapping<ElementRecord<TRGT, ID>, TARGETTABLE> elementRecordMappingStrategy = new EmbeddedClassMapping<>(elementRecordClass, targetTable, projectedColumnMap);
			elementRecordStrategy = new ElementRecordMapping<>(targetTable, elementRecordMappingStrategy, sourceIdentifierAssembler, primaryKeyForeignColumnMapping);
		}
			
		// Note that table will be added to schema thanks to select cascade because join is added to source persister
		SimpleRelationalEntityPersister<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>, TARGETTABLE> elementRecordPersister =
			new SimpleRelationalEntityPersister<>(elementRecordStrategy, dialect, connectionConfiguration);
		
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
	}
	
	private void registerColumnBinder(ForeignKey<?, ?, ID> reverseColumn, PrimaryKey<?, ID> sourcePK) {
		PairIterator<? extends Column<?, Object>, ? extends Column<?, Object>> pairIterator = new PairIterator<>(reverseColumn.getColumns(), sourcePK.getColumns());
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
								  EntityPersister<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>> wrapperPersister,
								  Accessor<SRC, C> collectionAccessor) {
		Function<SRC, Collection<ElementRecord<TRGT, ID>>> collectionProviderAsPersistedInstances = collectionProvider(
			collectionAccessor,
			sourcePersister.getMapping(),
			true);
		
		BiConsumer<Duo<SRC, SRC>, Boolean> updateListener = new CollectionUpdater<SRC, ElementRecord<TRGT, ID>, Collection<ElementRecord<TRGT, ID>>>(
				collectionProviderAsPersistedInstances,
				wrapperPersister,
				(o, i) -> { /* no reverse setter because we store only raw values */ },
				true,
				// we base our id policy on a particular identifier because Id is all the same for ElementCollection (it is source bean id)
				ElementRecord::footprint) {
			
			/**
			 * Override to mark for insertion given instance because parent implementation is based on IsNew which is always true
			 */
			@Override
			protected void onAddedTarget(UpdateContext updateContext, AbstractDiff<ElementRecord<TRGT, ID>> diff) {
				updateContext.getEntitiesToBeInserted().add(diff.getReplacingInstance());
			}
		};
		
		sourcePersister.addUpdateListener(new TargetInstancesUpdateCascader<>(wrapperPersister, updateListener));
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
		return src -> Iterables.collect(collectionAccessor.get(src),
										trgt -> new ElementRecord<>(idAccessor.getId(src), trgt).setPersisted(markAsPersisted),
										HashSet::new);
	}
	
	/**
	 * Mapping strategy dedicated to {@link ElementRecord}. Very close to {@link org.codefilarete.stalactite.engine.AssociationTableNamingStrategy}
	 * in its principle.
	 */
	private static class ElementRecordMapping<C, I, T extends Table<T>> extends ClassMapping<ElementRecord<C, I>, ElementRecord<C, I>, T> {
		
		private <LEFTTABLE extends Table<LEFTTABLE>> ElementRecordMapping(T targetTable,
																		  Column<T, C> elementColumn,
																		  IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
																		  Map<Column<LEFTTABLE, Object>, Column<T, Object>> primaryKeyForeignColumnMapping) {
			super((Class) ElementRecord.class,
					targetTable,
					(Map) Maps.forHashMap(ReversibleAccessor.class, Column.class)
							.add(ElementRecord.ELEMENT_ACCESSOR, elementColumn),
					new ElementRecordIdMapping<>(targetTable, elementColumn, sourceIdentifierAssembler, primaryKeyForeignColumnMapping));
		}
		
		private <LEFTTABLE extends Table<LEFTTABLE>> ElementRecordMapping(T targetTable,
																		  EmbeddedClassMapping<ElementRecord<C, I>, T> embeddableMapping,
																		  IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
																		  Map<Column<LEFTTABLE, Object>, Column<T, Object>> primaryKeyForeignColumnMapping) {
			super((Class) ElementRecord.class,
					targetTable,
					embeddableMapping.getPropertyToColumn(),
					new ElementRecordIdMapping<>(targetTable, embeddableMapping, sourceIdentifierAssembler, primaryKeyForeignColumnMapping));
		}
		
		/**
		 * {@link IdMapping} for {@link ElementRecord} : a composed id made of
		 * {@link ElementRecord#getId()} and {@link ElementRecord#getElement()}
		 */
		private static class ElementRecordIdMapping<C, I, T extends Table<T>> extends ComposedIdMapping<ElementRecord<C, I>, ElementRecord<C, I>> {
			
			public <LEFTTABLE extends Table<LEFTTABLE>> ElementRecordIdMapping(
					T targetTable,
					Column<T, C> elementColumn,
					IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
					Map<Column<LEFTTABLE, Object>, Column<T, Object>> primaryKeyForeignColumnMapping) {
				super(new ElementRecordIdAccessor<>(),
						new AlreadyAssignedIdentifierManager<>((Class<ElementRecord<C, I>>) (Class) ElementRecord.class,
								ElementRecord::markAsPersisted,
								ElementRecord::isPersisted),
						new DefaultElementRecordIdentifierAssembler<>(targetTable, elementColumn, sourceIdentifierAssembler, primaryKeyForeignColumnMapping));
			}
			
			public <LEFTTABLE extends Table<LEFTTABLE>> ElementRecordIdMapping(
					T targetTable,
					EmbeddedClassMapping<ElementRecord<C, I>, T> elementColumn,
					IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
					Map<Column<LEFTTABLE, Object>, Column<T, Object>> primaryKeyForeignColumnMapping) {
				super(new ElementRecordIdAccessor<>(),
						new AlreadyAssignedIdentifierManager<>((Class<ElementRecord<C, I>>) (Class) ElementRecord.class,
								ElementRecord::markAsPersisted,
								ElementRecord::isPersisted),
						new ConfiguredElementRecordIdentifierAssembler<>(targetTable, elementColumn, sourceIdentifierAssembler, primaryKeyForeignColumnMapping));
			}
			
			/**
			 * Override because {@link ComposedIdMapping} is based on null identifier to determine newness, which is always false for {@link ElementRecord}
			 * because they always have one. We delegate its computation to the entity.
			 * 
			 * @param entity any non-null entity
			 * @return true or false based on {@link ElementRecord#isNew()}
			 */
			@Override
			public boolean isNew(ElementRecord<C, I> entity) {
				return entity.isNew();
			}
			
			private static class ElementRecordIdAccessor<C, I> implements IdAccessor<ElementRecord<C, I>, ElementRecord<C, I>> {
				
				@Override
				public ElementRecord<C, I> getId(ElementRecord<C, I> associationRecord) {
					return associationRecord;
				}
				
				@Override
				public void setId(ElementRecord<C, I> associationRecord, ElementRecord<C, I> identifier) {
					associationRecord.setId(identifier.getId());
					associationRecord.setElement(identifier.getElement());
				}
			}
			
			/**
			 * Identifier assembler when {@link ElementRecord} is persisted according to a default behavior :
			 * - identifier is saved in table primary key 
			 * - element value is saved in elementColumn 
			 * 
			 * @param <TRGT> embedded bean type
			 * @param <ID> source identifier type
			 */
			private static class DefaultElementRecordIdentifierAssembler<TRGT, ID, T extends Table<T>> extends ComposedIdentifierAssembler<ElementRecord<TRGT, ID>, T> {
				
				private final Column<T, TRGT> elementColumn;
				private final IdentifierAssembler<ID, ?> sourceIdentifierAssembler;
				private final Map<Column<?, Object>, Column<T, Object>> primaryKeyForeignColumnMapping;
				
				private <LEFTTABLE extends Table<LEFTTABLE>> DefaultElementRecordIdentifierAssembler(T targetTable,
																Column<T, TRGT> elementColumn,
																IdentifierAssembler<ID, LEFTTABLE> sourceIdentifierAssembler,
																Map<Column<LEFTTABLE, Object>, Column<T, Object>> primaryKeyForeignColumnMapping) {
					super(targetTable);
					this.elementColumn = elementColumn;
					this.sourceIdentifierAssembler = sourceIdentifierAssembler;
					this.primaryKeyForeignColumnMapping = (Map) primaryKeyForeignColumnMapping;
				}
				
				@Override
				public ElementRecord<TRGT, ID> assemble(Function<Column<?, ?>, Object> columnValueProvider) {
					ID leftValue = sourceIdentifierAssembler.assemble(sourceColumn -> {
						Column<T, Object> targetColumn = primaryKeyForeignColumnMapping.get(sourceColumn);
						return columnValueProvider.apply(targetColumn);
					});
					TRGT rightValue = (TRGT) columnValueProvider.apply(elementColumn);
					// we should not return an id if any (both expected in fact) value is null
					if (leftValue == null || rightValue == null) {
						return null;
					} else {
						return new ElementRecord<>(leftValue, rightValue);
					}
				}
				
				@Override
				public Map<Column<T, Object>, Object> getColumnValues(ElementRecord<TRGT, ID> id) {
					Map<Column<?, Object>, Object> sourceColumnValues = (Map) sourceIdentifierAssembler.getColumnValues(id.getId());
					Map<Column<T, Object>, Object> idColumnValues = Maps.innerJoin(primaryKeyForeignColumnMapping, sourceColumnValues);
					Map<Column<T, Object>, Object> result = new HashMap<>();
					result.put((Column<T, Object>) elementColumn, id.getElement());
					result.putAll(idColumnValues);
					return result;
				}
			}
			
			/**
			 * Identifier assembler for cases where user gave a configuration to persist embedded beans (default way is not used)
			 * 
			 * @param <TRGT> embedded bean type
			 * @param <ID> source identifier type
			 */
			private static class ConfiguredElementRecordIdentifierAssembler<TRGT, ID, T extends Table<T>> extends ComposedIdentifierAssembler<ElementRecord<TRGT, ID>, T> {
				
				private final EmbeddedClassMapping<ElementRecord<TRGT, ID>, T> mappingStrategy;
				private final IdentifierAssembler<ID, ?> sourceIdentifierAssembler;
				private final Map<Column<?, Object>, Column<T, Object>> primaryKeyForeignColumnMapping;
				
				private <LEFTTABLE extends Table<LEFTTABLE>> ConfiguredElementRecordIdentifierAssembler(
						T targetTable,
						EmbeddedClassMapping<ElementRecord<TRGT, ID>, T> mappingStrategy,
						IdentifierAssembler<ID, LEFTTABLE> sourceIdentifierAssembler,
						Map<Column<LEFTTABLE, Object>, Column<T, Object>> primaryKeyForeignColumnMapping) {
					super(targetTable);
					this.mappingStrategy = mappingStrategy;
					this.sourceIdentifierAssembler = sourceIdentifierAssembler;
					this.primaryKeyForeignColumnMapping = (Map) primaryKeyForeignColumnMapping;
				}
				
				@Override
				public ElementRecord<TRGT, ID> assemble(Row row, ColumnedRow rowAliaser) {
					return mappingStrategy.copyTransformerWithAliases(rowAliaser).transform(row);
				}
				
				@Override
				public ElementRecord<TRGT, ID> assemble(Function<Column<?, ?>, Object> columnValueProvider) {
					// never called because we override assemble(Row, ColumnedRow)
					return null;
				}
				
				@Override
				public Map<Column<T, Object>, Object> getColumnValues(ElementRecord<TRGT, ID> id) {
					Map<Column<?, Object>, Object> sourceColumnValues = (Map) sourceIdentifierAssembler.getColumnValues(id.getId());
					Map<Column<T, Object>, Object> idColumnValues = Maps.innerJoin(primaryKeyForeignColumnMapping, sourceColumnValues);
					Map<Column<T, Object>, Object> result = new HashMap<>();
					result.putAll(idColumnValues);
					result.putAll(mappingStrategy.getInsertValues(id));
					return result;
				}
			}
		}
	}
	
	/**
	 * Represents a line in table storage, acts as a wrapper of element collection with source bean identifier addition.
	 * 
	 * @param <TRGT> raw value type (element collection type)
	 * @param <ID> source bean identifier type
	 */
	private static class ElementRecord<TRGT, ID> {
		
		private static final PropertyAccessor<ElementRecord<Object, Object>, Object> IDENTIFIER_ACCESSOR = PropertyAccessor.fromMethodReference(
				ElementRecord::getId,
				ElementRecord::setId);
		
		private static final PropertyAccessor<ElementRecord<Object, Object>, Object> ELEMENT_ACCESSOR = PropertyAccessor.fromMethodReference(
				ElementRecord::getElement,
				ElementRecord::setElement);
		
		
		private ID id;
		private TRGT element;
		private boolean persisted = false;
		
		/**
		 * Default constructor for select instantiation
		 */
		public ElementRecord() {
		}
		
		public ElementRecord(ID id, TRGT element) {
			setId(id);
			setElement(element);
		}
		
		public boolean isNew() {
			return !persisted;
		}
		
		public boolean isPersisted() {
			return persisted;
		}

		public void markAsPersisted() {
			this.persisted = true;
		}
		
		public ElementRecord<TRGT, ID> setPersisted(boolean persisted) {
			this.persisted = persisted;
			return this;
		}
		
		public ID getId() {
			return id;
		}
		
		public void setId(ID id) {
			this.id = id;
			this.persisted = true;
		}
		
		public TRGT getElement() {
			return element;
		}
		
		public void setElement(TRGT element) {
			this.element = element;
		}
		
		/**
		 * Identifier for {@link CollectionDiffer} support (update use case), because it compares beans
		 * through their "footprint" which is their id in default/entity case, but since we are value type, we must provide a dedicated footprint.
		 * Could be hashCode() if it was implemented on identifier + element, but implementing it would require implementing equals() (to comply
		 * with best practices) which is not our case nor required by {@link CollectionDiffer}.
		 * Note : name of this method is not important
		 */
		public int footprint() {
			int result = id.hashCode();
			result = 31 * result + element.hashCode();
			return result;
		}
	}
	
	public class TargetInstancesInsertCascader extends AfterInsertCollectionCascader<SRC, ElementRecord<TRGT, ID>> {
		
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
			Collection<ElementRecord<TRGT, ID>> targets = collectionGetter.apply(source);
			// We only insert non-persisted instances (for logic and to prevent duplicate primary key error)
			return Iterables.stream(targets)
					.filter(getPersister()::isNew)
					.collect(Collectors.toList());
		}
	}
}
