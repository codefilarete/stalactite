package org.codefilarete.stalactite.engine.configurer;

import javax.annotation.Nonnull;
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
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfiguration.Linkage;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.codefilarete.stalactite.engine.diff.AbstractDiff;
import org.codefilarete.stalactite.engine.diff.CollectionDiffer;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater;
import org.codefilarete.stalactite.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.codefilarete.stalactite.engine.runtime.OneToManyWithMappedAssociationEngine.DeleteTargetEntitiesBeforeDeleteCascader;
import org.codefilarete.stalactite.engine.runtime.OneToManyWithMappedAssociationEngine.TargetInstancesInsertCascader;
import org.codefilarete.stalactite.engine.runtime.OneToManyWithMappedAssociationEngine.TargetInstancesUpdateCascader;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.mapping.EmbeddedClassMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;

import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.bean.Objects.preventNull;

/**
 * Class that configures element-collection mapping
 * 
 * @author Guillaume Mary
 */
public class ElementCollectionRelationConfigurer<SRC, TRGT, ID, C extends Collection<TRGT>> {
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public ElementCollectionRelationConfigurer(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	public <T extends Table, TARGET_TABLE extends Table<?>> void configure(ElementCollectionRelation<SRC, TRGT, C> linkage,
																		   EntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister,
																		   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
																		   ColumnNamingStrategy columnNamingStrategy,
																		   ElementCollectionTableNamingStrategy tableNamingStrategy) {
		
		AccessorDefinition collectionProviderDefinition = AccessorDefinition.giveDefinition(linkage.getCollectionProvider());
		// schema configuration
		Column<T, ID> sourcePK = Iterables.first((Set<Column>) sourcePersister.getMapping().getTargetTable().getPrimaryKey().getColumns());
		
		String tableName = nullable(linkage.getTargetTableName()).getOr(() -> tableNamingStrategy.giveName(collectionProviderDefinition));
		TARGET_TABLE targetTable = (TARGET_TABLE) nullable(linkage.getTargetTable()).getOr(() -> new Table(tableName));
		
		String reverseColumnName = nullable(linkage.getReverseColumnName()).getOr(() ->
				columnNamingStrategy.giveName(AccessorDefinition.giveDefinition(new AccessorByMethodReference<>(ElementRecord<TRGT, ID>::getId))));
		Column<TARGET_TABLE, ID> reverseColumn = (Column<TARGET_TABLE, ID>) nullable(linkage.getReverseColumn())
				.getOr(() -> (Column) targetTable.addColumn(reverseColumnName, sourcePK.getJavaType()));
		registerColumnBinder(reverseColumn, sourcePK);	// because sourcePk binder might have been overloaded by column so we need to adjust to it
		
		reverseColumn.primaryKey();
		
		EmbeddableMappingConfiguration<TRGT> embeddableConfiguration =
				nullable(linkage.getEmbeddableConfigurationProvider()).map(EmbeddableMappingConfigurationProvider::getConfiguration).get();
		ClassMapping<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>, TARGET_TABLE> elementRecordStrategy;
		if (embeddableConfiguration == null) {
			String columnName = nullable(linkage.getElementColumnName())
					.getOr(() -> columnNamingStrategy.giveName(collectionProviderDefinition));
			Column<TARGET_TABLE, TRGT> elementColumn = (Column<TARGET_TABLE, TRGT>) targetTable.addColumn(columnName, linkage.getComponentType());
			elementColumn.primaryKey();
			targetTable.addForeignKey(foreignKeyNamingStrategy::giveName, (Column) reverseColumn, (Column) sourcePK);
			
			elementRecordStrategy = new ElementRecordMapping<>(targetTable, reverseColumn, elementColumn);
		} else {
			// a special configuration was given, we compute a EmbeddedClassMapping from it
			BeanMappingBuilder elementCollectionMappingBuilder = new BeanMappingBuilder();
			Map<ReversibleAccessor, Column> columnMap = elementCollectionMappingBuilder.build(embeddableConfiguration, targetTable,
					dialect.getColumnBinderRegistry(), new ColumnNameProvider(columnNamingStrategy) {
						@Override
						protected String giveColumnName(Linkage pawn) {
							return nullable(linkage.getOverriddenColumnNames().get(pawn.getAccessor()))
									.getOr(() -> super.giveColumnName(pawn));
						}
					});
			
			Map<ReversibleAccessor, Column> projectedColumnMap = new HashMap<>();
			columnMap.forEach((k, v) -> {
				AccessorChain accessorChain = AccessorChain.forModel(Arrays.asList(ElementRecord.ELEMENT_ACCESSOR, k), (accessor, valueType) -> {
					if (accessor == ElementRecord.ELEMENT_ACCESSOR) {
						// on getElement(), bean type can't be deduced by reflection due to generic type erasure : default mechanism returns Object
						// so we have to specify our bean type, else a simple Object is instantiated which throws a ClassCastException further
						return embeddableConfiguration.getBeanType();
					} else {
						// default mechanism
						return ValueInitializerOnNullValue.giveValueType(accessor, valueType);
					}
				});
				
				projectedColumnMap.put(accessorChain, v);
				v.primaryKey();
			});
			
			EmbeddedClassMapping<ElementRecord<TRGT, ID>, TARGET_TABLE> embeddedClassMappingStrategy = new EmbeddedClassMapping<>(ElementRecord.class,
																																  targetTable, (Map) projectedColumnMap);
			elementRecordStrategy = new ElementRecordMapping<>(targetTable, reverseColumn, embeddedClassMappingStrategy);
		}
			
		// Note that table will be added to schema thanks to select cascade because join is added to source persister
		SimpleRelationalEntityPersister<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>, TARGET_TABLE> elementRecordPersister =
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
		addSelectCascade(sourcePersister, elementRecordPersister, sourcePK, reverseColumn,
				linkage.getCollectionProvider().toMutator()::set, collectionAccessor::get,
				collectionFactory);
	}
	
	private void registerColumnBinder(Column reverseColumn, Column sourcePK) {
		dialect.getColumnBinderRegistry().register(reverseColumn, dialect.getColumnBinderRegistry().getBinder(sourcePK));
		dialect.getSqlTypeRegistry().put(reverseColumn, dialect.getSqlTypeRegistry().getTypeName(sourcePK));
	}
	
	private void addInsertCascade(EntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister,
								  EntityPersister<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>> wrapperPersister,
								  Accessor<SRC, C> collectionAccessor) {
		Function<SRC, Collection<ElementRecord<TRGT, ID>>> collectionProviderForInsert = collectionProvider(
			collectionAccessor,
			sourcePersister.getMapping(),
			false);
		
		sourcePersister.addInsertListener(new TargetInstancesInsertCascader<>(wrapperPersister, collectionProviderForInsert));
	}
	
	private void addUpdateCascade(EntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister,
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
	
	private void addDeleteCascade(EntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister,
								  EntityPersister<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>> wrapperPersister,
								  Accessor<SRC, C> collectionAccessor) {
		Function<SRC, Collection<ElementRecord<TRGT, ID>>> collectionProviderAsPersistedInstances = collectionProvider(
			collectionAccessor,
			sourcePersister.getMapping(),
			true);
		
		sourcePersister.addDeleteListener(new DeleteTargetEntitiesBeforeDeleteCascader<>(wrapperPersister, collectionProviderAsPersistedInstances));
	}
	
	private void addSelectCascade(EntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister,
								  RelationalEntityPersister<ElementRecord<TRGT, ID>, ElementRecord<TRGT, ID>> elementRecordPersister,
								  Column sourcePK,
								  Column elementRecordToSourceForeignKey,
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
	private static class ElementRecordMapping<C, I, T extends Table> extends ClassMapping<ElementRecord<C, I>, ElementRecord<C, I>, T> {
		
		private ElementRecordMapping(T targetTable, Column<T, I> idColumn, Column<T, C> elementColumn) {
			super((Class) ElementRecord.class,
				  targetTable,
				  (Map) Maps.forHashMap(ReversibleAccessor.class, Column.class)
							.add(ElementRecord.IDENTIFIER_ACCESSOR, idColumn)
							.add(ElementRecord.ELEMENT_ACCESSOR, elementColumn),
					new ElementRecordIdMapping<>(targetTable, idColumn, elementColumn));
		}
		
		private ElementRecordMapping(T targetTable, Column<T, I> idColumn, EmbeddedClassMapping<ElementRecord<C, I>, T> embeddableMapping) {
			super((Class) ElementRecord.class,
				  targetTable,
				  (Map) Maps.putAll(Maps.forHashMap(ReversibleAccessor.class, Column.class)
							.add(ElementRecord.IDENTIFIER_ACCESSOR, idColumn),
							embeddableMapping.getPropertyToColumn()),
					new ElementRecordIdMapping<>(targetTable, idColumn, embeddableMapping));
		}
		
		/**
		 * {@link IdMapping} for {@link ElementRecord} : a composed id made of
		 * {@link ElementRecord#getId()} and {@link ElementRecord#getElement()}
		 */
		private static class ElementRecordIdMapping<C, I, T extends Table> extends ComposedIdMapping<ElementRecord<C, I>, ElementRecord<C, I>> {
			
			public ElementRecordIdMapping(T targetTable, Column<T, I> idColumn, Column<T, C> elementColumn) {
				super(new ElementRecordIdAccessor<>(),
						new AlreadyAssignedIdentifierManager<>((Class<ElementRecord<C, I>>) (Class) ElementRecord.class,
															   ElementRecord::markAsPersisted,
															   ElementRecord::isPersisted),
						new DefaultElementRecordIdentifierAssembler<>(targetTable, idColumn, elementColumn));
			}
			
			public ElementRecordIdMapping(T targetTable, Column<T, I> idColumn, EmbeddedClassMapping<ElementRecord<C, I>, T> elementColumn) {
				super(new ElementRecordIdAccessor<>(),
						new AlreadyAssignedIdentifierManager<>((Class<ElementRecord<C, I>>) (Class) ElementRecord.class,
															   ElementRecord::markAsPersisted,
															   ElementRecord::isPersisted),
						new ConfiguredElementRecordIdentifierAssembler<>(targetTable, idColumn, elementColumn));
			}
			
			/**
			 * Override because {@link ComposedIdMapping} is based on null identifier to determine newness, which is always false for {@link ElementRecord}
			 * because they always have one. We delegate its computation to the entity.
			 * 
			 * @param entity any non-null entity
			 * @return true or false based on {@link ElementRecord#isNew()}
			 */
			@Override
			public boolean isNew(@Nonnull ElementRecord<C, I> entity) {
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
			 * - identifier is saved in idColumn 
			 * - element value is saved in elementColumn 
			 * 
			 * @param <TRGT> embedded bean type
			 * @param <ID> source identifier type
			 */
			private static class DefaultElementRecordIdentifierAssembler<TRGT, ID> extends ComposedIdentifierAssembler<ElementRecord<TRGT, ID>> {
				
				private final Column<?, ID> idColumn;
				private final Column<?, TRGT> elementColumn;
				
				private <T extends Table> DefaultElementRecordIdentifierAssembler(T targetTable,
																				  Column<?, ID> idColumn,
																				  Column<T, TRGT> elementColumn) {
					super(targetTable);
					this.idColumn = idColumn;
					this.elementColumn = elementColumn;
				}
				
				@Override
				protected ElementRecord<TRGT, ID> assemble(Map<Column, Object> primaryKeyElements) {
					ID leftValue = (ID) primaryKeyElements.get(idColumn);
					TRGT rightValue = (TRGT) primaryKeyElements.get(elementColumn);
					// we should not return an id if any (both expected in fact) value is null
					if (leftValue == null || rightValue == null) {
						return null;
					} else {
						return new ElementRecord<>(leftValue, rightValue);
					}
				}
				
				@Override
				public Map<Column, Object> getColumnValues(@Nonnull ElementRecord id) {
					return Maps.forHashMap(Column.class, Object.class)
						.add(idColumn, id.getId())
						.add(elementColumn, id.getElement());
				}
			}
			
			/**
			 * Identifier assembler for cases where user gave a configuration to persist embedded beans (default way is not used)
			 * 
			 * @param <TRGT> embedded bean type
			 * @param <ID> source identifier type
			 */
			private static class ConfiguredElementRecordIdentifierAssembler<TRGT, ID> extends ComposedIdentifierAssembler<ElementRecord<TRGT, ID>> {
				
				private final Column<?, ID> idColumn;
				private final EmbeddedClassMapping<ElementRecord<TRGT, ID>, ?> mappingStrategy;
				
				private <T extends Table> ConfiguredElementRecordIdentifierAssembler(T targetTable,
																					 Column<T, ID> idColumn,
																					 EmbeddedClassMapping<ElementRecord<TRGT, ID>, T> mappingStrategy) {
					super(targetTable);
					this.idColumn = idColumn;
					this.mappingStrategy = mappingStrategy;
				}
				
				@Override
				public ElementRecord<TRGT, ID> assemble(@Nonnull Row row, @Nonnull ColumnedRow rowAliaser) {
					ID leftValue = (ID) rowAliaser.getValue(idColumn, row);
					TRGT rightValue = (TRGT) mappingStrategy.getRowTransformer().copyWithAliases(rowAliaser).transform(row);
					// we should not return an id if any (both expected in fact) value is null
					if (leftValue == null || rightValue == null) {
						return null;
					} else {
						return new ElementRecord<>(leftValue, rightValue);
					}
				}
				
				@Override
				protected ElementRecord<TRGT, ID> assemble(Map<Column, Object> primaryKeyElements) {
					// never called because we override assemble(Row, ColumnedRow)
					return null;
				}
				
				@Override
				public Map<Column, Object> getColumnValues(@Nonnull ElementRecord id) {
					return Maps.putAll(Maps.asMap(idColumn, id.getId()), mappingStrategy.getInsertValues(id));
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
}
