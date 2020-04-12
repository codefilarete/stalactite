package org.gama.stalactite.persistence.engine.configurer;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.gama.lang.Duo;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.reflection.AccessorDefinition;
import org.gama.reflection.Accessors;
import org.gama.reflection.IAccessor;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.ElementCollectionTableNamingStrategy;
import org.gama.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.gama.stalactite.persistence.engine.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.IEntityPersister;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.builder.CollectionLinkage;
import org.gama.stalactite.persistence.engine.cascade.IJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.CollectionUpdater;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine.DeleteTargetEntitiesBeforeDeleteCascader;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine.TargetInstancesInsertCascader;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine.TargetInstancesUpdateCascader;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.id.assembly.ComposedIdentifierAssembler;
import org.gama.stalactite.persistence.id.diff.AbstractDiff;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.ComposedIdMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdAccessor;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.Nullable.nullable;
import static org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.ROOT_STRATEGY_NAME;

/**
 * @author Guillaume Mary
 */
public class ElementCollectionCascadeConfigurer<SRC, TRGT, ID, C extends Collection<TRGT>> {
	
	private final PersistenceContext persistenceContext;
	
	public ElementCollectionCascadeConfigurer(PersistenceContext persistenceContext) {
		this.persistenceContext = persistenceContext;
	}
	
	public <T extends Table<T>> void appendCascade(CollectionLinkage<SRC, TRGT, C> linkage,
												   IEntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister,
												   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
												   ColumnNamingStrategy columnNamingStrategy,
												   ElementCollectionTableNamingStrategy tableNamingStrategy) {
		
		AccessorDefinition collectionProviderDefinition = AccessorDefinition.giveDefinition(linkage.getCollectionProvider());
		// schema configuration
		Column sourcePK = Iterables.first((Set<Column>) sourcePersister.getMappingStrategy().getTargetTable().getPrimaryKey().getColumns());
		Table<?> targetTable = nullable(linkage.getTargetTable()).getOr(() -> new Table(tableNamingStrategy.giveName(collectionProviderDefinition)));
		Column idColumn = nullable(linkage.getForeignKeyColumn()).getOr(() ->
				targetTable.addColumn("id", sourcePK.getJavaType())
		);
		idColumn.primaryKey();
		Column<Table, TRGT> elementColumn = nullable(linkage.getForeignKeyColumn()).getOr(() ->
				(Column<Table, TRGT>) targetTable.addColumn(columnNamingStrategy.giveName(collectionProviderDefinition), linkage.getComponentType())
		);
		elementColumn.primaryKey();
		targetTable.addForeignKey(foreignKeyNamingStrategy::giveName, idColumn, sourcePK);
		
		ClassMappingStrategy<ElementRecord, ElementRecord, Table> wrapperStrategy = new ElementRecordMappingStrategy(targetTable, idColumn, elementColumn);
		
		// Note that table will be added to schema thanks to select cascade because join is added to source persister
		JoinedTablesPersister<ElementRecord, ElementRecord, Table> wrapperPersister = new JoinedTablesPersister<>(persistenceContext, wrapperStrategy);
		
		// insert management
		IAccessor<SRC, C> collectionAccessor = linkage.getCollectionProvider();
		addInsertCascade(sourcePersister, wrapperPersister, collectionAccessor);
		
		// update management
		addUpdateCascade(sourcePersister, wrapperPersister, collectionAccessor);
		
		// delete management (we provided persisted instances so they are perceived as deletable)
		addDeleteCascade(sourcePersister, wrapperPersister, collectionAccessor);
		
		// select management
		addSelectCascade(sourcePersister, wrapperPersister, sourcePK, idColumn, linkage.getCollectionProvider().toMutator()::set, collectionAccessor::get, (Class<C>) collectionProviderDefinition.getMemberType());
	}
	
	private void addInsertCascade(IEntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister,
								  IEntityPersister<ElementRecord, ElementRecord> wrapperPersister,
								  IAccessor<SRC, C> collectionAccessor) {
		Function<SRC, Collection<ElementRecord>> collectionProviderForInsert = collectionProvider(
				collectionAccessor,
				sourcePersister.getMappingStrategy(),
				PersistableIdentifier::new);
		
		sourcePersister.addInsertListener(new TargetInstancesInsertCascader<>(wrapperPersister, collectionProviderForInsert));
	}
	
	private void addUpdateCascade(IEntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister,
								  IEntityPersister<ElementRecord, ElementRecord> wrapperPersister,
								  IAccessor<SRC, C> collectionAccessor) {
		Function<SRC, Collection<ElementRecord>> collectionProviderAsPersistedInstances = collectionProvider(
				collectionAccessor,
				sourcePersister.getMappingStrategy(),
				PersistedIdentifier::new);
		
		BiConsumer<Duo<SRC, SRC>, Boolean> updateListener = new CollectionUpdater<SRC, ElementRecord, Collection<ElementRecord>>(
				collectionProviderAsPersistedInstances,
				wrapperPersister,
				(o, i) -> { /* no reverse setter because we store only raw values */ },
				true,
				// we base our id policy on a particular identifier because Id is all the same for ElementCollection (it is source bean id)
				ElementRecord::footprint) {
			
			/**
			 * Overriden to avoid no insertion of persisted instances (isNew() = true) : we want insert of not-new instance because we declared
			 * collection provider as a {@link PersistedIdentifier} hence added instance are not considered new, which is wanted as such for
			 * collection removal (because they are persisted they can be removed from database)
			 */
			@Override
			protected void onAddedTarget(UpdateContext updateContext, AbstractDiff<ElementRecord> diff) {
				updateContext.getEntitiesToBeInserted().add(diff.getReplacingInstance());
			}
		};
		
		sourcePersister.addUpdateListener(new TargetInstancesUpdateCascader<>(wrapperPersister, updateListener));
	}
	
	private void addDeleteCascade(IEntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister,
								  IEntityPersister<ElementRecord, ElementRecord> wrapperPersister,
								  IAccessor<SRC, C> collectionAccessor) {
		Function<SRC, Collection<ElementRecord>> collectionProviderAsPersistedInstances = collectionProvider(
				collectionAccessor,
				sourcePersister.getMappingStrategy(),
				PersistedIdentifier::new);
		sourcePersister.addDeleteListener(new DeleteTargetEntitiesBeforeDeleteCascader<>(wrapperPersister, collectionProviderAsPersistedInstances));
	}
	
	private void addSelectCascade(IEntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister,
								  IJoinedTablesPersister<ElementRecord, ElementRecord> wrapperPersister,
								  Column sourcePK,
								  Column primaryKey,
								  BiConsumer<SRC, C> collectionSetter,
								  Function<SRC, C> collectionGetter,
								  Class<C> collectionType) {
		// a particular collection fixer that gets raw values (elements) from ElementRecord
		// because wrapperPersister manages ElementRecord, so it gives them as input of the relation,
		// hence an adaption is needed to "convert" it
		BeanRelationFixer<SRC, ElementRecord> srcElementWrapperAsEntityBeanRelationFixer = BeanRelationFixer.ofAdapter(
				collectionSetter,
				collectionGetter,
				BeanRelationFixer.giveCollectionFactory(collectionType),
				(bean, input, collection) -> collection.add((TRGT) input.getElement()));	// element value is taken from ElementRecord
		
		wrapperPersister.joinAsMany(sourcePersister, sourcePK, primaryKey, srcElementWrapperAsEntityBeanRelationFixer, ROOT_STRATEGY_NAME, true);
	}
	
	private Function<SRC, Collection<ElementRecord>> collectionProvider(IAccessor<SRC, C> collectionAccessor,
																		IdAccessor<SRC, ID> idAccessor,
																		Function<ID, StatefullIdentifier> idWrapper) {
		return src -> Iterables.collect(collectionAccessor.get(src),
						trgt -> new ElementRecord<>(idWrapper.apply(idAccessor.getId(src)), trgt), HashSet::new);
	}
	
	/**
	 * Mapping strategy dedicated to {@link ElementRecord}. Very close to {@link org.gama.stalactite.persistence.engine.AssociationRecordMappingStrategy}
	 * in its principle.
	 * 
	 */
	private static class ElementRecordMappingStrategy extends ClassMappingStrategy<ElementRecord, ElementRecord, Table> {
		private ElementRecordMappingStrategy(Table targetTable, Column idColumn, Column elementColumn) {
			super(ElementRecord.class, targetTable, (Map) Maps
							.forHashMap(IReversibleAccessor.class, Column.class)
							.add(ElementRecord.IDENTIFIER_ACCESSOR, idColumn)
							.add(ElementRecord.ELEMENT_ACCESSOR, elementColumn),
					new ElementRecordIdMappingStrategy(targetTable, idColumn, elementColumn));
		}
		
		/**
		 * {@link org.gama.stalactite.persistence.mapping.IdMappingStrategy} for {@link ElementRecord} : a composed id made of
		 * {@link ElementRecord#getIdentifier()} and {@link ElementRecord#getElement()}
		 */
		private static class ElementRecordIdMappingStrategy extends ComposedIdMappingStrategy<ElementRecord, ElementRecord> {
			public ElementRecordIdMappingStrategy(Table targetTable, Column idColumn, Column elementColumn) {
				super(new ElementRecordIdAccessor(),
						new AlreadyAssignedIdentifierManager<>(ElementRecord.class),
						new ElementRecordIdentifierAssembler(targetTable, idColumn, elementColumn));
			}
			
			/**
			 * Overriden because {@link ComposedIdMappingStrategy} doest not support {@link StatefullIdentifier} : super implementation is based
			 * on {@link ElementRecord#getIdentifier()} == null which is always false on {@link ElementRecord}  
			 * 
			 * @param entity any non null entity
			 * @return true or false based on {@link ElementRecord#isNew()}
			 */
			@Override
			public boolean isNew(@Nonnull ElementRecord entity) {
				return entity.isNew();
			}
			
			private static class ElementRecordIdAccessor implements IdAccessor<ElementRecord, ElementRecord> {
					@Override
					public ElementRecord getId(ElementRecord associationRecord) {
						return associationRecord;
					}
					
					@Override
					public void setId(ElementRecord associationRecord, ElementRecord identifier) {
						associationRecord.setIdentifier(identifier.getIdentifier());
						associationRecord.setElement(identifier.getElement());
					}
			}
			
			private static class ElementRecordIdentifierAssembler extends ComposedIdentifierAssembler<ElementRecord> {
				
				private final Column idColumn;
				private final Column elementColumn;
				
				private ElementRecordIdentifierAssembler(Table targetTable, Column idColumn, Column elementColumn) {
					super(targetTable);
					this.idColumn = idColumn;
					this.elementColumn = elementColumn;
				}
				
				@Override
				protected ElementRecord assemble(Map<Column, Object> primaryKeyElements) {
					Object leftValue = primaryKeyElements.get(idColumn);
					Object rightValue = primaryKeyElements.get(elementColumn);
					// we should not return an id if any (both expected in fact) value is null
					if (leftValue == null || rightValue == null) {
						return null;
					} else {
						return new ElementRecord(new PersistedIdentifier(leftValue), rightValue);
					}
				}
				
				@Override
				public Map<Column, Object> getColumnValues(@Nonnull ElementRecord id) {
					return Maps.asMap(idColumn, id.getIdentifier())
							.add(elementColumn, id.getElement());
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
		
		private static final PropertyAccessor<ElementRecord<Object, Object>, Object> IDENTIFIER_ACCESSOR = new PropertyAccessor<>(
				Accessors.accessorByMethodReference(ElementRecord::getIdentifier),
				Accessors.mutatorByMethodReference(ElementRecord::setIdentifier));
		
		private static final PropertyAccessor<ElementRecord<Object, Object>, Object> ELEMENT_ACCESSOR = new PropertyAccessor<>(
				Accessors.accessorByMethodReference(ElementRecord::getElement),
				Accessors.mutatorByMethodReference(ElementRecord::setElement));
		
		
		private StatefullIdentifier<ID> identifier;
		private TRGT element;
		
		/**
		 * Default constructor for select instanciation
		 */
		public ElementRecord() {
		}
		
		public ElementRecord(StatefullIdentifier<ID> identifier, TRGT element) {
			this.identifier = identifier;
			this.element = element;
		}
		
		public boolean isNew() {
			return !this.identifier.isPersisted();
		}
		
		public ID getIdentifier() {
			return identifier.getSurrogate();
		}
		
		public void setIdentifier(ID identifier) {
			this.identifier = new PersistedIdentifier<ID>(identifier);
		}
		
		public TRGT getElement() {
			return element;
		}
		
		public void setElement(TRGT element) {
			this.element = element;
		}
		
		/**
		 * Identifier for {@link org.gama.stalactite.persistence.id.diff.CollectionDiffer} support (update use case), because it compares beans
		 * through their "foot print" which is their id in default/entity case, but since we are value type, we must provide a dedicated foot print.
		 * Could be hashCode() if it was implemented on identifier + element, but implementing it would require to implement equals() (to comply
		 * with best pratices) which is not our case nor required by {@link org.gama.stalactite.persistence.id.diff.CollectionDiffer}.
		 * Note : name of this method is not important
		 */
		public int footprint() {
			int result = identifier.getSurrogate().hashCode();
			result = 31 * result + element.hashCode();
			return result;
		}
	}
}
