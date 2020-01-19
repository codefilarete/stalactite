package org.gama.stalactite.persistence.engine;

import javax.annotation.Nullable;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Reflections;
import org.gama.lang.StringAppender;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.AccessorByMember;
import org.gama.reflection.AccessorByMethod;
import org.gama.reflection.Accessors;
import org.gama.reflection.IMutator;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MemberDefinition;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.MutatorByMethod;
import org.gama.reflection.PropertyAccessor;
import org.gama.reflection.ValueAccessPointMap;
import org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.builder.CascadeManyList;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl;
import org.gama.stalactite.persistence.engine.runtime.AbstractOneToManyWithAssociationTableEngine;
import org.gama.stalactite.persistence.engine.runtime.IndexedMappedManyRelationDescriptor;
import org.gama.stalactite.persistence.engine.runtime.ManyRelationDescriptor;
import org.gama.stalactite.persistence.engine.runtime.MappedManyRelationDescriptor;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithAssociationTableEngine;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithIndexedAssociationTableEngine;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithIndexedMappedAssociationEngine;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdAccessor;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.IConnectionConfiguration;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.PrimaryKey;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.Nullable.nullable;
import static org.gama.lang.collection.Iterables.first;
import static org.gama.reflection.Accessors.accessor;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode.ASSOCIATION_ONLY;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode.READ_ONLY;

/**
 * @param <SRC> type of input (left/source entities)
 * @param <TRGT> type of output (right/target entities)
 * @param <ID> identifier type of source entities
 * @param <ID> identifier type of target entities
 * @param <C> collection type of the relation
 * @author Guillaume Mary
 */
public class CascadeManyConfigurer<SRC, TRGT, ID, C extends Collection<TRGT>> {
	
	private final PersistenceContext persistenceContext;
	private final PersisterBuilderImpl<TRGT, ID> persisterBuilder;
	
	public CascadeManyConfigurer(PersistenceContext persistenceContext, PersisterBuilderImpl<TRGT, ID> persisterBuilder) {
		this.persistenceContext = persistenceContext;
		this.persisterBuilder = persisterBuilder;
	}
	
	public <T extends Table<T>> void appendCascade(CascadeMany<SRC, TRGT, ID, C> cascadeMany,
												   IEntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister,
												   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
												   ColumnNamingStrategy joinColumnNamingStrategy,
												   AssociationTableNamingStrategy associationTableNamingStrategy) {
		Table reverseTable = nullable(cascadeMany.getReverseColumn()).map(Column::getTable).get();
		Table indexingTable = nullable((Column<?, ?>) (cascadeMany instanceof CascadeManyList ? ((CascadeManyList) cascadeMany).getIndexingColumn() : null)).map(Column::getTable).get();
		Set<Table> availableTables = Arrays.asHashSet(cascadeMany.getTargetTable(), reverseTable, indexingTable);
		availableTables.remove(null);
		if (availableTables.size() > 1) {
			throw new MappingConfigurationException("Different tables used for configuring mapping : " + new StringAppender() {
				@Override
				public StringAppender cat(Object table) {
					return super.cat(((Table) table).getName());
				}
			}.ccat(availableTables, ", "));
		}
		
		// please note that even if no table is found in configuration, build(..) will create one
		Table targetTable = nullable(cascadeMany.getTargetTable()).elseSet(reverseTable).elseSet(indexingTable).get();
		IEntityConfiguredJoinedTablesPersister<TRGT, ID> targetPersister = (IEntityConfiguredJoinedTablesPersister<TRGT, ID>) this.persisterBuilder
				.build(persistenceContext, targetTable);
		
		// finding joined columns: left one is primary key. Right one is given by the target strategy through the property accessor
		if (sourcePersister.getMappingStrategy().getTargetTable().getPrimaryKey().getColumns().size() > 1) {
			throw new NotYetSupportedOperationException("Joining tables on a composed primary key is not (yet) supported");
		}
		Column leftPrimaryKey = (Column) first(sourcePersister.getMappingStrategy().getTargetTable().getPrimaryKey().getColumns());
		
		RelationMode maintenanceMode = cascadeMany.getRelationMode();
		// selection is always present (else configuration is nonsense !)
		boolean orphanRemoval = maintenanceMode == ALL_ORPHAN_REMOVAL;
		boolean writeAuthorized = maintenanceMode != READ_ONLY;
		
		ManyAssociationConfiguration<SRC, TRGT, ID, C> manyAssociationConfiguration = new ManyAssociationConfiguration<>(cascadeMany,
				sourcePersister, targetPersister, leftPrimaryKey, foreignKeyNamingStrategy, joinColumnNamingStrategy,
				orphanRemoval, writeAuthorized);
		if (cascadeMany.isOwnedByReverseSide()) {
			// case : reverse property is defined through one of the setter, getter or column on the reverse side
			if (maintenanceMode == ASSOCIATION_ONLY) {
				throw new MappingConfigurationException(RelationMode.ASSOCIATION_ONLY + " is only relevent with an association table");
			}
			new CascadeManyWithMappedAssociationConfigurer<>(manyAssociationConfiguration, orphanRemoval).configure();
		} else {
			new CascadeManyWithAssociationTableConfigurer<>(manyAssociationConfiguration,
					associationTableNamingStrategy,
					persistenceContext.getDialect(),
					maintenanceMode == ASSOCIATION_ONLY,
					persistenceContext.getConnectionConfiguration())
					.configure();
		}
	}
	
	/**
	 * Class that stores elements necessary to one-to-many association configuration
	 */
	private static class ManyAssociationConfiguration<SRC, TRGT, ID, C extends Collection<TRGT>> {
		
		private final CascadeMany<SRC, TRGT, ID, C> cascadeMany;
		private final IEntityConfiguredJoinedTablesPersister<SRC, ID> srcPersister;
		private final IEntityConfiguredJoinedTablesPersister<TRGT, ID> targetPersister;
		private final Column leftPrimaryKey;
		private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
		private final ColumnNamingStrategy joinColumnNamingStrategy;
		private final IReversibleAccessor<SRC, C> collectionGetter;
		private final IMutator<SRC, C> setter;
		private final boolean orphanRemoval;
		private final boolean writeAuthorized;
		private final MemberDefinition memberDefinition;
		private IMutator<TRGT, SRC> reversePropertySetter;
		
		private ManyAssociationConfiguration(CascadeMany<SRC, TRGT, ID, C> cascadeMany,
											 IEntityConfiguredJoinedTablesPersister<SRC, ID> srcPersister,
											 IEntityConfiguredJoinedTablesPersister<TRGT, ID> targetPersister,
											 Column leftPrimaryKey,
											 ForeignKeyNamingStrategy foreignKeyNamingStrategy,
											 ColumnNamingStrategy joinColumnNamingStrategy,
											 boolean orphanRemoval,
											 boolean writeAuthorized) {
			this.cascadeMany = cascadeMany;
			this.srcPersister = srcPersister;
			this.targetPersister = targetPersister;
			this.leftPrimaryKey = leftPrimaryKey;
			this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
			this.joinColumnNamingStrategy = joinColumnNamingStrategy;
			this.collectionGetter = cascadeMany.getCollectionProvider();
			this.setter = collectionGetter.toMutator();
			// we don't use MemberDefinition.giveMemberDefinition(..) because it gives a cross-member definition, loosing get/set for example,
			// whereas we need this information to build better association table name
			this.memberDefinition = new MemberDefinition(
					cascadeMany.getMethodReference().getDeclaringClass(),
					cascadeMany.getMethodReference().getMethodName(),
					cascadeMany.getMethodReference().getPropertyType());
			this.orphanRemoval = orphanRemoval;
			this.writeAuthorized = writeAuthorized;
		}
		
		/**
		 * Gives the collection factory used to instanciate relation field.
		 * 
		 * @return the one given by {@link CascadeMany#getCollectionFactory()} or one deduced from member signature
		 */
		protected Supplier<C> giveCollectionFactory() {
			Supplier<C> collectionFactory = cascadeMany.getCollectionFactory();
			if (collectionFactory == null) {
				collectionFactory = () -> Reflections.newInstance(giveInstanciationType());
			}
			return collectionFactory;
		}
		
		/**
		 * Expected to give concrete class to be instanciated.
		 * @return a concrete and instanciable type compatible with acccessor input type
		 */
		protected Class<C> giveInstanciationType() {
			if (List.class.equals(memberDefinition.getMemberType())) {
				return (Class<C>) (Class) ArrayList.class;
			} else if (Set.class.equals(memberDefinition.getMemberType())) {
				return (Class<C>) (Class) HashSet.class;
			} else {
				return memberDefinition.getMemberType();
			}
		}
	}
	
	/**
	 * Configurer dedicated to association that needs an intermediary table between source entities and these of the relation
	 */
	private static class CascadeManyWithAssociationTableConfigurer<SRC, TRGT, ID, C extends Collection<TRGT>> {
		
		private final AssociationTableNamingStrategy associationTableNamingStrategy;
		private final Dialect dialect;
		private final ManyAssociationConfiguration<SRC, TRGT, ID, C> manyAssociationConfiguration;
		private final boolean maintainAssociationOnly;
		private final IConnectionConfiguration connectionConfiguration;
		
		private CascadeManyWithAssociationTableConfigurer(ManyAssociationConfiguration<SRC, TRGT, ID, C> manyAssociationConfiguration,
														  AssociationTableNamingStrategy associationTableNamingStrategy,
														  Dialect dialect,
														  boolean maintainAssociationOnly,
														  IConnectionConfiguration connectionConfiguration) {
			this.manyAssociationConfiguration = manyAssociationConfiguration;
			this.associationTableNamingStrategy = associationTableNamingStrategy;
			this.dialect = dialect;
			this.maintainAssociationOnly = maintainAssociationOnly;
			this.connectionConfiguration = connectionConfiguration;
		}
		
		private void configure() {
			// case : Collection mapping without reverse property : an association table is needed
			Table<?> rightTable = manyAssociationConfiguration.targetPersister.getMappingStrategy().getTargetTable();
			Column rightPrimaryKey = first(rightTable.getPrimaryKey().getColumns());
			
			String associationTableName = associationTableNamingStrategy.giveName(manyAssociationConfiguration.memberDefinition,
					manyAssociationConfiguration.leftPrimaryKey, rightPrimaryKey);
			AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, ID, C, ? extends AssociationRecord, ? extends AssociationTable> oneToManyWithAssociationTableEngine;
			ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor = new ManyRelationDescriptor<>(
					manyAssociationConfiguration.collectionGetter::get, manyAssociationConfiguration.setter::set,
					manyAssociationConfiguration.giveCollectionFactory(),
					manyAssociationConfiguration.reversePropertySetter == null ? null : manyAssociationConfiguration.reversePropertySetter::set);
			if (manyAssociationConfiguration.cascadeMany instanceof CascadeManyList) {
				oneToManyWithAssociationTableEngine = configureIndexedAssociation(rightPrimaryKey, associationTableName, manyRelationDescriptor);
			} else {
				oneToManyWithAssociationTableEngine = configureNonIndexedAssociation(rightPrimaryKey, associationTableName, manyRelationDescriptor);
			}
			
			oneToManyWithAssociationTableEngine.addSelectCascade(manyAssociationConfiguration.srcPersister);
			if (manyAssociationConfiguration.writeAuthorized) {
				oneToManyWithAssociationTableEngine.addInsertCascade(maintainAssociationOnly);
				oneToManyWithAssociationTableEngine.addUpdateCascade(manyAssociationConfiguration.orphanRemoval, maintainAssociationOnly);
				oneToManyWithAssociationTableEngine.addDeleteCascade(manyAssociationConfiguration.orphanRemoval, dialect.getColumnBinderRegistry());
			}
		}
		
		private AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, ID, C, ? extends AssociationRecord, ? extends AssociationTable>
		configureNonIndexedAssociation(Column rightPrimaryKey, String associationTableName, ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor) {
			
			AssociationTable intermediaryTable = new AssociationTable(null,
					associationTableName,
					manyAssociationConfiguration.leftPrimaryKey,
					rightPrimaryKey,
					associationTableNamingStrategy,
					manyAssociationConfiguration.foreignKeyNamingStrategy);
			AssociationRecordPersister<AssociationRecord, AssociationTable> associationPersister = new AssociationRecordPersister<>(
					new AssociationRecordMappingStrategy(intermediaryTable),
					dialect,
					connectionConfiguration);
			return new OneToManyWithAssociationTableEngine<>(
					manyAssociationConfiguration.srcPersister,
					manyAssociationConfiguration.targetPersister,
					manyRelationDescriptor,
					associationPersister);
		}
		
		private AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, ID, C, ? extends AssociationRecord, ? extends AssociationTable>
		configureIndexedAssociation(Column rightPrimaryKey, String associationTableName, ManyRelationDescriptor manyRelationDescriptor) {
			
			if (((CascadeManyList) manyAssociationConfiguration.cascadeMany).getIndexingColumn() != null) {
				throw new UnsupportedOperationException("Indexing column is defined without owner : relation is only declared by "
						+ MemberDefinition.toString(manyAssociationConfiguration.collectionGetter));
			}
			
			// NB: index column is part of the primary key
			IndexedAssociationTable intermediaryTable = new IndexedAssociationTable(null,
					associationTableName,
					manyAssociationConfiguration.leftPrimaryKey,
					rightPrimaryKey,
					associationTableNamingStrategy,
					manyAssociationConfiguration.foreignKeyNamingStrategy);
			AssociationRecordPersister<IndexedAssociationRecord, IndexedAssociationTable> indexedAssociationPersister =
					new AssociationRecordPersister<>(
							new IndexedAssociationRecordMappingStrategy(intermediaryTable),
							dialect,
							connectionConfiguration);
			return new OneToManyWithIndexedAssociationTableEngine<>(
					manyAssociationConfiguration.srcPersister,
					manyAssociationConfiguration.targetPersister,
					manyRelationDescriptor,
					indexedAssociationPersister);
		}
	}
	
	/**
	 * Configurer dedicated to association that are mapped on reverse side by a property and a column on table's target entities
	 */
	private static class CascadeManyWithMappedAssociationConfigurer<SRC, TRGT, ID, C extends Collection<TRGT>> {
		
		private final ManyAssociationConfiguration<SRC, TRGT, ID, C> manyAssociationConfiguration;
		private final boolean allowOrphanRemoval;
		
		private CascadeManyWithMappedAssociationConfigurer(ManyAssociationConfiguration<SRC, TRGT, ID, C> manyAssociationConfiguration,
														   boolean allowOrphanRemoval) {
			this.manyAssociationConfiguration = manyAssociationConfiguration;
			this.allowOrphanRemoval = allowOrphanRemoval;
		}
		
		private void configure() {
			// We're looking for the foreign key (for necessary join) and for getter/setter required to manage the relation 
			Column reverseColumn = manyAssociationConfiguration.cascadeMany.getReverseColumn();
			Method reverseMethod = null;
			String getterSignature = null;
			// Setter for applying source entity to reverse side (target entities)
			SerializableBiConsumer<TRGT, SRC> reverseSetter = null;
			SerializableFunction<TRGT, SRC> reverseGetter = null;
			PropertyAccessor<TRGT, SRC> reversePropertyAccessor = null;
			if (reverseColumn == null) {
				// Reverse side is surely defined by reverse method (because CascadeManyWithMappedAssociationConfigurer is invoked only
				// when association is mapped by reverse side and reverse column is null),
				// we look for the matching column
				MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer();
				if (manyAssociationConfiguration.cascadeMany.getReverseSetter() != null) {
					reverseSetter = manyAssociationConfiguration.cascadeMany.getReverseSetter();
					reverseMethod = methodReferenceCapturer.findMethod(reverseSetter);
					AccessorByMember<TRGT, SRC, ? extends Member> accessor = new MutatorByMethod<TRGT, SRC>(reverseMethod).toAccessor();
					reverseGetter = accessor::get;
					getterSignature = accessor.toString();
				} else {
					reverseGetter = manyAssociationConfiguration.cascadeMany.getReverseGetter();
					reverseMethod = methodReferenceCapturer.findMethod(reverseGetter);
					getterSignature = Reflections.toString(reverseMethod);
				}
				reversePropertyAccessor = accessor(reverseMethod);
				// Since reverse property accessor may not be declared the same way that it is present in ClassMappingStrategy
				// we must use a ValueAccessPointMap which allows to compare different ValueAccessPoints
				IEntityMappingStrategy<TRGT, ID, ?> targetMappingStrategy = manyAssociationConfiguration.targetPersister.getMappingStrategy();
				ValueAccessPointMap<? extends Column<?, Object>> accessPointMap = new ValueAccessPointMap<>(targetMappingStrategy.getPropertyToColumn());
				reverseColumn = accessPointMap.get(reversePropertyAccessor);
				if (reverseColumn == null) {
					IEntityMappingStrategy<SRC, ID, ?> sourceMappingStrategy = manyAssociationConfiguration.srcPersister.getMappingStrategy();
					// no column found for reverse side owner, we create it
					PrimaryKey<?> primaryKey = sourceMappingStrategy.getTargetTable().getPrimaryKey();
					reverseColumn = targetMappingStrategy.getTargetTable().addColumn(
							manyAssociationConfiguration.joinColumnNamingStrategy.giveName(MemberDefinition.giveMemberDefinition(reversePropertyAccessor)),
							Iterables.first(primaryKey.getColumns()).getJavaType());
					// column can be null if we don't remove orphans
					reverseColumn.setNullable(!allowOrphanRemoval);
					
					SerializableFunction<TRGT, SRC> finalReverseGetter = reverseGetter;
					IdAccessor<SRC, ID> idAccessor = sourceMappingStrategy.getIdMappingStrategy().getIdAccessor();
					Function<TRGT, ID> targetIdSupplier = trgt -> nullable(finalReverseGetter.apply(trgt)).map(idAccessor::getId).getOr((ID) null);
					targetMappingStrategy.addSilentColumnInserter(reverseColumn, targetIdSupplier);
					targetMappingStrategy.addSilentColumnUpdater(reverseColumn, targetIdSupplier);
				}
			} else {
				// Since reverse property accessor may not be declared the same way that it is present in ClassMappingStrategy
				// we must use a ValueAccessPointMap which allows to compare different ValueAccessPoints
				IEntityMappingStrategy<TRGT, ID, ?> targetMappingStrategy = manyAssociationConfiguration.targetPersister.getMappingStrategy();
				IEntityMappingStrategy<SRC, ID, ?> sourceMappingStrategy = manyAssociationConfiguration.srcPersister.getMappingStrategy();
				
				// Reverse side is surely defined by reverse method (because CascadeManyWithMappedAssociationConfigurer is invoked only
				// when association is mapped by reverse side and reverse column is null),
				// we look for the matching column
				SerializableFunction<TRGT, SRC> finalReverseGetter;
				if (manyAssociationConfiguration.cascadeMany.getReverseGetter() != null) {
					finalReverseGetter = manyAssociationConfiguration.cascadeMany.getReverseGetter();
				} else {
					AccessorByMethod<TRGT, SRC> accessor = Accessors.accessorByMethod(
							targetMappingStrategy.getClassToPersist(),
							sourceMappingStrategy.getClassToPersist().getSimpleName().toLowerCase()
					);
					reversePropertyAccessor = new PropertyAccessor<>(accessor);
					reverseGetter = accessor::get;
					getterSignature = accessor.toString();
					finalReverseGetter = reverseGetter;
				}
				
				IdAccessor<SRC, ID> idAccessor = sourceMappingStrategy.getIdMappingStrategy().getIdAccessor();
				Function<TRGT, ID> targetIdSupplier = trgt -> nullable(finalReverseGetter.apply(trgt)).map(idAccessor::getId).getOr((ID) null);
				targetMappingStrategy.addSilentColumnInserter(reverseColumn, targetIdSupplier);
				targetMappingStrategy.addSilentColumnUpdater(reverseColumn, targetIdSupplier);
			}
			manyAssociationConfiguration.reversePropertySetter = reversePropertyAccessor;
			
			if (manyAssociationConfiguration.cascadeMany instanceof CascadeManyList) {
				if (reverseGetter == null) {
					throw new UnsupportedOperationException("Indexed collection without getter is not supported : relation is mapped by "
							+ (reverseMethod != null ? Reflections.toString(reverseMethod) : manyAssociationConfiguration.cascadeMany.getReverseColumn())
							+ " but no indexing property is defined");
				}
				
				if (((CascadeManyList) manyAssociationConfiguration.cascadeMany).getIndexingColumn() == null) {
					throw new UnsupportedOperationException("Missing indexing column : relation is mapped by "
							+ (reverseMethod != null ? Reflections.toString(reverseMethod) : manyAssociationConfiguration.cascadeMany.getReverseColumn())
							+ " but no indexing property is defined");
				}
			}
			
			// adding foreign key constraint
			reverseColumn.getTable().addForeignKey(manyAssociationConfiguration.foreignKeyNamingStrategy.giveName(reverseColumn,
					manyAssociationConfiguration.leftPrimaryKey), reverseColumn, manyAssociationConfiguration.leftPrimaryKey);
			
			// we have a direct relation : relation is owned by target table as a foreign key
			OneToManyWithMappedAssociationEngine<SRC, TRGT, ID, C> mappedAssociationEngine;
			if (manyAssociationConfiguration.cascadeMany instanceof CascadeManyList) {
				mappedAssociationEngine = configureIndexedAssociation(getterSignature, reversePropertyAccessor == null ? null : reversePropertyAccessor::set, reverseGetter);
			} else {
				mappedAssociationEngine = configureNonIndexedAssociation(reversePropertyAccessor == null ? null : reversePropertyAccessor::set);
			}
			mappedAssociationEngine.addSelectCascade(manyAssociationConfiguration.leftPrimaryKey, reverseColumn);
			if (manyAssociationConfiguration.writeAuthorized) {
				mappedAssociationEngine.addInsertCascade();
				mappedAssociationEngine.addUpdateCascade(manyAssociationConfiguration.orphanRemoval);
				mappedAssociationEngine.addDeleteCascade(manyAssociationConfiguration.orphanRemoval);
			}
		}
		
		private OneToManyWithMappedAssociationEngine<SRC, TRGT, ID, C> configureNonIndexedAssociation(@Nullable BiConsumer<TRGT, SRC> reverseSetter) {
			OneToManyWithMappedAssociationEngine<SRC, TRGT, ID, C> mappedAssociationEngine;
			MappedManyRelationDescriptor<SRC, TRGT, C> manyRelationDefinition = new MappedManyRelationDescriptor<>(
					manyAssociationConfiguration.collectionGetter::get, manyAssociationConfiguration.setter::set,
					manyAssociationConfiguration.giveCollectionFactory(), reverseSetter);
			mappedAssociationEngine = new OneToManyWithMappedAssociationEngine<>(
					manyAssociationConfiguration.targetPersister,
					manyRelationDefinition,
					manyAssociationConfiguration.srcPersister);
			return mappedAssociationEngine;
		}
		
		private OneToManyWithMappedAssociationEngine<SRC, TRGT, ID, C> configureIndexedAssociation(String getterSignature,
																											  @Nullable BiConsumer<TRGT, SRC> reverseSetter,
																											  SerializableFunction<TRGT, SRC> reverseGetter) {
			OneToManyWithMappedAssociationEngine<SRC, TRGT, ID, C> mappedAssociationEngine;
			IndexedMappedManyRelationDescriptor<SRC, TRGT, C> manyRelationDefinition = new IndexedMappedManyRelationDescriptor<>(
					manyAssociationConfiguration.collectionGetter::get, manyAssociationConfiguration.setter::set,
					manyAssociationConfiguration.giveCollectionFactory(), reverseSetter, reverseGetter, getterSignature);
			mappedAssociationEngine = (OneToManyWithMappedAssociationEngine) new OneToManyWithIndexedMappedAssociationEngine<>(
					manyAssociationConfiguration.targetPersister,
					(IndexedMappedManyRelationDescriptor) manyRelationDefinition,
					manyAssociationConfiguration.srcPersister,
					((CascadeManyList) manyAssociationConfiguration.cascadeMany).getIndexingColumn()
			);
			return mappedAssociationEngine;
		}
	}
}
