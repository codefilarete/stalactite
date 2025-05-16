package org.codefilarete.stalactite.engine.configurer.manytomany;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.MutatorByMethod;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.AssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.IndexedAssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation.MappedByConfiguration;
import org.codefilarete.stalactite.engine.configurer.onetomany.FirstPhaseCycleLoadListener;
import org.codefilarete.stalactite.engine.runtime.AssociationRecord;
import org.codefilarete.stalactite.engine.runtime.AssociationRecordPersister;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecord;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.engine.runtime.onetomany.AbstractOneToManyWithAssociationTableEngine;
import org.codefilarete.stalactite.engine.runtime.onetomany.ManyRelationDescriptor;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithAssociationTableEngine;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithIndexedAssociationTableEngine;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.bean.FieldIterator;
import org.codefilarete.tool.bean.InstanceFieldIterator;
import org.codefilarete.tool.collection.Iterables;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * @param <SRC> type of input (left/source entities)
 * @param <TRGT> type of output (right/target entities)
 * @param <SRCID> identifier type of source entities
 * @param <TRGTID> identifier type of target entities
 * @param <C1> collection type of the relation
 * @author Guillaume Mary
 */
public class ManyToManyRelationConfigurer<SRC, TRGT, SRCID, TRGTID, C1 extends Collection<TRGT>, C2 extends Collection<SRC>> {
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	private final ManyToManyWithAssociationTableConfigurer<SRC, TRGT, SRCID, TRGTID, C1, C2, ?, ?> configurer;
	private final ManyToManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C1, C2, ?, ?> associationConfiguration;
	
	public ManyToManyRelationConfigurer(ManyToManyRelation<SRC, TRGT, TRGTID, C1, C2> manyToManyRelation,
										ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
										Dialect dialect,
										ConnectionConfiguration connectionConfiguration,
										ForeignKeyNamingStrategy foreignKeyNamingStrategy,
										JoinColumnNamingStrategy joinColumnNamingStrategy,
										ColumnNamingStrategy indexColumnNamingStrategy,
										AssociationTableNamingStrategy associationTableNamingStrategy) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		
		PrimaryKey<?, SRCID> leftPrimaryKey = lookupSourcePrimaryKey(sourcePersister);
		
		RelationMode maintenanceMode = manyToManyRelation.getRelationMode();
		// selection is always present (else configuration is nonsense !)
		boolean orphanRemoval = maintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL;
		boolean writeAuthorized = maintenanceMode != RelationMode.READ_ONLY;
		
		this.associationConfiguration = new ManyToManyAssociationConfiguration<>(manyToManyRelation,
				sourcePersister,
				leftPrimaryKey,
				foreignKeyNamingStrategy,
				indexColumnNamingStrategy,
				orphanRemoval, writeAuthorized);
		this.configurer = new ManyToManyWithAssociationTableConfigurer<>(associationConfiguration,
				associationTableNamingStrategy,
				dialect,
				maintenanceMode == RelationMode.ASSOCIATION_ONLY,
				connectionConfiguration);
	}
	
	public void configure(PersisterBuilderImpl<TRGT, TRGTID> targetPersisterBuilder) {
		Table targetTable = determineTargetTable(associationConfiguration.getManyToManyRelation());
		ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister = targetPersisterBuilder
				.build(dialect, connectionConfiguration, targetTable);
		
		configurer.configure(targetPersister, associationConfiguration.getManyToManyRelation().isFetchSeparately());
	}
	
	public CascadeConfigurationResult<SRC, TRGT> configureWithSelectIn2Phases(String tableAlias,
																			  ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
																			  FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener) {
		return this.configurer.configureWithSelectIn2Phases(tableAlias, targetPersister, firstPhaseCycleLoadListener);
	}
	
	private Table determineTargetTable(ManyToManyRelation<SRC, TRGT, TRGTID, C1, C2> manyToManyRelation) {
		// NB: even if no table is found in configuration, build(..) will create one
		return manyToManyRelation.getTargetTable();
	}
	
	protected PrimaryKey<?, SRCID> lookupSourcePrimaryKey(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister) {
		return sourcePersister.getMapping().getTargetTable().getPrimaryKey();
	}
	
	// Could be merged with lonely subclass, but kept separated to keep track of parallelism with one-to-many configurer 
	private static abstract class ConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, C1 extends Collection<TRGT>, C2 extends Collection<SRC>,
			LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>> {
		
		protected final ManyToManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C1, C2, LEFTTABLE, RIGHTTABLE> associationConfiguration;
		
		/**
		 * Equivalent as cascadeMany.getMethodReference() but used for table and colum naming only.
		 * Collection access will be done through {@link ManyToManyAssociationConfiguration#getCollectionGetter()}
		 * and {@link ManyToManyAssociationConfiguration#giveCollectionFactory()}
		 */
		protected AccessorDefinition accessorDefinition;
		
		protected ConfigurerTemplate(ManyToManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C1, C2, LEFTTABLE, RIGHTTABLE> associationConfiguration) {
			this.associationConfiguration = associationConfiguration;
		}
		
		void determineAccessorDefinition(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
			// we don't use AccessorDefinition.giveMemberDefinition(..) because it gives a cross-member definition, loosing get/set for example,
			// whereas we need this information to build better association table name
			this.accessorDefinition = new AccessorDefinition(
					associationConfiguration.getManyToManyRelation().getMethodReference().getDeclaringClass(),
					AccessorDefinition.giveDefinition(associationConfiguration.getManyToManyRelation().getMethodReference()).getName(),
					// we prefer target persister type to method reference member type because the latter only get's collection type which is not
					// an interesting information for table / column naming
					targetPersister.getClassToPersist());
		}
		
		abstract void configure(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister, boolean loadSeparately);
		
		public abstract CascadeConfigurationResult<SRC,TRGT> configureWithSelectIn2Phases(String tableAlias, ConfiguredRelationalPersister<TRGT,
								TRGTID> targetPersister, FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener);
	}
	
	/**
	 * Configurer dedicated to association that needs an intermediary table between source entities and these of the relation
	 */
	private static class ManyToManyWithAssociationTableConfigurer<SRC, TRGT, SRCID, TRGTID, C1 extends Collection<TRGT>, C2 extends Collection<SRC>,
			LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
			extends ConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, C1, C2, LEFTTABLE, RIGHTTABLE> {
		
		private final AssociationTableNamingStrategy associationTableNamingStrategy;
		private final Dialect dialect;
		private final boolean maintainAssociationOnly;
		private final ConnectionConfiguration connectionConfiguration;
		
		private AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C1, AssociationRecord, ?> associationTableEngine;
		
		private ManyToManyWithAssociationTableConfigurer(ManyToManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C1, C2, LEFTTABLE, RIGHTTABLE> manyAssociationConfiguration,
														 AssociationTableNamingStrategy associationTableNamingStrategy,
														 Dialect dialect,
														 boolean maintainAssociationOnly,
														 ConnectionConfiguration connectionConfiguration) {
			super(manyAssociationConfiguration);
			this.associationTableNamingStrategy = associationTableNamingStrategy;
			this.dialect = dialect;
			this.maintainAssociationOnly = maintainAssociationOnly;
			this.connectionConfiguration = connectionConfiguration;
		}
		
		private void prepare(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
			// case : Collection mapping without reverse property : an association table is needed
			RIGHTTABLE rightTable = targetPersister.<RIGHTTABLE>getMapping().getTargetTable();
			PrimaryKey<RIGHTTABLE, TRGTID> rightPrimaryKey = rightTable.getPrimaryKey();
			
			determineAccessorDefinition(targetPersister);
			String associationTableName = associationTableNamingStrategy.giveName(accessorDefinition,
					associationConfiguration.getLeftPrimaryKey(), rightPrimaryKey);
			
			ManyRelationDescriptor<SRC, TRGT, C1> manyRelationDescriptor = new ManyRelationDescriptor<>(
					associationConfiguration.getCollectionGetter()::get,
					associationConfiguration.getSetter()::set,
					associationConfiguration.giveCollectionFactory(),
					buildReverseCombiner(targetPersister.getClassToPersist())); 
			if (associationConfiguration.getManyToManyRelation().isOrdered()) {
				assignEngineForIndexedAssociation(rightPrimaryKey, associationTableName, manyRelationDescriptor, targetPersister);
			} else {
				assignEngineForNonIndexedAssociation(rightPrimaryKey, associationTableName, manyRelationDescriptor, targetPersister);
			}
		}
		
		/**
		 * Build the combiner between target entities and source ones.
		 * 
		 * 
		 * @param targetClass target entity type, provided to lookup for reverse property if no sufficient info was given
		 * @return null if no information was provided about reverse side (no bidirectionality) 
		 */
		private SerializableBiConsumer<TRGT, SRC> buildReverseCombiner(Class<TRGT> targetClass) {
			MappedByConfiguration<SRC, TRGT, C2> mappedByConfiguration = associationConfiguration.getManyToManyRelation().getMappedByConfiguration();
			if (mappedByConfiguration.isEmpty()) {
				// relation is not bidirectional, and not even set by reverse link, there's nothing to do
				return null;
			} else {
				PropertyAccessor<TRGT, C2> collectionAccessor = associationConfiguration.buildReversePropertyAccessor();
				if (collectionAccessor == null) {
					// since some reverse info has been done but not the collection accessor, we try to find the matching property by type
					FieldIterator targetFields = new InstanceFieldIterator(targetClass);
					Class<SRC> sourceEntityType = associationConfiguration.getSrcPersister().getClassToPersist();
					Field reverseField = Iterables.find(targetFields, field -> Collection.class.isAssignableFrom(field.getType())
							&& ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0].equals(sourceEntityType));
					if (reverseField != null) {
						Nullable<AccessorByMethod<TRGT, C2>> reverseGetterMethod = nullable(Accessors.accessorByMethod(reverseField));
						if (reverseGetterMethod.isPresent()) {
							collectionAccessor = new PropertyAccessor<>(reverseGetterMethod.get());
						} else {
							Nullable<MutatorByMethod<TRGT, C2>> reverseSetterMethod = nullable(Accessors.mutatorByMethod(reverseField));
							if (reverseSetterMethod.isPresent()) {
								collectionAccessor = new PropertyAccessor<>(reverseSetterMethod.get());
							}
						}
					} // else : relation is not bidirectional, or not a usual one, may be set by reverse link
				}
				
				Nullable<SerializableBiConsumer<TRGT, SRC>> configuredCombiner = nullable(mappedByConfiguration.getReverseCombiner());
				if (collectionAccessor == null) {
					return configuredCombiner.get();
				} else {
					// collection factory is in priority the one configured
					Supplier<C2> reverseCollectionFactory = mappedByConfiguration.getReverseCollectionFactory();
					if (reverseCollectionFactory == null) {
						Class<C2> collectionType = AccessorDefinition.giveDefinition(collectionAccessor).getMemberType();
						reverseCollectionFactory = BeanRelationFixer.giveCollectionFactory(collectionType);
					}
					PropertyAccessor<TRGT, C2> finalCollectionAccessor = collectionAccessor;
					SerializableBiConsumer<TRGT, SRC> combiner = configuredCombiner.getOr((TRGT trgt, SRC src) -> {
						// collectionAccessor can't be null due to nullable check
						finalCollectionAccessor.get(trgt).add(src);
					});
					
					Supplier<C2> effectiveCollectionFactory = reverseCollectionFactory;
					return (TRGT trgt, SRC src) -> {
						// we call the collection factory to ensure that property is initialized
						if (finalCollectionAccessor.get(trgt) == null) {
							finalCollectionAccessor.set(trgt, effectiveCollectionFactory.get());
						}
						// Note that combiner can't be null here thanks to nullable(..) check
						combiner.accept(trgt, src);
					};
				}
			}
		}
		
		@Override
		void configure(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister, boolean loadSeparately) {
			prepare(targetPersister);
			associationTableEngine.addSelectCascade(associationConfiguration.getSrcPersister(), loadSeparately);
			addWriteCascades(associationTableEngine, targetPersister);
		}
		
		@Override
		public CascadeConfigurationResult<SRC,TRGT> configureWithSelectIn2Phases(String tableAlias,
																				 ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
																				 FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener) {
			prepare(targetPersister);
			associationTableEngine.addSelectCascadeIn2Phases(firstPhaseCycleLoadListener);
			addWriteCascades(associationTableEngine, targetPersister);
			return new CascadeConfigurationResult<>(associationTableEngine.getManyRelationDescriptor().getRelationFixer(), associationConfiguration.getSrcPersister());
		}
		
		private void addWriteCascades(AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C1, ? extends AssociationRecord, ? extends AssociationTable> oneToManyWithAssociationTableEngine, ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
			if (associationConfiguration.isWriteAuthorized()) {
				oneToManyWithAssociationTableEngine.addInsertCascade(maintainAssociationOnly, targetPersister);
				oneToManyWithAssociationTableEngine.addUpdateCascade(associationConfiguration.isOrphanRemoval(), maintainAssociationOnly, targetPersister);
				oneToManyWithAssociationTableEngine.addDeleteCascade(associationConfiguration.isOrphanRemoval(), dialect, targetPersister);
			}
		}
		
		private <ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
		void assignEngineForNonIndexedAssociation(
				PrimaryKey<RIGHTTABLE, TRGTID> rightPrimaryKey,
				String associationTableName,
				ManyRelationDescriptor<SRC, TRGT, C1> manyRelationDescriptor,
				ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
			
			// we don't create foreign key for table-per-class because source columns should reference different tables (the one
			// per entity) which is not allowed by databases
			boolean createOneSideForeignKey = !(associationConfiguration.getManyToManyRelation().isSourceTablePerClassPolymorphic());
			boolean createManySideForeignKey = !associationConfiguration.getManyToManyRelation().isTargetTablePerClassPolymorphic();
			ASSOCIATIONTABLE intermediaryTable = (ASSOCIATIONTABLE) new AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>(
					associationConfiguration.getLeftPrimaryKey().getTable().getSchema(),
					associationTableName,
					associationConfiguration.getLeftPrimaryKey(),
					rightPrimaryKey,
					accessorDefinition,
					associationTableNamingStrategy,
					associationConfiguration.getForeignKeyNamingStrategy(),
					createOneSideForeignKey,
					createManySideForeignKey
			);
			
			AssociationRecordPersister<AssociationRecord, ASSOCIATIONTABLE> associationPersister = new AssociationRecordPersister<>(
					new AssociationRecordMapping<>(intermediaryTable,
							associationConfiguration.getSrcPersister().getMapping().getIdMapping().getIdentifierAssembler(),
							targetPersister.getMapping().getIdMapping().getIdentifierAssembler(),
							intermediaryTable.getLeftIdentifierColumnMapping(),
							intermediaryTable.getRightIdentifierColumnMapping()),
					dialect,
					connectionConfiguration);
			associationTableEngine = new OneToManyWithAssociationTableEngine<>(
					associationConfiguration.getSrcPersister(),
					targetPersister,
					manyRelationDescriptor,
					associationPersister, dialect.getWriteOperationFactory());
		}
		
		private <ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
		void assignEngineForIndexedAssociation(
				PrimaryKey<RIGHTTABLE, TRGTID> rightPrimaryKey,
				String associationTableName,
				ManyRelationDescriptor manyRelationDescriptor,
				ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
			
			ManyToManyRelation<SRC, TRGT, TRGTID, C1, C2> relation = associationConfiguration.getManyToManyRelation();
			String indexingColumnName = nullable(relation.getIndexingColumnName()).getOr(() -> associationConfiguration.getIndexColumnNamingStrategy().giveName(accessorDefinition));
			
			// we don't create foreign key for table-per-class because source columns should reference different tables (the one
			// per entity) which is not allowed by databases
			boolean createOneSideForeignKey = !relation.isSourceTablePerClassPolymorphic();
			boolean createManySideForeignKey = !relation.isTargetTablePerClassPolymorphic();
			// NB: index column is part of the primary key
			ASSOCIATIONTABLE intermediaryTable = (ASSOCIATIONTABLE) new IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>(
					associationConfiguration.getLeftPrimaryKey().getTable().getSchema(),
					associationTableName,
					associationConfiguration.getLeftPrimaryKey(),
					rightPrimaryKey,
					accessorDefinition,
					associationTableNamingStrategy,
					associationConfiguration.getForeignKeyNamingStrategy(),
					createOneSideForeignKey,
					createManySideForeignKey,
					indexingColumnName);
			
			intermediaryTable.addForeignKey(associationConfiguration.getForeignKeyNamingStrategy()::giveName,
					intermediaryTable.getOneSideForeignKey(), associationConfiguration.getLeftPrimaryKey());
			
			AssociationRecordPersister<IndexedAssociationRecord, ASSOCIATIONTABLE> indexedAssociationPersister =
					new AssociationRecordPersister<>(
							new IndexedAssociationRecordMapping<>(intermediaryTable,
									associationConfiguration.getSrcPersister().getMapping().getIdMapping().getIdentifierAssembler(),
									targetPersister.getMapping().getIdMapping().getIdentifierAssembler(),
									intermediaryTable.getLeftIdentifierColumnMapping(),
									intermediaryTable.getRightIdentifierColumnMapping()),
							dialect,
							connectionConfiguration);
			associationTableEngine = new OneToManyWithIndexedAssociationTableEngine<>(
					associationConfiguration.getSrcPersister(),
					targetPersister,
					manyRelationDescriptor,
					indexedAssociationPersister,
					intermediaryTable.getIndexColumn(),
					dialect.getWriteOperationFactory());
		}
	}
}
