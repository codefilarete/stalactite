package org.codefilarete.stalactite.engine.configurer.manytomany;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.engine.NotYetSupportedOperationException;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.configurer.AssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.IndexedAssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelationConfigurer.FirstPhaseCycleLoadListener;
import org.codefilarete.stalactite.engine.runtime.onetomany.AbstractOneToManyWithAssociationTableEngine;
import org.codefilarete.stalactite.engine.runtime.AssociationRecord;
import org.codefilarete.stalactite.engine.runtime.AssociationRecordPersister;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecord;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.engine.runtime.onetomany.ManyRelationDescriptor;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithAssociationTableEngine;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithIndexedAssociationTableEngine;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

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
	private final PersisterRegistry persisterRegistry;
	private PrimaryKey<?, SRCID> sourcePrimaryKey;
	private final ManyToManyWithAssociationTableConfigurer<SRC, TRGT, SRCID, TRGTID, C1, C2, ?, ?> configurer;
	
	public ManyToManyRelationConfigurer(ManyToManyRelation<SRC, TRGT, TRGTID, C1, C2> manyToManyRelation,
										EntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister,
										Dialect dialect,
										ConnectionConfiguration connectionConfiguration,
										PersisterRegistry persisterRegistry,
										ForeignKeyNamingStrategy foreignKeyNamingStrategy,
										JoinColumnNamingStrategy joinColumnNamingStrategy,
										AssociationTableNamingStrategy associationTableNamingStrategy) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.persisterRegistry = persisterRegistry;
		
		PrimaryKey<?, SRCID> leftPrimaryKey = lookupSourcePrimaryKey(sourcePersister);
		
		RelationMode maintenanceMode = manyToManyRelation.getRelationMode();
		// selection is always present (else configuration is nonsense !)
		boolean orphanRemoval = maintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL;
		boolean writeAuthorized = maintenanceMode != RelationMode.READ_ONLY;
		
		ManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C1, C2, ?, ?> manyAssociationConfiguration = new ManyAssociationConfiguration<>(manyToManyRelation,
				sourcePersister, leftPrimaryKey,
				foreignKeyNamingStrategy,
				orphanRemoval, writeAuthorized);
		configurer = new ManyToManyWithAssociationTableConfigurer<>(manyAssociationConfiguration,
				associationTableNamingStrategy,
				dialect,
				maintenanceMode == RelationMode.ASSOCIATION_ONLY,
				connectionConfiguration);
	}
	
	/**
	 * Sets source primary key. Necessary for foreign key creation in case of inheritance and many relation defined for each subclass :
	 * by default a foreign key will be created from target entity table to subclass source primary key which will create several one pointing
	 * to different table, hence when inserting target entity the column owning relation should point to each subclass table, which is not possible,
	 * throwing a foreign key violation.
	 * 
	 * @param sourcePrimaryKey primary key to which the foreign key from side that owns relation must point to
	 * @return this
	 */
	public ManyToManyRelationConfigurer<SRC, TRGT, SRCID, TRGTID, C1, C2> setSourcePrimaryKey(PrimaryKey<?, SRCID> sourcePrimaryKey) {
		this.sourcePrimaryKey = sourcePrimaryKey;
		return this;
	}
	
	public void configure(ManyToManyRelation<SRC, TRGT, TRGTID, C1, C2> manyToManyRelation,
						  EntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister,
						  ForeignKeyNamingStrategy foreignKeyNamingStrategy,
						  JoinColumnNamingStrategy joinColumnNamingStrategy,
						  AssociationTableNamingStrategy associationTableNamingStrategy,
						  PersisterBuilderImpl<TRGT, TRGTID> targetPersisterBuilder) {
		Table targetTable = determineTargetTable(manyToManyRelation);
		EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister = targetPersisterBuilder
				.build(dialect, connectionConfiguration, persisterRegistry, targetTable);
		
		configure(manyToManyRelation, sourcePersister, foreignKeyNamingStrategy, joinColumnNamingStrategy,
				associationTableNamingStrategy, targetPersister);
	}
	
	public CascadeConfigurationResult<SRC, TRGT> configureWithSelectIn2Phases(String tableAlias,
																			  EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister,
																			  FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener) {
		return this.configurer.configureWithSelectIn2Phases(tableAlias, targetPersister, firstPhaseCycleLoadListener);
	}
	
	void configure(ManyToManyRelation<SRC, TRGT, TRGTID, C1, C2> manyToManyRelation,
				   EntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister,
				   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
				   JoinColumnNamingStrategy joinColumnNamingStrategy,
				   AssociationTableNamingStrategy associationTableNamingStrategy,
				   EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister) {
		PrimaryKey<?, SRCID> leftPrimaryKey = sourcePrimaryKey == null ? lookupSourcePrimaryKey(sourcePersister) : null;
		
		RelationMode maintenanceMode = manyToManyRelation.getRelationMode();
		// selection is always present (else configuration is nonsense !)
		boolean orphanRemoval = maintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL;
		boolean writeAuthorized = maintenanceMode != RelationMode.READ_ONLY;
		
		ManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C1, C2, ?, ?> manyAssociationConfiguration = new ManyAssociationConfiguration<>(manyToManyRelation,
				sourcePersister, leftPrimaryKey,
				foreignKeyNamingStrategy,
				orphanRemoval, writeAuthorized);
		ConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, C1, C2, ?, ?> configurer;
		
		configurer = new ManyToManyWithAssociationTableConfigurer<>(manyAssociationConfiguration,
				associationTableNamingStrategy,
				dialect,
				maintenanceMode == RelationMode.ASSOCIATION_ONLY,
				connectionConfiguration);
		configurer.configure(targetPersister, manyToManyRelation.isFetchSeparately());
	}
	
	private Table determineTargetTable(ManyToManyRelation<SRC, TRGT, TRGTID, C1, C2> manyToManyRelation) {
		// NB: even if no table is found in configuration, build(..) will create one
		return manyToManyRelation.getTargetTable();
	}
	
	protected PrimaryKey<?, SRCID> lookupSourcePrimaryKey(EntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister) {
		// finding joined columns: left one is primary key. Right one is given by the target strategy through the property accessor
		if (sourcePersister.getMapping().getTargetTable().getPrimaryKey().getColumns().size() > 1) {
			throw new NotYetSupportedOperationException("Joining tables on a composed primary key is not (yet) supported");
		}
		return sourcePersister.getMapping().getTargetTable().getPrimaryKey();
	}
	
	/**
	 * Class that stores elements necessary to many-to-many association configuration
	 */
	private static class ManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C1 extends Collection<TRGT>, C2 extends Collection<SRC>,
			LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>> {
		
		private final ManyToManyRelation<SRC, TRGT, TRGTID, C1, C2> manyToManyRelation;
		private final EntityConfiguredJoinedTablesPersister<SRC, SRCID> srcPersister;
		private final PrimaryKey<LEFTTABLE, SRCID> leftPrimaryKey;
		private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
		private final ReversibleAccessor<SRC, C1> collectionGetter;
		private final Mutator<SRC, C1> setter;
		private final boolean orphanRemoval;
		private final boolean writeAuthorized;
		
		private ManyAssociationConfiguration(ManyToManyRelation<SRC, TRGT, TRGTID, C1, C2> manyToManyRelation,
											 EntityConfiguredJoinedTablesPersister<SRC, SRCID> srcPersister,
											 PrimaryKey<LEFTTABLE, SRCID> leftPrimaryKey,
											 ForeignKeyNamingStrategy foreignKeyNamingStrategy,
											 boolean orphanRemoval,
											 boolean writeAuthorized) {
			this.manyToManyRelation = manyToManyRelation;
			this.srcPersister = srcPersister;
			this.leftPrimaryKey = leftPrimaryKey;
			this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
			this.collectionGetter = manyToManyRelation.getCollectionProvider();
			this.setter = collectionGetter.toMutator();
			// we don't use AccessorDefinition.giveMemberDefinition(..) because it gives a cross-member definition, loosing get/set for example,
			// whereas we need this information to build better association table name
			this.orphanRemoval = orphanRemoval;
			this.writeAuthorized = writeAuthorized;
		}
		
		/**
		 * Gives the collection factory used to instantiate relation field.
		 * 
		 * @return the one given by {@link OneToManyRelation#getCollectionFactory()} or one deduced from member signature
		 */
		protected Supplier<C1> giveCollectionFactory() {
			Supplier<C1> collectionFactory = manyToManyRelation.getCollectionFactory();
			if (collectionFactory == null) {
				collectionFactory = BeanRelationFixer.giveCollectionFactory((Class<C1>) manyToManyRelation.getMethodReference().getPropertyType());
			}
			return collectionFactory;
		}
	}
	
	// Could be merged with lonely subclass, but kept separated to keep track of parallelism with one-to-many configurer 
	private static abstract class ConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, C1 extends Collection<TRGT>, C2 extends Collection<SRC>,
			LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>> {
		
		protected final ManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C1, C2, LEFTTABLE, RIGHTTABLE> manyAssociationConfiguration;
		
		/**
		 * Equivalent as cascadeMany.getMethodReference() but used for table and colum naming only.
		 * Collection access will be done through {@link ManyAssociationConfiguration#collectionGetter} and {@link ManyAssociationConfiguration#giveCollectionFactory()}
		 */
		protected AccessorDefinition accessorDefinition;
		
		protected ConfigurerTemplate(ManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C1, C2, LEFTTABLE, RIGHTTABLE> manyAssociationConfiguration) {
			this.manyAssociationConfiguration = manyAssociationConfiguration;
		}
		
		void determineAccessorDefinition(EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister) {
			// we don't use AccessorDefinition.giveMemberDefinition(..) because it gives a cross-member definition, loosing get/set for example,
			// whereas we need this information to build better association table name
			this.accessorDefinition = new AccessorDefinition(
					manyAssociationConfiguration.manyToManyRelation.getMethodReference().getDeclaringClass(),
					AccessorDefinition.giveDefinition(manyAssociationConfiguration.manyToManyRelation.getMethodReference()).getName(),
					// we prefer target persister type to method reference member type because the latter only get's collection type which is not
					// an interesting information for table / column naming
					targetPersister.getClassToPersist());
		}
		
		abstract void configure(EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister, boolean loadSeparately);
		
		public abstract CascadeConfigurationResult<SRC,TRGT> configureWithSelectIn2Phases(String tableAlias, EntityConfiguredJoinedTablesPersister<TRGT,
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
		
		private ManyToManyWithAssociationTableConfigurer(ManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C1, C2, LEFTTABLE, RIGHTTABLE> manyAssociationConfiguration,
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
		
		private void prepare(EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister) {
			// case : Collection mapping without reverse property : an association table is needed
			RIGHTTABLE rightTable = targetPersister.<RIGHTTABLE>getMapping().getTargetTable();
			PrimaryKey<RIGHTTABLE, TRGTID> rightPrimaryKey = rightTable.getPrimaryKey();
			
			determineAccessorDefinition(targetPersister);
			String associationTableName = associationTableNamingStrategy.giveName(accessorDefinition,
					manyAssociationConfiguration.leftPrimaryKey, rightPrimaryKey);
			
			ManyRelationDescriptor<SRC, TRGT, C1> manyRelationDescriptor = new ManyRelationDescriptor<>(
					manyAssociationConfiguration.collectionGetter::get, manyAssociationConfiguration.setter::set,
					manyAssociationConfiguration.giveCollectionFactory(),
					null);	// no reverse setter since we don't support it for many-to-many (see 
			if (manyAssociationConfiguration.manyToManyRelation.isIndexed()) {
				assignEngineForIndexedAssociation(rightPrimaryKey, associationTableName, manyRelationDescriptor, targetPersister);
			} else {
				assignEngineForNonIndexedAssociation(rightPrimaryKey, associationTableName, manyRelationDescriptor, targetPersister);
			}
		}
		
		@Override
		void configure(EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister, boolean loadSeparately) {
			prepare(targetPersister);
			associationTableEngine.addSelectCascade(manyAssociationConfiguration.srcPersister, loadSeparately);
			addWriteCascades(associationTableEngine);
		}
		
		@Override
		public CascadeConfigurationResult<SRC,TRGT> configureWithSelectIn2Phases(String tableAlias,
																				 EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister,
																				 FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener) {
			prepare(targetPersister);
			associationTableEngine.addSelectCascadeIn2Phases(firstPhaseCycleLoadListener);
			addWriteCascades(associationTableEngine);
			return new CascadeConfigurationResult<>(associationTableEngine.getManyRelationDescriptor().getRelationFixer(), manyAssociationConfiguration.srcPersister);
		}
		
		private void addWriteCascades(AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C1, ? extends AssociationRecord, ? extends AssociationTable> oneToManyWithAssociationTableEngine) {
			if (manyAssociationConfiguration.writeAuthorized) {
				oneToManyWithAssociationTableEngine.addInsertCascade(maintainAssociationOnly);
				oneToManyWithAssociationTableEngine.addUpdateCascade(manyAssociationConfiguration.orphanRemoval, maintainAssociationOnly);
				oneToManyWithAssociationTableEngine.addDeleteCascade(manyAssociationConfiguration.orphanRemoval, dialect);
			}
		}
		
		private <ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
		void assignEngineForNonIndexedAssociation(
				PrimaryKey<RIGHTTABLE, TRGTID> rightPrimaryKey,
				String associationTableName,
				ManyRelationDescriptor<SRC, TRGT, C1> manyRelationDescriptor,
				EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister) {
			
			// we don't create foreign key for table-per-class because source columns should reference different tables (the one
			// per entity) which is not allowed by databases
			boolean createManySideForeignKey = !manyAssociationConfiguration.manyToManyRelation.isTargetTablePerClassPolymorphic();
			ASSOCIATIONTABLE intermediaryTable = (ASSOCIATIONTABLE) new AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>(
					manyAssociationConfiguration.leftPrimaryKey.getTable().getSchema(),
					associationTableName,
					manyAssociationConfiguration.leftPrimaryKey,
					rightPrimaryKey,
					accessorDefinition,
					associationTableNamingStrategy,
					manyAssociationConfiguration.foreignKeyNamingStrategy,
					createManySideForeignKey
			);
			
			AssociationRecordPersister<AssociationRecord, ASSOCIATIONTABLE> associationPersister = new AssociationRecordPersister<>(
					new AssociationRecordMapping<>(intermediaryTable,
							manyAssociationConfiguration.srcPersister.getMapping().getIdMapping().getIdentifierAssembler(),
							targetPersister.getMapping().getIdMapping().getIdentifierAssembler(),
							intermediaryTable.getLeftIdentifierColumnMapping(),
							intermediaryTable.getRightIdentifierColumnMapping()),
					dialect,
					connectionConfiguration);
			associationTableEngine = new OneToManyWithAssociationTableEngine<>(
					manyAssociationConfiguration.srcPersister,
					targetPersister,
					manyRelationDescriptor,
					associationPersister, dialect.getWriteOperationFactory());
		}
		
		private <ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
		void assignEngineForIndexedAssociation(
				PrimaryKey<RIGHTTABLE, TRGTID> rightPrimaryKey,
				String associationTableName,
				ManyRelationDescriptor manyRelationDescriptor,
				EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister) {
			
			// we don't create foreign key for table-per-class because source columns should reference different tables (the one
			// per entity) which is not allowed by databases
			boolean createManySideForeignKey = !manyAssociationConfiguration.manyToManyRelation.isTargetTablePerClassPolymorphic();
			// NB: index column is part of the primary key
			ASSOCIATIONTABLE intermediaryTable = (ASSOCIATIONTABLE) new IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>(
					manyAssociationConfiguration.leftPrimaryKey.getTable().getSchema(),
					associationTableName,
					manyAssociationConfiguration.leftPrimaryKey,
					rightPrimaryKey,
					accessorDefinition,
					associationTableNamingStrategy,
					manyAssociationConfiguration.foreignKeyNamingStrategy,
					createManySideForeignKey);
			
			intermediaryTable.addForeignKey(manyAssociationConfiguration.foreignKeyNamingStrategy::giveName,
					intermediaryTable.getOneSideForeignKey(), manyAssociationConfiguration.leftPrimaryKey);
			
			AssociationRecordPersister<IndexedAssociationRecord, ASSOCIATIONTABLE> indexedAssociationPersister =
					new AssociationRecordPersister<>(
							new IndexedAssociationRecordMapping<>(intermediaryTable,
									manyAssociationConfiguration.srcPersister.getMapping().getIdMapping().getIdentifierAssembler(),
									targetPersister.getMapping().getIdMapping().getIdentifierAssembler(),
									null,
									null),
							dialect,
							connectionConfiguration);
			associationTableEngine = new OneToManyWithIndexedAssociationTableEngine<>(
							manyAssociationConfiguration.srcPersister,
							targetPersister,
							manyRelationDescriptor,
							indexedAssociationPersister,
							intermediaryTable.getIndexColumn(),
							dialect.getWriteOperationFactory());
		}
	}
}
