package org.codefilarete.stalactite.engine.configurer.manytomany;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.configurer.AssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.IndexedAssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelationConfigurer.FirstPhaseCycleLoadListener;
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
	private final PersisterRegistry persisterRegistry;
	private final ManyToManyWithAssociationTableConfigurer<SRC, TRGT, SRCID, TRGTID, C1, C2, ?, ?> configurer;
	private final ManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C1, C2, ?, ?> associationConfiguration;
	
	public ManyToManyRelationConfigurer(ManyToManyRelation<SRC, TRGT, TRGTID, C1, C2> manyToManyRelation,
										ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
										Dialect dialect,
										ConnectionConfiguration connectionConfiguration,
										PersisterRegistry persisterRegistry,
										ForeignKeyNamingStrategy foreignKeyNamingStrategy,
										JoinColumnNamingStrategy joinColumnNamingStrategy,
										ColumnNamingStrategy indexColumnNamingStrategy,
										AssociationTableNamingStrategy associationTableNamingStrategy) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.persisterRegistry = persisterRegistry;
		
		PrimaryKey<?, SRCID> leftPrimaryKey = lookupSourcePrimaryKey(sourcePersister);
		
		RelationMode maintenanceMode = manyToManyRelation.getRelationMode();
		// selection is always present (else configuration is nonsense !)
		boolean orphanRemoval = maintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL;
		boolean writeAuthorized = maintenanceMode != RelationMode.READ_ONLY;
		
		this.associationConfiguration = new ManyAssociationConfiguration<>(manyToManyRelation,
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
		Table targetTable = determineTargetTable(associationConfiguration.manyToManyRelation);
		ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister = targetPersisterBuilder
				.build(dialect, connectionConfiguration, persisterRegistry, targetTable);
		
		configurer.configure(targetPersister, associationConfiguration.manyToManyRelation.isFetchSeparately());
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
	
	/**
	 * Class that stores elements necessary to many-to-many association configuration
	 */
	private static class ManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C1 extends Collection<TRGT>, C2 extends Collection<SRC>,
			LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>> {
		
		private final ManyToManyRelation<SRC, TRGT, TRGTID, C1, C2> manyToManyRelation;
		private final ConfiguredRelationalPersister<SRC, SRCID> srcPersister;
		private final PrimaryKey<LEFTTABLE, SRCID> leftPrimaryKey;
		private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
		private final ColumnNamingStrategy indexColumnNamingStrategy;
		private final ReversibleAccessor<SRC, C1> collectionGetter;
		private final Mutator<SRC, C1> setter;
		private final boolean orphanRemoval;
		private final boolean writeAuthorized;
		
		private ManyAssociationConfiguration(ManyToManyRelation<SRC, TRGT, TRGTID, C1, C2> manyToManyRelation,
											 ConfiguredRelationalPersister<SRC, SRCID> srcPersister,
											 PrimaryKey<LEFTTABLE, SRCID> leftPrimaryKey,
											 ForeignKeyNamingStrategy foreignKeyNamingStrategy,
											 ColumnNamingStrategy indexColumnNamingStrategy,
											 boolean orphanRemoval,
											 boolean writeAuthorized) {
			this.manyToManyRelation = manyToManyRelation;
			this.srcPersister = srcPersister;
			this.leftPrimaryKey = leftPrimaryKey;
			this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
			this.collectionGetter = manyToManyRelation.getCollectionProvider();
			this.indexColumnNamingStrategy = indexColumnNamingStrategy;
			this.setter = collectionGetter.toMutator();
			// we don't use AccessorDefinition.giveMemberDefinition(..) because it gives a cross-member definition, loosing get/set for example,
			// whereas we need this information to build better association table name
			this.orphanRemoval = orphanRemoval;
			this.writeAuthorized = writeAuthorized;
		}
		
		public ManyToManyRelation<SRC, TRGT, TRGTID, C1, C2> getManyToManyRelation() {
			return manyToManyRelation;
		}
		
		public ColumnNamingStrategy getIndexColumnNamingStrategy() {
			return indexColumnNamingStrategy;
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
		
		protected final ManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C1, C2, LEFTTABLE, RIGHTTABLE> associationConfiguration;
		
		/**
		 * Equivalent as cascadeMany.getMethodReference() but used for table and colum naming only.
		 * Collection access will be done through {@link ManyAssociationConfiguration#collectionGetter} and {@link ManyAssociationConfiguration#giveCollectionFactory()}
		 */
		protected AccessorDefinition accessorDefinition;
		
		protected ConfigurerTemplate(ManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C1, C2, LEFTTABLE, RIGHTTABLE> associationConfiguration) {
			this.associationConfiguration = associationConfiguration;
		}
		
		void determineAccessorDefinition(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
			// we don't use AccessorDefinition.giveMemberDefinition(..) because it gives a cross-member definition, loosing get/set for example,
			// whereas we need this information to build better association table name
			this.accessorDefinition = new AccessorDefinition(
					associationConfiguration.manyToManyRelation.getMethodReference().getDeclaringClass(),
					AccessorDefinition.giveDefinition(associationConfiguration.manyToManyRelation.getMethodReference()).getName(),
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
		
		private void prepare(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
			// case : Collection mapping without reverse property : an association table is needed
			RIGHTTABLE rightTable = targetPersister.<RIGHTTABLE>getMapping().getTargetTable();
			PrimaryKey<RIGHTTABLE, TRGTID> rightPrimaryKey = rightTable.getPrimaryKey();
			
			determineAccessorDefinition(targetPersister);
			String associationTableName = associationTableNamingStrategy.giveName(accessorDefinition,
					associationConfiguration.leftPrimaryKey, rightPrimaryKey);
			
			ManyRelationDescriptor<SRC, TRGT, C1> manyRelationDescriptor = new ManyRelationDescriptor<>(
					associationConfiguration.collectionGetter::get,
					associationConfiguration.setter::set,
					associationConfiguration.giveCollectionFactory(),
					associationConfiguration.getManyToManyRelation().getReverseLink());	// no reverse setter since we don't support it for many-to-many (see 
			if (associationConfiguration.manyToManyRelation.isOrdered()) {
				assignEngineForIndexedAssociation(rightPrimaryKey, associationTableName, manyRelationDescriptor, targetPersister);
			} else {
				assignEngineForNonIndexedAssociation(rightPrimaryKey, associationTableName, manyRelationDescriptor, targetPersister);
			}
		}
		
		@Override
		void configure(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister, boolean loadSeparately) {
			prepare(targetPersister);
			associationTableEngine.addSelectCascade(associationConfiguration.srcPersister, loadSeparately);
			addWriteCascades(associationTableEngine);
		}
		
		@Override
		public CascadeConfigurationResult<SRC,TRGT> configureWithSelectIn2Phases(String tableAlias,
																				 ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
																				 FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener) {
			prepare(targetPersister);
			associationTableEngine.addSelectCascadeIn2Phases(firstPhaseCycleLoadListener);
			addWriteCascades(associationTableEngine);
			return new CascadeConfigurationResult<>(associationTableEngine.getManyRelationDescriptor().getRelationFixer(), associationConfiguration.srcPersister);
		}
		
		private void addWriteCascades(AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C1, ? extends AssociationRecord, ? extends AssociationTable> oneToManyWithAssociationTableEngine) {
			if (associationConfiguration.writeAuthorized) {
				oneToManyWithAssociationTableEngine.addInsertCascade(maintainAssociationOnly);
				oneToManyWithAssociationTableEngine.addUpdateCascade(associationConfiguration.orphanRemoval, maintainAssociationOnly);
				oneToManyWithAssociationTableEngine.addDeleteCascade(associationConfiguration.orphanRemoval, dialect);
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
			boolean createOneSideForeignKey = !(associationConfiguration.manyToManyRelation.isSourceTablePerClassPolymorphic());
			boolean createManySideForeignKey = !associationConfiguration.manyToManyRelation.isTargetTablePerClassPolymorphic();
			ASSOCIATIONTABLE intermediaryTable = (ASSOCIATIONTABLE) new AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>(
					associationConfiguration.leftPrimaryKey.getTable().getSchema(),
					associationTableName,
					associationConfiguration.leftPrimaryKey,
					rightPrimaryKey,
					accessorDefinition,
					associationTableNamingStrategy,
					associationConfiguration.foreignKeyNamingStrategy,
					createOneSideForeignKey,
					createManySideForeignKey
			);
			
			AssociationRecordPersister<AssociationRecord, ASSOCIATIONTABLE> associationPersister = new AssociationRecordPersister<>(
					new AssociationRecordMapping<>(intermediaryTable,
							associationConfiguration.srcPersister.getMapping().getIdMapping().getIdentifierAssembler(),
							targetPersister.getMapping().getIdMapping().getIdentifierAssembler(),
							intermediaryTable.getLeftIdentifierColumnMapping(),
							intermediaryTable.getRightIdentifierColumnMapping()),
					dialect,
					connectionConfiguration);
			associationTableEngine = new OneToManyWithAssociationTableEngine<>(
					associationConfiguration.srcPersister,
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
			
			ManyToManyRelation<SRC, TRGT, TRGTID, C1, C2> relation = associationConfiguration.manyToManyRelation;
			String indexingColumnName = nullable(relation.getIndexingColumnName()).getOr(() -> associationConfiguration.getIndexColumnNamingStrategy().giveName(accessorDefinition));
			
			// we don't create foreign key for table-per-class because source columns should reference different tables (the one
			// per entity) which is not allowed by databases
			boolean createOneSideForeignKey = !relation.isSourceTablePerClassPolymorphic();
			boolean createManySideForeignKey = !relation.isTargetTablePerClassPolymorphic();
			// NB: index column is part of the primary key
			ASSOCIATIONTABLE intermediaryTable = (ASSOCIATIONTABLE) new IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>(
					associationConfiguration.leftPrimaryKey.getTable().getSchema(),
					associationTableName,
					associationConfiguration.leftPrimaryKey,
					rightPrimaryKey,
					accessorDefinition,
					associationTableNamingStrategy,
					associationConfiguration.foreignKeyNamingStrategy,
					createOneSideForeignKey,
					createManySideForeignKey,
					indexingColumnName);
			
			intermediaryTable.addForeignKey(associationConfiguration.foreignKeyNamingStrategy::giveName,
					intermediaryTable.getOneSideForeignKey(), associationConfiguration.leftPrimaryKey);
			
			AssociationRecordPersister<IndexedAssociationRecord, ASSOCIATIONTABLE> indexedAssociationPersister =
					new AssociationRecordPersister<>(
							new IndexedAssociationRecordMapping<>(intermediaryTable,
									associationConfiguration.srcPersister.getMapping().getIdMapping().getIdentifierAssembler(),
									targetPersister.getMapping().getIdMapping().getIdentifierAssembler(),
									intermediaryTable.getLeftIdentifierColumnMapping(),
									intermediaryTable.getRightIdentifierColumnMapping()),
							dialect,
							connectionConfiguration);
			associationTableEngine = new OneToManyWithIndexedAssociationTableEngine<>(
					associationConfiguration.srcPersister,
					targetPersister,
					manyRelationDescriptor,
					indexedAssociationPersister,
					intermediaryTable.getIndexColumn(),
					dialect.getWriteOperationFactory());
		}
	}
}
