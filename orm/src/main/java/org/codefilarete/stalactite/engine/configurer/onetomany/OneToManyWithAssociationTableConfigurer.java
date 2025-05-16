package org.codefilarete.stalactite.engine.configurer.onetomany;

import java.util.Collection;

import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.AssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.IndexedAssociationRecordMapping;
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

/**
 * Configurer dedicated to association that needs an intermediary table between source and target entities
 * @author Guillaume Mary
 */
class OneToManyWithAssociationTableConfigurer<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>,
		LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
		extends OneToManyConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, C, LEFTTABLE> {
	
	private final AssociationTableNamingStrategy associationTableNamingStrategy;
	private final Dialect dialect;
	private final boolean maintainAssociationOnly;
	private final ConnectionConfiguration connectionConfiguration;
	
	private AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C, ? extends AssociationRecord, ?> associationTableEngine;
	
	OneToManyWithAssociationTableConfigurer(OneToManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C, LEFTTABLE> associationConfiguration,
											boolean loadSeparately,
											AssociationTableNamingStrategy associationTableNamingStrategy,
											boolean maintainAssociationOnly,
											Dialect dialect,
											ConnectionConfiguration connectionConfiguration) {
		super(associationConfiguration, loadSeparately);
		this.associationTableNamingStrategy = associationTableNamingStrategy;
		this.dialect = dialect;
		this.maintainAssociationOnly = maintainAssociationOnly;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	@Override
	protected void configure(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		prepare(targetPersister);
		associationTableEngine.addSelectCascade(associationConfiguration.getSrcPersister(), loadSeparately);
		addWriteCascades(associationTableEngine, targetPersister);
	}
	
	private void prepare(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		// case : Collection mapping without reverse property : an association table is needed
		PrimaryKey<RIGHTTABLE, TRGTID> rightPrimaryKey = targetPersister.<RIGHTTABLE>getMapping().getTargetTable().getPrimaryKey();
		
		String associationTableName = associationTableNamingStrategy.giveName(associationConfiguration.getAccessorDefinition(),
				associationConfiguration.getLeftPrimaryKey(), rightPrimaryKey);
		if (associationConfiguration.getOneToManyRelation().isOrdered()) {
			assignEngineForIndexedAssociation(rightPrimaryKey, associationTableName, targetPersister);
		} else {
			assignEngineForNonIndexedAssociation(rightPrimaryKey, associationTableName, targetPersister);
		}
	}
	
	@Override
	public CascadeConfigurationResult<SRC, TRGT> configureWithSelectIn2Phases(String tableAlias,
																			  ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
																			  FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener) {
		prepare(targetPersister);
		associationTableEngine.addSelectCascadeIn2Phases(firstPhaseCycleLoadListener);
		addWriteCascades(associationTableEngine, targetPersister);
		return new CascadeConfigurationResult<>(associationTableEngine.getManyRelationDescriptor().getRelationFixer(), associationConfiguration.getSrcPersister());
	}
	
	private void addWriteCascades(AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C, ? extends AssociationRecord, ? extends AssociationTable> oneToManyWithAssociationTableEngine,
								  ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
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
			ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		
		// we don't create foreign key for table-per-class because source columns should reference different tables (the one
		// per entity) which is not allowed by databases
		boolean createOneSideForeignKey = !(associationConfiguration.getOneToManyRelation().isSourceTablePerClassPolymorphic());
		boolean createManySideForeignKey = !associationConfiguration.getOneToManyRelation().isTargetTablePerClassPolymorphic();
		ASSOCIATIONTABLE intermediaryTable = (ASSOCIATIONTABLE) new AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>(
				associationConfiguration.getLeftPrimaryKey().getTable().getSchema(),
				associationTableName,
				associationConfiguration.getLeftPrimaryKey(),
				rightPrimaryKey,
				associationConfiguration.getAccessorDefinition(),
				associationTableNamingStrategy,
				associationConfiguration.getForeignKeyNamingStrategy(),
				createOneSideForeignKey,
				createManySideForeignKey
		);
		
		associationConfiguration.getSrcPersister().getMapping().getIdMapping();
		targetPersister.getMapping().getIdMapping();
		AssociationRecordPersister<AssociationRecord, ASSOCIATIONTABLE> associationPersister = new AssociationRecordPersister<>(
				new AssociationRecordMapping<>(
						intermediaryTable,
						associationConfiguration.getSrcPersister().getMapping().getIdMapping().getIdentifierAssembler(),
						targetPersister.getMapping().getIdMapping().getIdentifierAssembler(),
						intermediaryTable.getLeftIdentifierColumnMapping(),
						intermediaryTable.getRightIdentifierColumnMapping()),
				dialect,
				connectionConfiguration);
		ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor = new ManyRelationDescriptor<>(
				associationConfiguration.getCollectionGetter()::get,
				associationConfiguration.getSetter()::set,
				associationConfiguration.getCollectionFactory(),
				associationConfiguration.getOneToManyRelation().getReverseLink());
		associationTableEngine = new OneToManyWithAssociationTableEngine<>(
				associationConfiguration.getSrcPersister(),
				targetPersister,
				manyRelationDescriptor,
				associationPersister,
				dialect.getWriteOperationFactory());
	}
	
	private <ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	void assignEngineForIndexedAssociation(PrimaryKey<RIGHTTABLE, TRGTID> rightPrimaryKey,
										   String associationTableName,
										   ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		
		// we don't create foreign key for table-per-class because source columns should reference different tables (the one
		// per entity) which is not allowed by databases
		boolean createOneSideForeignKey = !(associationConfiguration.getOneToManyRelation().isSourceTablePerClassPolymorphic());
		boolean createManySideForeignKey = !associationConfiguration.getOneToManyRelation().isTargetTablePerClassPolymorphic();
		// NB: index column is part of the primary key
		ASSOCIATIONTABLE intermediaryTable = (ASSOCIATIONTABLE) new IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>(
				associationConfiguration.getLeftPrimaryKey().getTable().getSchema(),
				associationTableName,
				associationConfiguration.getLeftPrimaryKey(),
				rightPrimaryKey,
				associationConfiguration.getAccessorDefinition(),
				associationTableNamingStrategy,
				associationConfiguration.getForeignKeyNamingStrategy(),
				createOneSideForeignKey,
				createManySideForeignKey,
				associationConfiguration.getOneToManyRelation().getIndexingColumn());
		
		AssociationRecordPersister<IndexedAssociationRecord, ASSOCIATIONTABLE> indexedAssociationPersister =
				new AssociationRecordPersister<>(
						new IndexedAssociationRecordMapping<>(intermediaryTable,
								associationConfiguration.getSrcPersister().getMapping().getIdMapping().getIdentifierAssembler(),
								targetPersister.getMapping().getIdMapping().getIdentifierAssembler(),
								intermediaryTable.getLeftIdentifierColumnMapping(),
								intermediaryTable.getRightIdentifierColumnMapping()),
						dialect,
						connectionConfiguration);
		ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor = new ManyRelationDescriptor<>(
				associationConfiguration.getCollectionGetter()::get, associationConfiguration.getSetter()::set,
				associationConfiguration.getCollectionFactory(),
				associationConfiguration.getOneToManyRelation().getReverseLink());
		associationTableEngine = new OneToManyWithIndexedAssociationTableEngine<>(
				associationConfiguration.getSrcPersister(),
				targetPersister,
				manyRelationDescriptor,
				indexedAssociationPersister,
				intermediaryTable.getIndexColumn(),
				dialect.getWriteOperationFactory());
	}
}
