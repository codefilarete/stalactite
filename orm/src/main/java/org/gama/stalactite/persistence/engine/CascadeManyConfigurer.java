package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.gama.lang.Reflections;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.SetPersistedFlagAfterInsertListener;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.builder.CascadeManyList;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.runtime.AbstractOneToManyWithAssociationTableEngine;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithAssociationTableEngine;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithIndexedAssociationTableEngine;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.collection.Iterables.first;
import static org.gama.reflection.Accessors.of;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode.ALL_ORPHAN_REMOVAL;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode.ASSOCIATION_ONLY;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode.READ_ONLY;

/**
 * @author Guillaume Mary
 */
public class CascadeManyConfigurer<I extends Identified, O extends Identified, J extends Identifier, C extends Collection<O>> {
	
	private AssociationRecordPersister<AssociationRecord, AssociationTable> associationPersister;
	private AssociationRecordPersister<IndexedAssociationRecord, IndexedAssociationTable> indexedAssociationPersister;
	private Column<? extends AssociationTable, Object> pointerToLeftColumn;
	private Dialect dialect;
	private AssociationTable intermediaryTable;
	/** Setter for applying source entity to reverse side (target entity). Available only when association is mapped without intermediary table */
	private BiConsumer<O, I> reverseSetter = null;
	
	public <T extends Table<T>> void appendCascade(CascadeMany<I, O, J, C> cascadeMany,
												   JoinedTablesPersister<I, J, T> joinedTablesPersister,
												   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
												   AssociationTableNamingStrategy associationTableNamingStrategy, Dialect dialect) {
		this.dialect = dialect;
		Persister<O, J, ?> leftPersister = cascadeMany.getPersister();
		
		// adding persistence flag setters on both side
		joinedTablesPersister.getPersisterListener().addInsertListener((InsertListener<I>) SetPersistedFlagAfterInsertListener.INSTANCE);
		leftPersister.getPersisterListener().addInsertListener((InsertListener<O>) SetPersistedFlagAfterInsertListener.INSTANCE);
		
		// finding joined columns: left one is primary key. Right one is given by the target strategy through the property accessor
		if (joinedTablesPersister.getTargetTable().getPrimaryKey().getColumns().size() > 1) {
			throw new NotYetSupportedOperationException("Joining tables on a composed primary key is not (yet) supported");
		}
		Column leftPrimaryKey = first(joinedTablesPersister.getTargetTable().getPrimaryKey().getColumns());
		
		Column rightJoinColumn;
		if (cascadeMany.getReverseSetter() == null && cascadeMany.getReverseGetter() == null && cascadeMany.getReverseColumn() == null) {
			// case : Collection mapping without reverse property : an association table is needed
			Table<?> rightTable = leftPersister.getMappingStrategy().getTargetTable();
			Column rightPrimaryKey = first(rightTable.getPrimaryKey().getColumns());
			
			String associationTableName = associationTableNamingStrategy.giveName(cascadeMany.getMember(), leftPrimaryKey, rightPrimaryKey);
			if (cascadeMany instanceof CascadeManyList) {
				
				if (((CascadeManyList) cascadeMany).getIndexingColumn() != null) {
					throw new UnsupportedOperationException("Indexing column id defined but not owner is defined : relation is only mapped by "
							+ Reflections.toString(cascadeMany.getMember()));
				}
				
				// NB: index column is part of the primary key
				intermediaryTable = new IndexedAssociationTable(null, associationTableName, leftPrimaryKey, rightPrimaryKey,
						associationTableNamingStrategy, foreignKeyNamingStrategy);
				pointerToLeftColumn = intermediaryTable.getOneSideKeyColumn();
				indexedAssociationPersister = new AssociationRecordPersister<>(
						new IndexedAssociationRecordMappingStrategy((IndexedAssociationTable) intermediaryTable),
						dialect,
						joinedTablesPersister.getConnectionProvider(),
						joinedTablesPersister.getBatchSize());
			} else {
				intermediaryTable = new AssociationTable(null, associationTableName, leftPrimaryKey, rightPrimaryKey, associationTableNamingStrategy,
						foreignKeyNamingStrategy);
				pointerToLeftColumn = intermediaryTable.getOneSideKeyColumn();
				associationPersister = new AssociationRecordPersister<>(
						new AssociationRecordMappingStrategy(intermediaryTable),
						dialect, 
						joinedTablesPersister.getConnectionProvider(),
						joinedTablesPersister.getBatchSize());
			}
			
			// for further select usage, see far below
			rightJoinColumn = rightPrimaryKey;
		} else {
			// case : reverse property is defined through one of the setter, getter or column on the reverse side
			// We're looking for the foreign key (no association table needed)
			// Determining right side column
			Column foreignKey = cascadeMany.getReverseColumn();
			
			Method reverseMember = null;
			if (foreignKey == null) {
				// Here reverse side is surely defined by method reference (because of assertion some lines upper), we look for the matching column
				MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer();
				if (cascadeMany.getReverseSetter() != null) {
					reverseMember = methodReferenceCapturer.findMethod(cascadeMany.getReverseSetter());
				} else {
					reverseMember = methodReferenceCapturer.findMethod(cascadeMany.getReverseGetter());
				}
				reverseSetter = of(reverseMember)::set;
				foreignKey = leftPersister.getMappingStrategy().getMainMappingStrategy().getPropertyToColumn().get(of(reverseMember));
				if (foreignKey == null) {
					// This should not happen, left for bug safety
					throw new NotYetSupportedOperationException("Reverse side mapping is not declared, please add the mapping of a "
							+ Reflections.toString(joinedTablesPersister.getMappingStrategy().getClassToPersist())
							+ " to persister of " + cascadeMany.getPersister().getMappingStrategy().getClassToPersist().getName());
				}
			}
			
			if (cascadeMany instanceof CascadeManyList && ((CascadeManyList) cascadeMany).getIndexingColumn() == null) {
				throw new UnsupportedOperationException("Missing indexing column : relation is mapped by "
						+ (reverseMember != null ? Reflections.toString(reverseMember) : cascadeMany.getReverseColumn())
						+ " but no indexing property is defined");
			}
			
			// adding foreign key constraint
			foreignKey.getTable().addForeignKey(foreignKeyNamingStrategy.giveName(foreignKey, leftPrimaryKey), foreignKey, leftPrimaryKey);
			
			// for further select usage, see far below
			rightJoinColumn = foreignKey;
		}
		
		// managing cascades
		Function<I, C> collectionGetter = cascadeMany.getTargetProvider();
		PersisterListener<I, J> persisterListener = joinedTablesPersister.getPersisterListener();
		
		RelationshipMode maintenanceMode = cascadeMany.getRelationshipMode();
		if (maintenanceMode == ASSOCIATION_ONLY && intermediaryTable == null) {
			throw new MappingConfigurationException(RelationshipMode.ASSOCIATION_ONLY + " is only relevent with an association table");
		}
		// selection is always present (else configuration is nonsense !)
		boolean orphanRemoval = maintenanceMode == ALL_ORPHAN_REMOVAL;
		AbstractOneToManyWithAssociationTableEngine<I, O, J, C, ? extends AssociationRecord, ? extends AssociationTable> oneToManyWithAssociationTableEngine;
		if (associationPersister != null) {
			oneToManyWithAssociationTableEngine = new OneToManyWithAssociationTableEngine(associationPersister);
			oneToManyWithAssociationTableEngine.addSelectCascade(
					cascadeMany, joinedTablesPersister, leftPersister, leftPrimaryKey, collectionGetter, rightJoinColumn, persisterListener);
			if (maintenanceMode != READ_ONLY) {
				oneToManyWithAssociationTableEngine.addInsertCascade(cascadeMany, leftPersister, collectionGetter, persisterListener);
				oneToManyWithAssociationTableEngine.addUpdateCascade(cascadeMany, leftPersister, collectionGetter, persisterListener,
						orphanRemoval);
				oneToManyWithAssociationTableEngine.addDeleteCascade(cascadeMany, joinedTablesPersister, leftPersister, collectionGetter,
						persisterListener, orphanRemoval, this.dialect, pointerToLeftColumn);
			}
		} else if (indexedAssociationPersister != null) {
			oneToManyWithAssociationTableEngine = new OneToManyWithIndexedAssociationTableEngine(indexedAssociationPersister);
			oneToManyWithAssociationTableEngine.addSelectCascade(
					(CascadeManyList) cascadeMany,
					joinedTablesPersister, leftPersister, leftPrimaryKey, (Function) collectionGetter, rightJoinColumn, persisterListener);
			if (maintenanceMode != READ_ONLY) {
				oneToManyWithAssociationTableEngine.addInsertCascade((CascadeManyList) cascadeMany, leftPersister, (Function) collectionGetter, persisterListener);
				oneToManyWithAssociationTableEngine.addUpdateCascade((CascadeManyList) cascadeMany, leftPersister, (Function) collectionGetter, persisterListener, orphanRemoval);
				oneToManyWithAssociationTableEngine.addDeleteCascade((CascadeManyList) cascadeMany, joinedTablesPersister, leftPersister, (Function) collectionGetter,
						persisterListener, orphanRemoval, this.dialect, pointerToLeftColumn);
			}
		} else {
			// we have a direct relation : relationship is owned by target table as a foreign key
			OneToManyWithMappedAssociationEngine mappedAssociationEngine = new OneToManyWithMappedAssociationEngine(reverseSetter);
			mappedAssociationEngine.addSelectCascade(cascadeMany, joinedTablesPersister, leftPersister, leftPrimaryKey, collectionGetter, rightJoinColumn, persisterListener);
			if (maintenanceMode != READ_ONLY) {
				mappedAssociationEngine.addInsertCascade(cascadeMany, leftPersister, collectionGetter, persisterListener);
				mappedAssociationEngine.addUpdateCascade(cascadeMany, leftPersister, collectionGetter, persisterListener, orphanRemoval);
				mappedAssociationEngine.addDeleteCascade(cascadeMany, joinedTablesPersister, leftPersister, collectionGetter,
						persisterListener, orphanRemoval, this.dialect, pointerToLeftColumn);
			}
		}
	}
}
