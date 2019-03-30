package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Reflections;
import org.gama.reflection.AccessorByMember;
import org.gama.reflection.AccessorByMethod;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.MutatorByMember;
import org.gama.reflection.MutatorByMethod;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.builder.CascadeManyList;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.AbstractOneToManyWithAssociationTableEngine;
import org.gama.stalactite.persistence.engine.runtime.IndexedMappedManyRelationDescriptor;
import org.gama.stalactite.persistence.engine.runtime.ManyRelationDescriptor;
import org.gama.stalactite.persistence.engine.runtime.MappedManyRelationDescriptor;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithAssociationTableEngine;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithIndexedAssociationTableEngine;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithIndexedMappedAssociationEngine;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.collection.Iterables.first;
import static org.gama.reflection.Accessors.of;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode.ALL_ORPHAN_REMOVAL;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode.ASSOCIATION_ONLY;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode.READ_ONLY;

/**
 * @param <SRC> type of input (left/source entities)
 * @param <TRGT> type of output (right/target entities)
 * @param <SRCID> identifier type of source entities
 * @param <TRGTID> identifier type of target entities
 * @param <C> collection type of the relation
 * @author Guillaume Mary
 */
public class CascadeManyConfigurer<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>> {
	
	public <T extends Table<T>> void appendCascade(CascadeMany<SRC, TRGT, TRGTID, C> cascadeMany,
												   JoinedTablesPersister<SRC, SRCID, T> joinedTablesPersister,
												   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
												   AssociationTableNamingStrategy associationTableNamingStrategy,
												   Dialect dialect) {
		Persister<TRGT, TRGTID, ?> targetPersister = cascadeMany.getPersister();
		
		// adding persistence flag setters on both sides : this could be done by Persiter itself,
		// but we would loose the reason why it does it : the cascade functionnality
		joinedTablesPersister.getPersisterListener().addInsertListener(
				joinedTablesPersister.getMappingStrategy().getIdMappingStrategy().getIdentifierInsertionManager().getInsertListener());
		targetPersister.getPersisterListener().addInsertListener(
				targetPersister.getMappingStrategy().getIdMappingStrategy().getIdentifierInsertionManager().getInsertListener());
		
		// finding joined columns: left one is primary key. Right one is given by the target strategy through the property accessor
		if (joinedTablesPersister.getMainTable().getPrimaryKey().getColumns().size() > 1) {
			throw new NotYetSupportedOperationException("Joining tables on a composed primary key is not (yet) supported");
		}
		Column leftPrimaryKey = first(joinedTablesPersister.getMainTable().getPrimaryKey().getColumns());
		
		RelationshipMode maintenanceMode = cascadeMany.getRelationshipMode();
		// selection is always present (else configuration is nonsense !)
		boolean orphanRemoval = maintenanceMode == ALL_ORPHAN_REMOVAL;
		boolean writeAuthorized = maintenanceMode != READ_ONLY;
		
		ManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C> manyAssociationConfiguration = new ManyAssociationConfiguration<>(cascadeMany,
				joinedTablesPersister, targetPersister, leftPrimaryKey, foreignKeyNamingStrategy,
				orphanRemoval, writeAuthorized);
		if (cascadeMany.getReverseSetter() == null && cascadeMany.getReverseGetter() == null && cascadeMany.getReverseColumn() == null) {
			new CascadeManyWithAssociationTableConfigurer<>(manyAssociationConfiguration, associationTableNamingStrategy, dialect).configure();
		} else {
			// case : reverse property is defined through one of the setter, getter or column on the reverse side
			new CascadeManyWithMappedAssociationConfigurer<>(manyAssociationConfiguration, maintenanceMode).configure();
		}
	}
	
	/**
	 * Class that stores elements necessary to one-to-many association configuration
	 */
	private static class ManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>> {
		
		private final CascadeMany<SRC, TRGT, TRGTID, C> cascadeMany;
		private final JoinedTablesPersister<SRC, SRCID, ?> joinedTablesPersister;
		private final Persister<TRGT, TRGTID, ?> targetPersister;
		private final Column leftPrimaryKey;
		private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
		private final Function<SRC, C> collectionGetter;
		private final MutatorByMember<SRC, C, ? extends Member> setter;
		private final boolean orphanRemoval;
		private final boolean writeAuthorized;
		
		public ManyAssociationConfiguration(CascadeMany<SRC, TRGT, TRGTID, C> cascadeMany,
											JoinedTablesPersister<SRC, SRCID, ?> joinedTablesPersister,
											Persister<TRGT, TRGTID, ?> targetPersister,
											Column leftPrimaryKey,
											ForeignKeyNamingStrategy foreignKeyNamingStrategy,
											boolean orphanRemoval,
											boolean writeAuthorized) {
			this.cascadeMany = cascadeMany;
			this.joinedTablesPersister = joinedTablesPersister;
			this.targetPersister = targetPersister;
			this.leftPrimaryKey = leftPrimaryKey;
			this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
			this.collectionGetter = cascadeMany.getCollectionProvider();
			this.setter = new AccessorByMethod<SRC, C>(cascadeMany.getCollectionGetter()).toMutator();
			this.orphanRemoval = orphanRemoval;
			this.writeAuthorized = writeAuthorized;
		}
	}
	
	/**
	 * Configurer dedicated to association that needs an intermediary table between source entities and these of the relation
	 */
	private static class CascadeManyWithAssociationTableConfigurer<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>> {
		
		private final AssociationTableNamingStrategy associationTableNamingStrategy;
		private final Dialect dialect;
		private final ManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C> manyAssociationConfiguration;
		
		private CascadeManyWithAssociationTableConfigurer(ManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C> manyAssociationConfiguration,
														  AssociationTableNamingStrategy associationTableNamingStrategy,
														  Dialect dialect) {
			this.manyAssociationConfiguration = manyAssociationConfiguration;
			this.associationTableNamingStrategy = associationTableNamingStrategy;
			this.dialect = dialect;
		}
		
		private void configure() {
			// case : Collection mapping without reverse property : an association table is needed
			Table<?> rightTable = manyAssociationConfiguration.targetPersister.getMappingStrategy().getTargetTable();
			Column rightPrimaryKey = first(rightTable.getPrimaryKey().getColumns());
			
			String associationTableName = associationTableNamingStrategy.giveName(manyAssociationConfiguration.cascadeMany.getCollectionGetter(),
					manyAssociationConfiguration.leftPrimaryKey, rightPrimaryKey);
			AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C, ? extends AssociationRecord, ? extends AssociationTable> oneToManyWithAssociationTableEngine;
			ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor = new ManyRelationDescriptor<>(manyAssociationConfiguration.collectionGetter,
					manyAssociationConfiguration.setter::set, manyAssociationConfiguration.cascadeMany.getCollectionTargetClass());
			if (manyAssociationConfiguration.cascadeMany instanceof CascadeManyList) {
				oneToManyWithAssociationTableEngine = configureIndexedAssociation(rightPrimaryKey, associationTableName, manyRelationDescriptor);
			} else {
				oneToManyWithAssociationTableEngine = configureNonIndexedAssociation(rightPrimaryKey, associationTableName, manyRelationDescriptor);
			}
			
			oneToManyWithAssociationTableEngine.addSelectCascade(manyAssociationConfiguration.joinedTablesPersister);
			if (manyAssociationConfiguration.writeAuthorized) {
				oneToManyWithAssociationTableEngine.addInsertCascade();
				oneToManyWithAssociationTableEngine.addUpdateCascade(manyAssociationConfiguration.orphanRemoval);
				oneToManyWithAssociationTableEngine.addDeleteCascade(manyAssociationConfiguration.orphanRemoval, dialect.getColumnBinderRegistry());
			}
		}
		
		private AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C, ? extends AssociationRecord, ? extends AssociationTable>
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
					manyAssociationConfiguration.joinedTablesPersister.getConnectionProvider(),
					manyAssociationConfiguration.joinedTablesPersister.getBatchSize());
			return new OneToManyWithAssociationTableEngine<>(
					manyAssociationConfiguration.joinedTablesPersister,
					manyAssociationConfiguration.targetPersister,
					manyRelationDescriptor,
					associationPersister);
		}
		
		private AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C, ? extends AssociationRecord, ? extends AssociationTable>
		configureIndexedAssociation(Column rightPrimaryKey, String associationTableName, ManyRelationDescriptor manyRelationDescriptor) {
			
			if (((CascadeManyList) manyAssociationConfiguration.cascadeMany).getIndexingColumn() != null) {
				throw new UnsupportedOperationException("Indexing column is defined without owner : relation is only declared by "
						+ Reflections.toString(manyAssociationConfiguration.cascadeMany.getCollectionGetter()));
			}
			
			// NB: index column is part of the primary key
			AssociationTable intermediaryTable = new IndexedAssociationTable(null,
					associationTableName,
					manyAssociationConfiguration.leftPrimaryKey,
					rightPrimaryKey,
					associationTableNamingStrategy,
					manyAssociationConfiguration.foreignKeyNamingStrategy);
			AssociationRecordPersister<IndexedAssociationRecord, IndexedAssociationTable> indexedAssociationPersister =
					new AssociationRecordPersister<>(
							new IndexedAssociationRecordMappingStrategy((IndexedAssociationTable) intermediaryTable),
							dialect,
							manyAssociationConfiguration.joinedTablesPersister.getConnectionProvider(),
							manyAssociationConfiguration.joinedTablesPersister.getBatchSize());
			return new OneToManyWithIndexedAssociationTableEngine<>(
					manyAssociationConfiguration.joinedTablesPersister,
					manyAssociationConfiguration.targetPersister,
					manyRelationDescriptor,
					indexedAssociationPersister);
		}
	}
	
	/**
	 * Configurer dedicated to association that are mapped on reverse side by a property and a column on table's target entities
	 */
	private static class CascadeManyWithMappedAssociationConfigurer<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>> {
		
		private final RelationshipMode maintenanceMode;
		private final ManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C> manyAssociationConfiguration;
		
		private CascadeManyWithMappedAssociationConfigurer(ManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C> manyAssociationConfiguration,
														   RelationshipMode maintenanceMode) {
			this.manyAssociationConfiguration = manyAssociationConfiguration;
			this.maintenanceMode = maintenanceMode;
		}
		
		private void configure() {
			if (maintenanceMode == ASSOCIATION_ONLY) {
				throw new MappingConfigurationException(RelationshipMode.ASSOCIATION_ONLY + " is only relevent with an association table");
			}
			
			// We're looking for the foreign key (for necessary join) and for getter/setter required to manage the relation 
			Column foreignKey = manyAssociationConfiguration.cascadeMany.getReverseColumn();
			Method reverseMethod = null;
			String getterSignature = null;
			// Setter for applying source entity to reverse side (target entities)
			SerializableBiConsumer<TRGT, SRC> reverseSetter = null;
			SerializableFunction<TRGT, SRC> reverseGetter = null;
			if (foreignKey == null) {
				// Here reverse side is surely defined by method reference (because of assertion some lines upper), we look for the matching column
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
				PropertyAccessor<TRGT, SRC> reversePropertyAccessor = of(reverseMethod);
				foreignKey = manyAssociationConfiguration.targetPersister.getMappingStrategy().getMainMappingStrategy().getPropertyToColumn().get(reversePropertyAccessor);
				if (foreignKey == null) {
					// This should not happen, left for bug safety
					throw new NotYetSupportedOperationException("Can't build a relation with on a non mapped property, please add the mapping of a "
							+ Reflections.toString(manyAssociationConfiguration.joinedTablesPersister.getMappingStrategy().getClassToPersist())
							+ " to persister of " + Reflections.toString(manyAssociationConfiguration.cascadeMany.getPersister().getMappingStrategy().getClassToPersist()));
				}
			}
			
			if (manyAssociationConfiguration.cascadeMany instanceof CascadeManyList) {
				if (reverseGetter == null) {
					throw new UnsupportedOperationException("Indexed collection without getter is not supporter : relation is mapped by "
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
			foreignKey.getTable().addForeignKey(manyAssociationConfiguration.foreignKeyNamingStrategy.giveName(foreignKey,
					manyAssociationConfiguration.leftPrimaryKey), foreignKey, manyAssociationConfiguration.leftPrimaryKey);
			
			// we have a direct relation : relationship is owned by target table as a foreign key
			OneToManyWithMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C> mappedAssociationEngine;
			if (manyAssociationConfiguration.cascadeMany instanceof CascadeManyList) {
				mappedAssociationEngine = configureIndexedAssociation(getterSignature, reverseSetter, reverseGetter);
			} else {
				mappedAssociationEngine = configureNonIndexedAssociation(reverseSetter);
			}
			mappedAssociationEngine.addSelectCascade(manyAssociationConfiguration.leftPrimaryKey, foreignKey);
			if (manyAssociationConfiguration.writeAuthorized) {
				mappedAssociationEngine.addInsertCascade();
				mappedAssociationEngine.addUpdateCascade(manyAssociationConfiguration.orphanRemoval);
				mappedAssociationEngine.addDeleteCascade(manyAssociationConfiguration.orphanRemoval);
			}
		}
		
		private OneToManyWithMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C> configureNonIndexedAssociation(BiConsumer<TRGT, SRC> reverseSetter) {
			OneToManyWithMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C> mappedAssociationEngine;
			MappedManyRelationDescriptor<SRC, TRGT, C> manyRelationDefinition = new MappedManyRelationDescriptor<>(
					manyAssociationConfiguration.collectionGetter, manyAssociationConfiguration.setter::set,
					manyAssociationConfiguration.cascadeMany.getCollectionTargetClass(), reverseSetter);
			mappedAssociationEngine = new OneToManyWithMappedAssociationEngine<>(
					manyAssociationConfiguration.targetPersister,
					manyRelationDefinition,
					manyAssociationConfiguration.joinedTablesPersister);
			return mappedAssociationEngine;
		}
		
		private OneToManyWithMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C> configureIndexedAssociation(String getterSignature, BiConsumer<TRGT, SRC> reverseSetter,
																									  SerializableFunction<TRGT, SRC> reverseGetter) {
			OneToManyWithMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C> mappedAssociationEngine;
			IndexedMappedManyRelationDescriptor<SRC, TRGT, C> manyRelationDefinition = new IndexedMappedManyRelationDescriptor<>(
					manyAssociationConfiguration.collectionGetter, manyAssociationConfiguration.setter::set,
					manyAssociationConfiguration.cascadeMany.getCollectionTargetClass(), reverseSetter, reverseGetter, getterSignature);
			mappedAssociationEngine = (OneToManyWithMappedAssociationEngine) new OneToManyWithIndexedMappedAssociationEngine<>(
					manyAssociationConfiguration.targetPersister,
					(IndexedMappedManyRelationDescriptor) manyRelationDefinition,
					manyAssociationConfiguration.joinedTablesPersister,
					((CascadeManyList) manyAssociationConfiguration.cascadeMany).getIndexingColumn()
			);
			return mappedAssociationEngine;
		}
	}
}
