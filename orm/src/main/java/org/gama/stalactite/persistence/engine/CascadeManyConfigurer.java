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
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.SetPersistedFlagAfterInsertListener;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.builder.CascadeManyList;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.runtime.AbstractOneToManyWithAssociationTableEngine;
import org.gama.stalactite.persistence.engine.runtime.IndexedMappedManyRelationDescriptor;
import org.gama.stalactite.persistence.engine.runtime.ManyRelationDescriptor;
import org.gama.stalactite.persistence.engine.runtime.MappedManyRelationDescriptor;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithAssociationTableEngine;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithIndexedAssociationTableEngine;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithIndexedMappedAssociationEngine;
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
 * @param <I> type of input (left/source entities)
 * @param <O> type of output (right/target entities)
 * @param <J> identifier type of target entities
 * @param <C> collection type of the relation
 */
public class CascadeManyConfigurer<I extends Identified, O extends Identified, J extends Identifier, C extends Collection<O>> {
	
	public <T extends Table<T>> void appendCascade(CascadeMany<I, O, J, C> cascadeMany,
												   JoinedTablesPersister<I, J, T> joinedTablesPersister,
												   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
												   AssociationTableNamingStrategy associationTableNamingStrategy, Dialect dialect) {
		Persister<O, J, ?> targetPersister = cascadeMany.getPersister();
		
		// adding persistence flag setters on both side
		joinedTablesPersister.getPersisterListener().addInsertListener((InsertListener<I>) SetPersistedFlagAfterInsertListener.INSTANCE);
		targetPersister.getPersisterListener().addInsertListener((InsertListener<O>) SetPersistedFlagAfterInsertListener.INSTANCE);
		
		// finding joined columns: left one is primary key. Right one is given by the target strategy through the property accessor
		if (joinedTablesPersister.getTargetTable().getPrimaryKey().getColumns().size() > 1) {
			throw new NotYetSupportedOperationException("Joining tables on a composed primary key is not (yet) supported");
		}
		Column leftPrimaryKey = first(joinedTablesPersister.getTargetTable().getPrimaryKey().getColumns());
		
		RelationshipMode maintenanceMode = cascadeMany.getRelationshipMode();
		// selection is always present (else configuration is nonsense !)
		boolean orphanRemoval = maintenanceMode == ALL_ORPHAN_REMOVAL;
		boolean writeAuthorized = maintenanceMode != READ_ONLY;
		
		ManyAssociationConfiguration<I, O, J, C> manyAssociationConfiguration = new ManyAssociationConfiguration<>(cascadeMany,
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
	private static class ManyAssociationConfiguration<
			I extends Identified,
			O extends Identified,
			J extends Identifier,
			C extends Collection<O>> {
		
		private final CascadeMany<I, O, J, C> cascadeMany;
		private final JoinedTablesPersister<I, J, ?> joinedTablesPersister;
		private final Persister<O, J, ?> targetPersister;
		private final Column leftPrimaryKey;
		private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
		private final Function<I, C> collectionGetter;
		private final MutatorByMember<I, C, ? extends Member> setter;
		private final PersisterListener<I, J> persisterListener;
		private final boolean orphanRemoval;
		private final boolean writeAuthorized;
		
		public ManyAssociationConfiguration(CascadeMany<I, O, J, C> cascadeMany,
											JoinedTablesPersister<I, J, ?> joinedTablesPersister,
											Persister<O, J, ?> targetPersister,
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
			this.setter = new AccessorByMethod<I, C>(cascadeMany.getCollectionGetter()).toMutator();
			this.persisterListener = joinedTablesPersister.getPersisterListener();
			this.orphanRemoval = orphanRemoval;
			this.writeAuthorized = writeAuthorized;
		}
	}
	
	/**
	 * Configurer dedicated to association that needs an intermediary table between source entities and these of the relation
	 */
	private static class CascadeManyWithAssociationTableConfigurer<
			I extends Identified,
			O extends Identified,
			J extends Identifier,
			C extends Collection<O>> {
		
		private final AssociationTableNamingStrategy associationTableNamingStrategy;
		private final Dialect dialect;
		private final ManyAssociationConfiguration<I, O, J, C> manyAssociationConfiguration;
		
		private CascadeManyWithAssociationTableConfigurer(ManyAssociationConfiguration<I, O, J, C> manyAssociationConfiguration,
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
			AbstractOneToManyWithAssociationTableEngine<I, O, J, C, ? extends AssociationRecord, ? extends AssociationTable> oneToManyWithAssociationTableEngine;
			ManyRelationDescriptor<I, O, C> manyRelationDescriptor = new ManyRelationDescriptor<>(manyAssociationConfiguration.collectionGetter,
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
		
		private AbstractOneToManyWithAssociationTableEngine<I, O, J, C, ? extends AssociationRecord, ? extends AssociationTable>
		configureNonIndexedAssociation(Column rightPrimaryKey, String associationTableName, ManyRelationDescriptor<I, O, C> manyRelationDescriptor) {
			
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
					manyAssociationConfiguration.persisterListener,
					manyAssociationConfiguration.targetPersister,
					manyRelationDescriptor,
					associationPersister);
		}
		
		private AbstractOneToManyWithAssociationTableEngine<I, O, J, C, ? extends AssociationRecord, ? extends AssociationTable>
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
					manyAssociationConfiguration.persisterListener,
					manyAssociationConfiguration.targetPersister,
					manyRelationDescriptor,
					indexedAssociationPersister);
		}
	}
	
	/**
	 * Configurer dedicated to association that are mapped on reverse side by a property and a column on table's target entities
	 */
	private static class CascadeManyWithMappedAssociationConfigurer<
			I extends Identified,
			O extends Identified,
			J extends Identifier,
			C extends Collection<O>> {
		
		private final RelationshipMode maintenanceMode;
		private final ManyAssociationConfiguration<I, O, J, C> manyAssociationConfiguration;
		
		private CascadeManyWithMappedAssociationConfigurer(ManyAssociationConfiguration<I, O, J, C> manyAssociationConfiguration,
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
			SerializableBiConsumer<O, I> reverseSetter = null;
			SerializableFunction<O, I> reverseGetter = null;
			if (foreignKey == null) {
				// Here reverse side is surely defined by method reference (because of assertion some lines upper), we look for the matching column
				MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer();
				if (manyAssociationConfiguration.cascadeMany.getReverseSetter() != null) {
					reverseSetter = manyAssociationConfiguration.cascadeMany.getReverseSetter();
					reverseMethod = methodReferenceCapturer.findMethod(reverseSetter);
					AccessorByMember<O, I, ? extends Member> accessor = new MutatorByMethod<O, I>(reverseMethod).toAccessor();
					reverseGetter = accessor::get;
					getterSignature = accessor.toString();
				} else {
					reverseGetter = manyAssociationConfiguration.cascadeMany.getReverseGetter();
					reverseMethod = methodReferenceCapturer.findMethod(reverseGetter);
					getterSignature = Reflections.toString(reverseMethod);
				}
				PropertyAccessor<O, I> reversePropertyAccessor = of(reverseMethod);
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
			OneToManyWithMappedAssociationEngine<I, O, J, C> mappedAssociationEngine;
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
		
		private OneToManyWithMappedAssociationEngine<I, O, J, C> configureNonIndexedAssociation(BiConsumer<O, I> reverseSetter) {
			OneToManyWithMappedAssociationEngine<I, O, J, C> mappedAssociationEngine;
			MappedManyRelationDescriptor<I, O, C> manyRelationDefinition = new MappedManyRelationDescriptor<>(
					manyAssociationConfiguration.collectionGetter, manyAssociationConfiguration.setter::set,
					manyAssociationConfiguration.cascadeMany.getCollectionTargetClass(), reverseSetter);
			mappedAssociationEngine = new OneToManyWithMappedAssociationEngine<>(
					manyAssociationConfiguration.persisterListener,
					manyAssociationConfiguration.targetPersister,
					manyRelationDefinition,
					manyAssociationConfiguration.joinedTablesPersister);
			return mappedAssociationEngine;
		}
		
		private OneToManyWithMappedAssociationEngine<I, O, J, C> configureIndexedAssociation(String getterSignature, BiConsumer<O, I> reverseSetter,
																							 SerializableFunction<O, I> reverseGetter) {
			OneToManyWithMappedAssociationEngine<I, O, J, C> mappedAssociationEngine;
			IndexedMappedManyRelationDescriptor<I, O, C> manyRelationDefinition = new IndexedMappedManyRelationDescriptor<>(
					manyAssociationConfiguration.collectionGetter, manyAssociationConfiguration.setter::set,
					manyAssociationConfiguration.cascadeMany.getCollectionTargetClass(), reverseSetter, reverseGetter, getterSignature);
			mappedAssociationEngine = (OneToManyWithMappedAssociationEngine) new OneToManyWithIndexedMappedAssociationEngine<>(
					manyAssociationConfiguration.persisterListener,
					manyAssociationConfiguration.targetPersister,
					(IndexedMappedManyRelationDescriptor) manyRelationDefinition,
					manyAssociationConfiguration.joinedTablesPersister,
					((CascadeManyList) manyAssociationConfiguration.cascadeMany).getIndexingColumn()
			);
			return mappedAssociationEngine;
		}
	}
}
