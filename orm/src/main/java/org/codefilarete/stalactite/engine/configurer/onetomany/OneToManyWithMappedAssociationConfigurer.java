package org.codefilarete.stalactite.engine.configurer.onetomany;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.ReversibleMutator;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.runtime.AbstractPolymorphismPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.onetomany.IndexedMappedManyRelationDescriptor;
import org.codefilarete.stalactite.engine.runtime.onetomany.MappedManyRelationDescriptor;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithIndexedMappedAssociationEngine;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Configurer dedicated to association that are mapped on reverse side by a property and a column on table's target entities
 * @author Guillaume Mary
 */
class OneToManyWithMappedAssociationConfigurer<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>,
		LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
		extends OneToManyConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, C, LEFTTABLE> {
	
	private OneToManyWithMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C, RIGHTTABLE> mappedAssociationEngine;
	
	private Key<RIGHTTABLE, SRCID> foreignKey;
	
	private Set<Column<RIGHTTABLE, ?>> mappedReverseColumns;
	
	private Function<SRCID, Map<Column<RIGHTTABLE, ?>, ?>> reverseColumnsValueProvider;
	
	OneToManyWithMappedAssociationConfigurer(OneToManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C, LEFTTABLE> associationConfiguration,
											 boolean loadSeparately) {
		super(associationConfiguration, loadSeparately);
	}
	
	@Override
	protected String configure(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		determineForeignKeyColumns(targetPersister);
		assignAssociationEngine(targetPersister);
		propagateMappedAssociationToSubTables(targetPersister);
		
		String relationJoinNodeName = mappedAssociationEngine.addSelectCascade(associationConfiguration.getLeftPrimaryKey(), loadSeparately);
		addWriteCascades(mappedAssociationEngine, targetPersister);
		return relationJoinNodeName;
	}
	
	public void propagateMappedAssociationToSubTables(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		ForeignKeyNamingStrategy foreignKeyNamingStrategy = associationConfiguration.getForeignKeyNamingStrategy();
		// adding foreign key constraint
		// When source persister is table-per-class, adding a FK from right side (owner) to the several left sides is not possible
		if (!associationConfiguration.getOneToManyRelation().isSourceTablePerClassPolymorphic()) {
			// NB: we ask to add FK to targetPersister because it may be polymorphic (ie contains several tables) so it knows better how to do it
			AbstractPolymorphismPersister<?, ?> targetPersisterAsPolymorphic = AbstractPolymorphismPersister.lookupForPolymorphicPersister(targetPersister);
			if (targetPersisterAsPolymorphic == null) {
				targetPersister.<RIGHTTABLE>getMainTable().addForeignKey(foreignKeyNamingStrategy::giveName,
						foreignKey, associationConfiguration.getSrcPersister().<RIGHTTABLE>getMainTable().getPrimaryKey());
			} else {
				targetPersisterAsPolymorphic.propagateMappedAssociationToSubTables(foreignKey, associationConfiguration.getSrcPersister().getMainTable().getPrimaryKey(), foreignKeyNamingStrategy::giveName);
			}
		}
	}
	
	protected void determineForeignKeyColumns(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		RIGHTTABLE mainTargetTable = targetPersister.getMainTable();
		KeyBuilder<RIGHTTABLE, SRCID> foreignKeyBuilder = Key.from(mainTargetTable);
		OneToManyRelation<SRC, TRGT, TRGTID, C> relation = associationConfiguration.getOneToManyRelation();
		if (!relation.getForeignKeyNameMapping().isEmpty()) {
			Map<Accessor<SRCID, ?>, Column<RIGHTTABLE, ?>> foreignKeyColumnMapping = new HashMap<>();
			relation.getForeignKeyNameMapping().forEach((valueAccessPoint, colName) -> {
				AccessorDefinition localAccessorDefinition = AccessorDefinition.giveDefinition(valueAccessPoint);
				Accessor<SRCID, Object> accessor = valueAccessPoint instanceof Accessor
						? (Accessor) valueAccessPoint
						: (valueAccessPoint instanceof ReversibleMutator ? ((ReversibleMutator) valueAccessPoint).toAccessor() : null);
				if (accessor == null) {
					throw new UnsupportedOperationException("Can't get accessor from " + valueAccessPoint);
				}
				Column<RIGHTTABLE, ?> column = mainTargetTable.addColumn(colName, localAccessorDefinition.getMemberType());
				foreignKeyBuilder.addColumn(column);
				foreignKeyColumnMapping.put(accessor, column);
			});
			mappedReverseColumns = new HashSet<>(foreignKeyColumnMapping.values());
			reverseColumnsValueProvider = srcid -> {
				Map<Column<RIGHTTABLE, ?>, Object> result = new HashMap<>();
				foreignKeyColumnMapping.forEach((accessor, column) -> {
					result.put(column, accessor.get(srcid));
				});
				return result;
			};
		} else if (!relation.getForeignKeyColumnMapping().isEmpty()) {
			Map<Accessor<SRCID, ?>, Column<RIGHTTABLE, ?>> foreignKeyColumnMapping = new HashMap<>();
			relation.getForeignKeyColumnMapping().forEach((valueAccessPoint, column) -> {
				Accessor<SRCID, Object> accessor = valueAccessPoint instanceof Accessor
						? (Accessor) valueAccessPoint
						: (valueAccessPoint instanceof ReversibleMutator ? ((ReversibleMutator) valueAccessPoint).toAccessor() : null);
				if (accessor == null) {
					throw new UnsupportedOperationException("Can't get accessor from " + valueAccessPoint);
				}
				foreignKeyBuilder.addColumn((Column<RIGHTTABLE, ?>) column);
				foreignKeyColumnMapping.put(accessor, (Column<RIGHTTABLE, ?>) column);
			});
			mappedReverseColumns = new HashSet<>(foreignKeyColumnMapping.values());
			reverseColumnsValueProvider = srcid -> {
				Map<Column<RIGHTTABLE, ?>, Object> result = new HashMap<>();
				foreignKeyColumnMapping.forEach((accessor, column) -> {
					result.put(column, accessor.get(srcid));
				});
				return result;
			};
		} else if (relation.getReverseColumnName() != null || relation.getReverseColumn() != null) {
			Column<RIGHTTABLE, ?> reverseColumn;
			if (relation.getReverseColumnName() != null) {
				PrimaryKey<LEFTTABLE, SRCID> srcPrimaryKey = associationConfiguration.getSrcPersister().<LEFTTABLE>getMainTable().getPrimaryKey();
				// with a reverse column name, the primary key should be a single column key
				if (srcPrimaryKey.isComposed()) {
					throw new MappingConfigurationException("Giving reverse column whereas the primary key is composed :"
							+ " primary key = [" + srcPrimaryKey.getColumns().stream().map(Column::getName).collect(Collectors.joining(", ")) + "] vs "
							+ " reverse column = " + relation.getReverseColumnName());
				}
				Column<LEFTTABLE, ?> pk = Iterables.first(srcPrimaryKey.getColumns());
				reverseColumn = mainTargetTable.addColumn(
						relation.getReverseColumnName(),
						pk.getJavaType(),
						pk.getSize());
			} else {
				reverseColumn = (Column<RIGHTTABLE, ?>) relation.getReverseColumn();
			}
			foreignKeyBuilder.addColumn(reverseColumn);
			mappedReverseColumns = Arrays.asHashSet(reverseColumn);
			reverseColumnsValueProvider = srcid -> {
				Map<Column<RIGHTTABLE, ?>, Object> result = new HashMap<>();
				result.put(reverseColumn, srcid);
				return result;
			};
		} else if (relation.giveReverseSetter() != null) {
			// Please note that here Setter is only used to get foreign key column names, not to set values on reverse side
			Map<Column<LEFTTABLE, ?>, Column<RIGHTTABLE, ?>> foreignKeyColumnMapping = new HashMap<>();
			PrimaryKey<LEFTTABLE, SRCID> primaryKey = associationConfiguration.getSrcPersister().<LEFTTABLE>getMainTable().getPrimaryKey();
			AccessorDefinition reverseGetterDefinition = AccessorDefinition.giveDefinition(relation.giveReverseSetter());
			primaryKey.getColumns().forEach(pkColumn -> {
				String colName = associationConfiguration.getJoinColumnNamingStrategy().giveName(reverseGetterDefinition, pkColumn);
				Column<RIGHTTABLE, ?> fkColumn = mainTargetTable.addColumn(colName, pkColumn.getJavaType(), pkColumn.getSize(), null);
				foreignKeyBuilder.addColumn(fkColumn);
				foreignKeyColumnMapping.put(pkColumn, fkColumn);
			});
			mappedReverseColumns = new HashSet<>(foreignKeyColumnMapping.values());
			reverseColumnsValueProvider = srcid -> {
				IdentifierAssembler<SRCID, LEFTTABLE> identifierAssembler = associationConfiguration.getSrcPersister().getMapping().getIdMapping().<LEFTTABLE>getIdentifierAssembler();
				Map<Column<LEFTTABLE, ?>, ?> columnValues = identifierAssembler.getColumnValues(srcid);
				return Maps.innerJoin(foreignKeyColumnMapping, columnValues);
			};
		} // else : no reverse side mapped, this case can't happen since OneToManyWithMappedAssociationConfigurer is only
		// invoked when reverse side is mapped (see OneToManyRelation.isOwnedByReverseSide())
		foreignKey = foreignKeyBuilder.build();
		if (relation.isReverseAsMandatory() != null && relation.isReverseAsMandatory()) {
			foreignKey.getColumns().stream().map(Column.class::cast).forEach(Column::notNull);
		}
	}
	
	void assignAssociationEngine(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		// We're looking for the foreign key (for necessary join) and for getter/setter required to manage the relation 
		Mutator<TRGT, SRC> reversePropertyAccessor = null;
		if (associationConfiguration.getOneToManyRelation().giveReverseSetter() != null) {
			reversePropertyAccessor = associationConfiguration.getOneToManyRelation().giveReverseSetter();
		}
		BiConsumer<TRGT, SRC> reverseSetterAsConsumer = reversePropertyAccessor == null ? null : reversePropertyAccessor::set;
		if (associationConfiguration.getOneToManyRelation().isOrdered()) {
			assignEngineForIndexedAssociation(reverseSetterAsConsumer, foreignKey,
					associationConfiguration.getOneToManyRelation().getIndexingColumn(), targetPersister);
		} else {
			assignEngineForNonIndexedAssociation(foreignKey, targetPersister, reverseSetterAsConsumer);
		}
	}
	
	@Override
	public CascadeConfigurationResult<SRC, TRGT> configureWithSelectIn2Phases(String tableAlias,
																			  ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
																			  FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener) {
		determineForeignKeyColumns(targetPersister);
		assignAssociationEngine(targetPersister);
		mappedAssociationEngine.addSelectIn2Phases(associationConfiguration.getLeftPrimaryKey(),
				(Key<LEFTTABLE, SRCID>) mappedAssociationEngine.getManyRelationDescriptor().getReverseColumn(),
				associationConfiguration.getCollectionGetter(),
				firstPhaseCycleLoadListener);
		addWriteCascades(mappedAssociationEngine, targetPersister);
		return new CascadeConfigurationResult<>(mappedAssociationEngine.getManyRelationDescriptor().getRelationFixer(), associationConfiguration.getSrcPersister());
	}
	
	private void addWriteCascades(OneToManyWithMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C, RIGHTTABLE> mappedAssociationEngine,
								  ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		if (associationConfiguration.isWriteAuthorized()) {
			mappedAssociationEngine.addInsertCascade(targetPersister);
			mappedAssociationEngine.addUpdateCascade(associationConfiguration.isOrphanRemoval(), targetPersister);
			mappedAssociationEngine.addDeleteCascade(associationConfiguration.isOrphanRemoval(), targetPersister);
		}
	}
	
	private void assignEngineForNonIndexedAssociation(Key<?, SRCID> reverseColumn,
													  ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
													  @Nullable BiConsumer<TRGT, SRC> reverseSetter) {
		MappedManyRelationDescriptor<SRC, TRGT, C, SRCID> manyRelationDefinition = new MappedManyRelationDescriptor<>(
				associationConfiguration.getCollectionGetter(),
				associationConfiguration.getSetter()::set,
				associationConfiguration.getCollectionFactory(), reverseSetter, reverseColumn);
		mappedAssociationEngine = new OneToManyWithMappedAssociationEngine<>(
				targetPersister,
				manyRelationDefinition,
				associationConfiguration.getSrcPersister(),
				mappedReverseColumns,
				reverseColumnsValueProvider
		);
	}
	
	private void assignEngineForIndexedAssociation(@Nullable BiConsumer<TRGT, SRC> reverseSetter,
												   Key<?, SRCID> reverseColumn,
												   @Nullable Column<RIGHTTABLE, Integer> indexingColumn,
												   ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		if (indexingColumn == null) {
			String indexingColumnName = nullable(associationConfiguration.getIndexingColumnName()).getOr(() -> associationConfiguration.getIndexColumnNamingStrategy().giveName(accessorDefinitionForTableNaming));
			Class indexColumnType = this.associationConfiguration.isOrphanRemoval()
					? int.class
					: Integer.class;	// column must be nullable since row won't be deleted through orphan removal but only "detached" from parent row
			indexingColumn = targetPersister.getMapping().getTargetTable().addColumn(indexingColumnName, indexColumnType);
		}
		
		IndexedMappedManyRelationDescriptor<SRC, TRGT, C, SRCID, TRGTID> manyRelationDefinition = new IndexedMappedManyRelationDescriptor<>(
				associationConfiguration.getCollectionGetter(),
				associationConfiguration.getSetter()::set,
				associationConfiguration.getCollectionFactory(),
				reverseSetter,
				reverseColumn,
				indexingColumn,
				associationConfiguration.getSrcPersister()::getId,
				targetPersister::getId);
		mappedAssociationEngine = new OneToManyWithIndexedMappedAssociationEngine<>(
				targetPersister,
				manyRelationDefinition,
				associationConfiguration.getSrcPersister(),
				mappedReverseColumns,
				indexingColumn,
				reverseColumnsValueProvider
		);
	}
}
