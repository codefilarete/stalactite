package org.codefilarete.stalactite.engine.configurer.onetomany;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.ReversibleMutator;
import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelationConfigurer.FirstPhaseCycleLoadListener;
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
import org.codefilarete.tool.collection.Maps;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Configurer dedicated to association that are mapped on reverse side by a property and a column on table's target entities
 */
class OneToManyWithMappedAssociationConfigurer<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>,
		LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
		extends OneToManyConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, C, LEFTTABLE> {
	
	private OneToManyWithMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C, RIGHTTABLE> mappedAssociationEngine;
	
	
	private Key<RIGHTTABLE, SRCID> foreignKey;
	
	private Set<Column<RIGHTTABLE, Object>> mappedReverseColumns;
	
	private  Function<SRCID, Map<Column<RIGHTTABLE, Object>, Object>> reverseColumnsValueProvider;
	
	OneToManyWithMappedAssociationConfigurer(OneToManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C, LEFTTABLE> associationConfiguration,
											 ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
											 boolean loadSeparately) {
		super(associationConfiguration, targetPersister, loadSeparately);
	}
	
	@Override
	protected void configure() {
		determineForeignKeyColumns();
		assignAssociationEngine();
		mappedAssociationEngine.addSelectCascade(associationConfiguration.getLeftPrimaryKey(), loadSeparately);
		addWriteCascades(mappedAssociationEngine);
	}
	
	protected void determineForeignKeyColumns() {
		RIGHTTABLE mainTargetTable = (RIGHTTABLE) targetPersister.getMainTable();
		KeyBuilder<RIGHTTABLE, SRCID> foreignKeyBuilder = Key.from(mainTargetTable);
		OneToManyRelation<SRC, TRGT, TRGTID, C> relation = associationConfiguration.getOneToManyRelation();
		if (!relation.getForeignKeyNameMapping().isEmpty()) {
			Map<Accessor<SRCID, Object>, Column<RIGHTTABLE, Object>> foreignKeyColumnMapping = new HashMap<>();
			relation.getForeignKeyNameMapping().forEach((valueAccessPoint, colName) -> {
				AccessorDefinition localAccessorDefinition = AccessorDefinition.giveDefinition(valueAccessPoint);
				Accessor<SRCID, Object> accessor = valueAccessPoint instanceof Accessor
						? (Accessor) valueAccessPoint
						: (valueAccessPoint instanceof ReversibleMutator ? ((ReversibleMutator) valueAccessPoint).toAccessor() : null);
				if (accessor == null) {
					throw new UnsupportedOperationException("Can't get accessor from " + valueAccessPoint);
				}
				Column<RIGHTTABLE, Object> column = mainTargetTable.addColumn(colName, localAccessorDefinition.getMemberType());
				foreignKeyBuilder.addColumn(column);
				foreignKeyColumnMapping.put(accessor, column);
			});
			mappedReverseColumns = new HashSet<>(foreignKeyColumnMapping.values());
			reverseColumnsValueProvider = srcid -> {
				Map<Column<RIGHTTABLE, Object>, Object> result = new HashMap<>();
				foreignKeyColumnMapping.forEach((accessor, column) -> {
					result.put(column, accessor.get(srcid));
				});
				return result;
			};
		} else if (!relation.getForeignKeyColumnMapping().isEmpty()) {
			Map<Accessor<SRCID, Object>, Column<RIGHTTABLE, Object>> foreignKeyColumnMapping = new HashMap<>();
			relation.getForeignKeyColumnMapping().forEach((valueAccessPoint, column) -> {
				Accessor<SRCID, Object> accessor = valueAccessPoint instanceof Accessor
						? (Accessor) valueAccessPoint
						: (valueAccessPoint instanceof ReversibleMutator ? ((ReversibleMutator) valueAccessPoint).toAccessor() : null);
				if (accessor == null) {
					throw new UnsupportedOperationException("Can't get accessor from " + valueAccessPoint);
				}
				foreignKeyBuilder.addColumn((Column<RIGHTTABLE, Object>) column);
				foreignKeyColumnMapping.put(accessor, (Column<RIGHTTABLE, Object>) column);
			});
			mappedReverseColumns = new HashSet<>(foreignKeyColumnMapping.values());
			reverseColumnsValueProvider = srcid -> {
				Map<Column<RIGHTTABLE, Object>, Object> result = new HashMap<>();
				foreignKeyColumnMapping.forEach((accessor, column) -> {
					result.put(column, accessor.get(srcid));
				});
				return result;
			};
		} else if (relation.getReverseColumn() != null) {
			foreignKeyBuilder.addColumn((Column<RIGHTTABLE, Object>) relation.getReverseColumn());
			mappedReverseColumns = Arrays.asHashSet((Column<RIGHTTABLE, Object>) relation.getReverseColumn());
			reverseColumnsValueProvider = srcid -> {
				Map<Column<RIGHTTABLE, Object>, Object> result = new HashMap<>();
				result.put((Column<RIGHTTABLE, Object>) relation.getReverseColumn(), srcid);
				return result;
			};
		} else if (relation.getReverseGetter() != null) {
			// Please note that here Getter is only used to get foreign key column names, not to set values on reverse side
			Map<Column<LEFTTABLE, Object>, Column<RIGHTTABLE, Object>> foreignKeyColumnMapping = new HashMap<>();
			PrimaryKey<LEFTTABLE, SRCID> primaryKey = associationConfiguration.getSrcPersister().getMainTable().getPrimaryKey();
			AccessorDefinition reverseGetterDefinition = AccessorDefinition.giveDefinition(Accessors.accessor(relation.getReverseGetter()));
			primaryKey.getColumns().forEach(pkColumn -> {
				String colName = associationConfiguration.getJoinColumnNamingStrategy().giveName(reverseGetterDefinition, pkColumn);
				Column<RIGHTTABLE, Object> fkColumn = mainTargetTable.addColumn(colName, pkColumn.getJavaType(), pkColumn.getSize());
				foreignKeyBuilder.addColumn(fkColumn);
				foreignKeyColumnMapping.put(pkColumn, fkColumn);
			});
			mappedReverseColumns = new HashSet<>(foreignKeyColumnMapping.values());
			reverseColumnsValueProvider = srcid -> {
				IdentifierAssembler<SRCID, LEFTTABLE> identifierAssembler = associationConfiguration.getSrcPersister().getMapping().getIdMapping().<LEFTTABLE>getIdentifierAssembler();
				Map<Column<LEFTTABLE, Object>, Object> columnValues = identifierAssembler.getColumnValues(srcid);
				return Maps.innerJoin(foreignKeyColumnMapping, columnValues);
			};
		} else if (relation.getReverseSetter() != null) {
			// Please note that here Setter is only used to get foreign key column names, not to set values on reverse side
			Map<Column<LEFTTABLE, Object>, Column<RIGHTTABLE, Object>> foreignKeyColumnMapping = new HashMap<>();
			PrimaryKey<LEFTTABLE, SRCID> primaryKey = associationConfiguration.getSrcPersister().getMainTable().getPrimaryKey();
			AccessorDefinition reverseGetterDefinition = AccessorDefinition.giveDefinition(Accessors.mutator(relation.getReverseSetter()));
			primaryKey.getColumns().forEach(pkColumn -> {
				String colName = associationConfiguration.getJoinColumnNamingStrategy().giveName(reverseGetterDefinition, pkColumn);
				Column<RIGHTTABLE, Object> fkColumn = mainTargetTable.addColumn(colName, pkColumn.getJavaType(), pkColumn.getSize());
				foreignKeyBuilder.addColumn(fkColumn);
				foreignKeyColumnMapping.put(pkColumn, fkColumn);
			});
			mappedReverseColumns = new HashSet<>(foreignKeyColumnMapping.values());
			reverseColumnsValueProvider = srcid -> {
				IdentifierAssembler<SRCID, LEFTTABLE> identifierAssembler = associationConfiguration.getSrcPersister().getMapping().getIdMapping().<LEFTTABLE>getIdentifierAssembler();
				Map<Column<LEFTTABLE, Object>, Object> columnValues = identifierAssembler.getColumnValues(srcid);
				return Maps.innerJoin(foreignKeyColumnMapping, columnValues);
			};
		} // else : no reverse side mapped, this case can't happen since OneToManyWithMappedAssociationConfigurer is only
		// invoked when reverse side is mapped (see OneToManyRelation.isOwnedByReverseSide())
		
		
		// adding foreign key constraint
		// NB: we ask it to targetPersister because it may be polymorphic or complex (ie contains several tables) so it knows better how to do it
		foreignKey = foreignKeyBuilder.build();
		if (!(relation.isTargetTablePerClassPolymorphic())) {
			(mainTargetTable).addForeignKey(associationConfiguration.getForeignKeyNamingStrategy()::giveName,
					foreignKey, associationConfiguration.getLeftPrimaryKey());
		} else {
			// table-per-class case : we add a foreign key between each table of subentity and source primary key
			targetPersister.giveImpliedTables().forEach(table -> {
				KeyBuilder<Table, SRCID> projectedKeyBuilder = Key.from(table);
				((Set<Column<RIGHTTABLE, Object>>) foreignKey.getColumns()).forEach(column -> {
					projectedKeyBuilder.addColumn(table.addColumn(column.getName(), column.getJavaType(), column.getSize()));
				});
				// necessary cast due to projectedKey unknown exact Table type
				Key<Table, SRCID> projectedKey = projectedKeyBuilder.build();
				table.addForeignKey((BiFunction<Key<Table, SRCID>, PrimaryKey<LEFTTABLE, SRCID>, String>) associationConfiguration.getForeignKeyNamingStrategy()::giveName,
						projectedKey, associationConfiguration.getLeftPrimaryKey());
			});
		}
	}
	
	void assignAssociationEngine() {
		// We're looking for the foreign key (for necessary join) and for getter/setter required to manage the relation 
		Mutator<TRGT, SRC> reversePropertyAccessor = null;
		if (associationConfiguration.getOneToManyRelation().getReverseSetter() != null) {
			reversePropertyAccessor = Accessors.mutator(associationConfiguration.getOneToManyRelation().getReverseSetter());
		} else if (associationConfiguration.getOneToManyRelation().getReverseGetter() != null) {
			reversePropertyAccessor = Accessors.accessor(associationConfiguration.getOneToManyRelation().getReverseGetter()).toMutator();
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
																			  FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener) {
		determineForeignKeyColumns();
		assignAssociationEngine();
		mappedAssociationEngine.addSelectIn2Phases(associationConfiguration.getLeftPrimaryKey(),
				(Key<LEFTTABLE, SRCID>) mappedAssociationEngine.getManyRelationDescriptor().getReverseColumn(),
				associationConfiguration.getCollectionGetter(),
				firstPhaseCycleLoadListener);
		addWriteCascades(mappedAssociationEngine);
		return new CascadeConfigurationResult<>(mappedAssociationEngine.getManyRelationDescriptor().getRelationFixer(), associationConfiguration.getSrcPersister());
	}
	
	private void addWriteCascades(OneToManyWithMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C, RIGHTTABLE> mappedAssociationEngine) {
		if (associationConfiguration.isWriteAuthorized()) {
			mappedAssociationEngine.addInsertCascade();
			mappedAssociationEngine.addUpdateCascade(associationConfiguration.isOrphanRemoval());
			mappedAssociationEngine.addDeleteCascade(associationConfiguration.isOrphanRemoval());
		}
	}
	
	private void assignEngineForNonIndexedAssociation(Key<?, SRCID> reverseColumn,
													  ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
													  @Nullable BiConsumer<TRGT, SRC> reverseSetter) {
		MappedManyRelationDescriptor<SRC, TRGT, C, SRCID> manyRelationDefinition = new MappedManyRelationDescriptor<>(
				associationConfiguration.getCollectionGetter()::get, associationConfiguration.getSetter()::set,
				associationConfiguration.giveCollectionFactory(), reverseSetter, reverseColumn);
		mappedAssociationEngine = new OneToManyWithMappedAssociationEngine(
				targetPersister,
				manyRelationDefinition,
				associationConfiguration.getSrcPersister(),
				mappedReverseColumns,
				reverseColumnsValueProvider
		);
	}
	
	private void assignEngineForIndexedAssociation(@Nullable BiConsumer<TRGT, SRC> reverseSetter,
												   Key<?, SRCID> reverseColumn,
												   @Nullable Column<?, Integer> indexingColumn,
												   ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		if (indexingColumn == null) {
			String indexingColumnName = nullable(associationConfiguration.getColumnName()).getOr(() -> associationConfiguration.getIndexColumnNamingStrategy().giveName(accessorDefinition));
			Class indexColumnType = this.associationConfiguration.isOrphanRemoval()
					? int.class
					: Integer.class;	// column must be nullable since row won't be deleted through orphan removal but only "detached" from parent row
			indexingColumn = targetPersister.getMapping().getTargetTable().addColumn(indexingColumnName, indexColumnType);
		}
		
		IndexedMappedManyRelationDescriptor<SRC, TRGT, C, SRCID> manyRelationDefinition = new IndexedMappedManyRelationDescriptor<>(
				associationConfiguration.getCollectionGetter()::get,
				associationConfiguration.getSetter()::set,
				associationConfiguration.giveCollectionFactory(),
				reverseSetter,
				reverseColumn,
				indexingColumn,
				associationConfiguration.getSrcPersister()::getId);
		mappedAssociationEngine = new OneToManyWithIndexedMappedAssociationEngine(
				targetPersister,
				manyRelationDefinition,
				associationConfiguration.getSrcPersister(),
				mappedReverseColumns,
				reverseColumnsValueProvider
		);
	}
}
