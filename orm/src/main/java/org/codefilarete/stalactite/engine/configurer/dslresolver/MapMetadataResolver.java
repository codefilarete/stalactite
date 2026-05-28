package org.codefilarete.stalactite.engine.configurer.dslresolver;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.DefaultReadWritePropertyAccessPoint;
import org.codefilarete.reflection.ReadWriteAccessorChain;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.MapEntryTableNamingStrategy;
import org.codefilarete.stalactite.dsl.property.CascadeOptions;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.configurer.dslresolver.InheritanceConfigurationResolver.ResolvedConfiguration;
import org.codefilarete.stalactite.engine.configurer.dslresolver.MetadataSolvingCache.EntitySource;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecord;
import org.codefilarete.stalactite.engine.configurer.map.MapRelation;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.PairIterator;

import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.bean.Objects.preventNull;
import static org.codefilarete.tool.collection.Iterables.first;

/**
 * Resolves Map DSL configuration to a {@link ResolvedMapRelation}.
 * Supports scalar keys/values and entity keys/values (single or composite identifiers).
 */
public class MapMetadataResolver {

	private static final AccessorDefinition KEY_VALUE_RECORD_ID_ACCESSOR_DEFINITION = AccessorDefinition.giveDefinition(
			new AccessorByMethodReference<>(KeyValueRecord<Object, Object, Object>::getId));
	private static final AccessorDefinition ENTRY_KEY_ACCESSOR_DEFINITION = AccessorDefinition.giveDefinition(
			new AccessorByMethodReference<Map.Entry<Object, Object>, Object>(Map.Entry::getKey));
	private static final AccessorDefinition ENTRY_VALUE_ACCESSOR_DEFINITION = AccessorDefinition.giveDefinition(
			new AccessorByMethodReference<Map.Entry<Object, Object>, Object>(Map.Entry::getValue));

	private static final ReadWritePropertyAccessPoint<KeyValueRecord<Object, Object, Object>, Object> KEY_ACCESSOR =
			DefaultReadWritePropertyAccessPoint.fromMethodReference(KeyValueRecord::getKey, KeyValueRecord::setKey);
	private static final ReadWritePropertyAccessPoint<KeyValueRecord<Object, Object, Object>, Object> VALUE_ACCESSOR =
			DefaultReadWritePropertyAccessPoint.fromMethodReference(KeyValueRecord::getValue, KeyValueRecord::setValue);

	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;

	public MapMetadataResolver(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}

	<C, I> Set<EntitySource<?, ?>> resolve(EntitySource<C, I> source) {
		KeepOrderSet<EntitySource<?, ?>> targetEntities = new KeepOrderSet<>();
		source.getResolvedConfigurations().forEach(resolvedConfiguration ->
				targetEntities.addAll(resolve(source.getEntity(), resolvedConfiguration)));
		return targetEntities;
	}

	private <C, I> Set<EntitySource<?, ?>> resolve(Entity<C, I, ?> entity, ResolvedConfiguration<C, I> resolvedConfiguration) {
		KeepOrderSet<EntitySource<?, ?>> targetEntities = new KeepOrderSet<>();
		resolvedConfiguration.getMappingConfiguration().getMaps().forEach(mapRelation ->
				targetEntities.addAll(resolve(entity, resolvedConfiguration, mapRelation)));
		return targetEntities;
	}

	private <SRC, SRCID, K, V, M extends Map<K, V>, SRCTABLE extends Table<SRCTABLE>, MAPTABLE extends Table<MAPTABLE>>
	Set<EntitySource<?, ?>> resolve(Entity<SRC, SRCID, SRCTABLE> source,
	                                ResolvedConfiguration<SRC, SRCID> resolvedConfiguration,
	                                MapRelation<SRC, K, V, M> mapRelation) {

		if (mapRelation.getKeyEmbeddableConfigurationProvider() != null || mapRelation.getValueEmbeddableConfigurationProvider() != null) {
			throw new UnsupportedOperationException("ResolvedMapRelation currently supports scalar and entity key/value mappings (embeddable key/value is not supported yet)");
		}

		EntitySource<?, ?> keyEntitySource = resolveEntitySource(mapRelation.getKeyEntityConfigurationProvider());
		EntitySource<?, ?> valueEntitySource = resolveEntitySource(mapRelation.getValueEntityConfigurationProvider());

		AccessorDefinition mapAccessorDefinition = AccessorDefinition.giveDefinition(mapRelation.getMapProvider());
		PrimaryKey<SRCTABLE, SRCID> sourcePK = source.getTable().getPrimaryKey();
		NamingConfiguration namingConfiguration = resolvedConfiguration.getNamingConfiguration();

		MAPTABLE targetTable = determineTable(mapRelation, mapAccessorDefinition, namingConfiguration.getEntryMapTableNamingStrategy());
		Map<Column<SRCTABLE, ?>, Column<MAPTABLE, ?>> primaryKeyForeignKeyColumnMapping = buildPrimaryKeyForeignKeyColumnMapping(
				mapRelation,
				targetTable,
				sourcePK,
				namingConfiguration.getColumnNamingStrategy(),
				namingConfiguration.getForeignKeyNamingStrategy());

		SideColumnBinder keyBinder = buildSideColumnBinder(mapRelation.getKeyColumnName(), mapRelation.getKeyType(), mapRelation.getKeyColumnSize(),
				ENTRY_KEY_ACCESSOR_DEFINITION, keyEntitySource, true, KEY_ACCESSOR);
		SideColumnBinder valueBinder = buildSideColumnBinder(mapRelation.getValueColumnName(), mapRelation.getValueType(), mapRelation.getValueColumnSize(),
				ENTRY_VALUE_ACCESSOR_DEFINITION, valueEntitySource, false, VALUE_ACCESSOR);
		Map<ReadWritePropertyAccessPoint<KeyValueRecord<K, V, SRCID>, ?>, Column<MAPTABLE, ?>> columnMapping = buildMapTableMapping(
				targetTable,
				namingConfiguration.getColumnNamingStrategy(),
				namingConfiguration.getForeignKeyNamingStrategy(),
				keyBinder,
				valueBinder);

		Supplier<M> mapFactory = preventNull(
				mapRelation.getMapFactory(),
				Reflections.giveMapFactory((Class<M>) mapAccessorDefinition.getMemberType()));

		BeanRelationFixer<SRC, KeyValueRecord<K, V, SRCID>> relationFixer = BeanRelationFixer.ofMapAdapter(
				mapRelation.getMapProvider(),
				mapRelation.getMapProvider(),
				mapFactory,
				(bean, input, map) -> map.put(input.getKey(), input.getValue()));

		KeyBuilder<MAPTABLE, SRCID> targetKeyBuilder = Key.from(targetTable);
		targetKeyBuilder.addAllColumns(primaryKeyForeignKeyColumnMapping.values());
		DirectRelationJoin<SRCTABLE, MAPTABLE, SRCID> join = new DirectRelationJoin<>(sourcePK, targetKeyBuilder.build());

		ResolvedMapRelation<SRC, K, V, M, SRCID, SRCTABLE, MAPTABLE> relation = new ResolvedMapRelation<>(
				mapRelation.getMapProvider(),
				CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL,
				mapRelation.isFetchSeparately(),
				join,
				relationFixer,
				mapFactory,
				columnMapping,
				primaryKeyForeignKeyColumnMapping);

		source.addRelation(relation);

		KeepOrderSet<EntitySource<?, ?>> targetEntities = new KeepOrderSet<>();
		if (keyEntitySource != null) {
			targetEntities.add(keyEntitySource);
		}
		if (valueEntitySource != null) {
			targetEntities.add(valueEntitySource);
		}
		return targetEntities;
	}

	@javax.annotation.Nullable
	private <X, XID> EntitySource<X, XID> resolveEntitySource(@javax.annotation.Nullable EntityMappingConfigurationProvider<X, XID> mappingConfigurationProvider) {
		if (mappingConfigurationProvider == null) {
			return null;
		}
		EntityMappingConfiguration<X, XID> mappingConfiguration = mappingConfigurationProvider.getConfiguration();
		InheritanceConfigurationResolver<X, XID> inheritanceConfigurationResolver = new InheritanceConfigurationResolver<>();
		KeepOrderSet<ResolvedConfiguration<?, XID>> ancestorsConfigurations = inheritanceConfigurationResolver.resolveConfigurations(mappingConfiguration);
		InheritanceMetadataResolver<X, XID, ?> inheritanceMetadataResolver = new InheritanceMetadataResolver<>(dialect, connectionConfiguration);
		return inheritanceMetadataResolver.resolve(ancestorsConfigurations);
	}

	private <SRC, K, V, M extends Map<K, V>, MAPTABLE extends Table<MAPTABLE>>
	MAPTABLE determineTable(MapRelation<SRC, K, V, M> mapRelation,
	                        AccessorDefinition mapAccessorDefinition,
	                        MapEntryTableNamingStrategy mapEntryTableNamingStrategy) {
		MAPTABLE targetTable = (MAPTABLE) mapRelation.getTargetTable();
		if (targetTable == null) {
			String tableName = nullable(mapRelation.getTargetTableName()).getOr(
					() -> mapEntryTableNamingStrategy.giveTableName(mapAccessorDefinition, mapRelation.getKeyType(), mapRelation.getValueType()));
			targetTable = (MAPTABLE) new Table<>(tableName.replace('.', '_'));
		}
		return targetTable;
	}

	private <SRC, SRCID, K, V, M extends Map<K, V>, SRCTABLE extends Table<SRCTABLE>, MAPTABLE extends Table<MAPTABLE>>
	Map<Column<SRCTABLE, ?>, Column<MAPTABLE, ?>> buildPrimaryKeyForeignKeyColumnMapping(MapRelation<SRC, K, V, M> mapRelation,
	                                                                                      MAPTABLE targetTable,
	                                                                                      PrimaryKey<SRCTABLE, SRCID> sourcePK,
	                                                                                      ColumnNamingStrategy columnNamingStrategy,
	                                                                                      ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		Map<Column<SRCTABLE, ?>, Column<MAPTABLE, ?>> primaryKeyForeignColumnMapping = new HashMap<>();
		if (!sourcePK.isComposed() && mapRelation.getReverseColumn() != null) {
			primaryKeyForeignColumnMapping.put(first(sourcePK.getColumns()), (Column) mapRelation.getReverseColumn());
		} else {
			sourcePK.getColumns().forEach(col -> {
				String reverseColumnName = nullable(mapRelation.getReverseColumnName()).getOr(
						() -> columnNamingStrategy.giveName(KEY_VALUE_RECORD_ID_ACCESSOR_DEFINITION));
				Column<MAPTABLE, ?> reverseCol = targetTable.addColumn(reverseColumnName, col.getJavaType(), col.getSize())
						.primaryKey();
				primaryKeyForeignColumnMapping.put(col, reverseCol);
			});
		}
		KeyBuilder<MAPTABLE, SRCID> keyBuilder = Key.from(targetTable);
		keyBuilder.addAllColumns(primaryKeyForeignColumnMapping.values());
		Key<MAPTABLE, SRCID> reverseKey = keyBuilder.build();
		targetTable.addForeignKey(foreignKeyNamingStrategy::giveName, reverseKey, sourcePK);
		return primaryKeyForeignColumnMapping;
	}

	private SideColumnBinder buildSideColumnBinder(@javax.annotation.Nullable String explicitColumnName,
	                                               Class<?> scalarType,
	                                               @javax.annotation.Nullable Size explicitColumnSize,
	                                               AccessorDefinition accessorDefinition,
	                                               @javax.annotation.Nullable EntitySource<?, ?> entitySource,
	                                               boolean primaryKeyMember,
	                                               ReadWritePropertyAccessPoint<KeyValueRecord<Object, Object, Object>, Object> keyValueRecordAccessor) {
		if (entitySource == null) {
			return new ScalarColumnBinder(explicitColumnName, scalarType, explicitColumnSize, accessorDefinition, primaryKeyMember);
		} else {
			return new EntityIdentifierColumnBinder(accessorDefinition,
					entitySource.getEntity().getTable().getPrimaryKey(),
					(ReadWritePropertyAccessPoint) entitySource.getEntity().getIdAccessor(),
					entitySource.getEntity().getEntityType(),
					primaryKeyMember,
					keyValueRecordAccessor);
		}
	}

	private <MAPTABLE extends Table<MAPTABLE>, K, V, SRCID>
	Map<ReadWritePropertyAccessPoint<KeyValueRecord<K, V, SRCID>, ?>, Column<MAPTABLE, ?>>
	buildMapTableMapping(MAPTABLE targetTable,
	                     ColumnNamingStrategy columnNamingStrategy,
	                     ForeignKeyNamingStrategy foreignKeyNamingStrategy,
	                     SideColumnBinder keyBinder,
	                     SideColumnBinder valueBinder) {
		Map<ReadWritePropertyAccessPoint<KeyValueRecord<K, V, SRCID>, ?>, Column<MAPTABLE, ?>> targetColumnMapping = new HashMap<>();
		KeepOrderSet<Column<MAPTABLE, ?>> keyColumns = keyBinder.bind(targetTable, columnNamingStrategy, foreignKeyNamingStrategy);
		if (keyColumns.size() == 1) {
			targetColumnMapping.put((ReadWritePropertyAccessPoint) KEY_ACCESSOR, first(keyColumns));
		} else if (keyBinder instanceof EntityIdentifierColumnBinder) {
			((EntityIdentifierColumnBinder<?, ?>) keyBinder).putCompositeColumnsInMapping((Map) targetColumnMapping, keyColumns);
		}
		KeepOrderSet<Column<MAPTABLE, ?>> valueColumns = valueBinder.bind(targetTable, columnNamingStrategy, foreignKeyNamingStrategy);
		if (valueColumns.size() == 1) {
			targetColumnMapping.put((ReadWritePropertyAccessPoint) VALUE_ACCESSOR, first(valueColumns));
		} else if (valueBinder instanceof EntityIdentifierColumnBinder) {
			((EntityIdentifierColumnBinder<?, ?>) valueBinder).putCompositeColumnsInMapping((Map) targetColumnMapping, valueColumns);
		}
		return targetColumnMapping;
	}

	private interface SideColumnBinder {
		<MAPTABLE extends Table<MAPTABLE>> KeepOrderSet<Column<MAPTABLE, ?>> bind(MAPTABLE targetTable,
		                                                                          ColumnNamingStrategy columnNamingStrategy,
		                                                                          ForeignKeyNamingStrategy foreignKeyNamingStrategy);
	}

	private static class ScalarColumnBinder implements SideColumnBinder {
		private final String explicitColumnName;
		private final Class<?> scalarType;
		private final Size explicitColumnSize;
		private final AccessorDefinition accessorDefinition;
		private final boolean primaryKeyMember;

		private ScalarColumnBinder(@javax.annotation.Nullable String explicitColumnName,
		                          Class<?> scalarType,
		                          @javax.annotation.Nullable Size explicitColumnSize,
		                          AccessorDefinition accessorDefinition,
		                          boolean primaryKeyMember) {
			this.explicitColumnName = explicitColumnName;
			this.scalarType = scalarType;
			this.explicitColumnSize = explicitColumnSize;
			this.accessorDefinition = accessorDefinition;
			this.primaryKeyMember = primaryKeyMember;
		}

		@Override
		public <MAPTABLE extends Table<MAPTABLE>> KeepOrderSet<Column<MAPTABLE, ?>> bind(MAPTABLE targetTable,
		                                                                                  ColumnNamingStrategy columnNamingStrategy,
		                                                                                  ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
			String columnName = nullable(explicitColumnName).getOr(() -> columnNamingStrategy.giveName(accessorDefinition));
			Column<MAPTABLE, Object> column = targetTable.addColumn(columnName, (Class<Object>) scalarType, explicitColumnSize);
			if (primaryKeyMember) {
				column.primaryKey();
			}
			return new KeepOrderSet<>(column);
		}
	}

	private static class EntityIdentifierColumnBinder<OTHERTABLE extends Table<OTHERTABLE>, OTHERID> implements SideColumnBinder {
		private final AccessorDefinition accessorDefinition;
		private final PrimaryKey<OTHERTABLE, OTHERID> targetPrimaryKey;
		private final ReadWritePropertyAccessPoint<Object, OTHERID> idAccessor;
		private final Class<?> entityType;
		private final boolean primaryKeyMember;
		private final ReadWritePropertyAccessPoint<KeyValueRecord<Object, Object, Object>, Object> keyValueRecordAccessor;

		private EntityIdentifierColumnBinder(AccessorDefinition accessorDefinition,
		                                     PrimaryKey<OTHERTABLE, OTHERID> targetPrimaryKey,
		                                     ReadWritePropertyAccessPoint<Object, OTHERID> idAccessor,
		                                     Class<?> entityType,
		                                     boolean primaryKeyMember,
		                                     ReadWritePropertyAccessPoint<KeyValueRecord<Object, Object, Object>, Object> keyValueRecordAccessor) {
			this.accessorDefinition = accessorDefinition;
			this.targetPrimaryKey = targetPrimaryKey;
			this.idAccessor = idAccessor;
			this.entityType = entityType;
			this.primaryKeyMember = primaryKeyMember;
			this.keyValueRecordAccessor = keyValueRecordAccessor;
		}

		@Override
		public <MAPTABLE extends Table<MAPTABLE>> KeepOrderSet<Column<MAPTABLE, ?>> bind(MAPTABLE mapTable,
		                                                                                  ColumnNamingStrategy columnNamingStrategy,
		                                                                                  ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
			KeepOrderSet<Column<?, ?>> referencedColumns = (KeepOrderSet<Column<?, ?>>) (KeepOrderSet) targetPrimaryKey.getColumns();
			
			KeyBuilder<MAPTABLE, OTHERID> relationKeyBuilder = Key.from(mapTable);
			if (referencedColumns.size() == 1) {
				Column<?, Object> referencedColumn = (Column<?, Object>) first(referencedColumns);
				String columnName = columnNamingStrategy.giveName(accessorDefinition);
				Column<MAPTABLE, Object> relationColumn = mapTable.addColumn(
						columnName,
						referencedColumn.getJavaType(),
						referencedColumn.getSize());
				if (primaryKeyMember) {
					relationColumn.primaryKey();
				}
				relationKeyBuilder.addColumn(relationColumn);
			} else {
				for (Column<?, ?> referencedColumn : referencedColumns) {
					Column<MAPTABLE, Object> relationColumn = mapTable.addColumn(
							referencedColumn.getName(),
							(Class<Object>) referencedColumn.getJavaType(),
							referencedColumn.getSize());
					if (primaryKeyMember) {
						relationColumn.primaryKey();
					}
					relationKeyBuilder.addColumn(relationColumn);
				}
			}
			
			Key<MAPTABLE, OTHERID> key = relationKeyBuilder.build();
			mapTable.addForeignKey(foreignKeyNamingStrategy::giveName, key, targetPrimaryKey);
			return key.getColumns();
		}

		private <MAPTABLE extends Table<MAPTABLE>, SRC, K, V, SRCID>
		void putCompositeColumnsInMapping(Map<ReadWritePropertyAccessPoint<KeyValueRecord<K, V, SRCID>, ?>, Column<MAPTABLE, ?>> mapping,
		                                  KeepOrderSet<Column<MAPTABLE, ?>> relationColumns) {
			KeepOrderSet<Column<?, ?>> referencedColumns = (KeepOrderSet<Column<?, ?>>) (KeepOrderSet) targetPrimaryKey.getColumns();
			Class<?> idType = AccessorDefinition.giveDefinition(idAccessor).getMemberType();
			PairIterator<Column<MAPTABLE, ?>, Column<?, ?>> pairIterator = new PairIterator<>(relationColumns, referencedColumns);
			pairIterator.forEachRemaining(pair -> {
				ReadWritePropertyAccessPoint<Object, Object> idPropertyAccessor = Accessors.propertyAccessor((Class<Object>) idType, pair.getRight().getName());
				ReadWriteAccessorChain<KeyValueRecord<Object, Object, Object>, Object, Object> chain =
						new ReadWriteAccessorChain<>(java.util.Arrays.asList(keyValueRecordAccessor, idAccessor), idPropertyAccessor);
				chain.setNullValueHandler(new AccessorChain.ValueInitializerOnNullValue((accessor, inputType) -> accessor == keyValueRecordAccessor
						? Reflections.newInstance(entityType)
						: Reflections.newInstance(inputType)));
				mapping.put((ReadWritePropertyAccessPoint<KeyValueRecord<K, V, SRCID>, ?>) (ReadWritePropertyAccessPoint) chain, pair.getLeft());
			});
		}
	}
}
