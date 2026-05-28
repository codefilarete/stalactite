package org.codefilarete.stalactite.engine.configurer.dslresolver;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.DefaultReadWritePropertyAccessPoint;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
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
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.bean.Objects.preventNull;
import static org.codefilarete.tool.collection.Iterables.first;

/**
 * Resolves Map DSL configuration to a {@link ResolvedMapRelation}.
 * Supports scalar maps plus entity-as-key and entity-as-value (single-column identifiers).
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

		EntitySource<Object, Object> keyEntitySource = (EntitySource<Object, Object>) resolveEntitySource(mapRelation.getKeyEntityConfigurationProvider());
		EntitySource<Object, Object> valueEntitySource = (EntitySource<Object, Object>) resolveEntitySource(mapRelation.getValueEntityConfigurationProvider());
		PrimaryKey<?, ?> keyPrimaryKey = keyEntitySource == null ? null : keyEntitySource.getEntity().getTable().getPrimaryKey();
		PrimaryKey<?, ?> valuePrimaryKey = valueEntitySource == null ? null : valueEntitySource.getEntity().getTable().getPrimaryKey();

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
		Map<ReadWritePropertyAccessPoint<KeyValueRecord<K, V, SRCID>, ?>, Column<MAPTABLE, ?>> columnMapping = buildMapTableMapping(
				mapRelation,
				targetTable,
				namingConfiguration.getColumnNamingStrategy(),
				namingConfiguration.getForeignKeyNamingStrategy(),
				keyPrimaryKey,
				valuePrimaryKey);

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
	private <X, XID> EntitySource<X, XID> resolveEntitySource(@javax.annotation.Nullable org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider<X, XID> mappingConfigurationProvider) {
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
		ForeignKey<MAPTABLE, SRCTABLE, SRCID> reverseForeignKey = targetTable.addForeignKey(foreignKeyNamingStrategy::giveName, reverseKey, sourcePK);
		return primaryKeyForeignColumnMapping;
	}

	private <SRC, SRCID, K, V, M extends Map<K, V>, MAPTABLE extends Table<MAPTABLE>>
	Map<ReadWritePropertyAccessPoint<KeyValueRecord<K, V, SRCID>, ?>, Column<MAPTABLE, ?>>
	buildMapTableMapping(MapRelation<SRC, K, V, M> mapRelation,
	                     MAPTABLE targetTable,
	                     ColumnNamingStrategy columnNamingStrategy,
	                     ForeignKeyNamingStrategy foreignKeyNamingStrategy,
	                     @javax.annotation.Nullable PrimaryKey<?, ?> keyPrimaryKey,
	                     @javax.annotation.Nullable PrimaryKey<?, ?> valuePrimaryKey) {
		Map<ReadWritePropertyAccessPoint<KeyValueRecord<K, V, SRCID>, ?>, Column<MAPTABLE, ?>> targetColumnMapping = new HashMap<>();
		if (keyPrimaryKey == null) {
			String keyColumnName = nullable(mapRelation.getKeyColumnName())
					.getOr(() -> columnNamingStrategy.giveName(ENTRY_KEY_ACCESSOR_DEFINITION));
			Column<MAPTABLE, K> keyColumn = targetTable.addColumn(keyColumnName, mapRelation.getKeyType(), mapRelation.getKeyColumnSize())
					.primaryKey();
			targetColumnMapping.put((ReadWritePropertyAccessPoint) KEY_ACCESSOR, keyColumn);
		} else {
			Column<MAPTABLE, ?> keyColumn = addEntityIdentifierColumn(targetTable,
					nullable(mapRelation.getKeyColumnName()).getOr(() -> columnNamingStrategy.giveName(ENTRY_KEY_ACCESSOR_DEFINITION)),
					keyPrimaryKey,
					mapRelation.getKeyColumnSize(),
					true,
					foreignKeyNamingStrategy);
			targetColumnMapping.put((ReadWritePropertyAccessPoint) KEY_ACCESSOR, keyColumn);
		}

		if (valuePrimaryKey == null) {
			String valueColumnName = nullable(mapRelation.getValueColumnName())
					.getOr(() -> columnNamingStrategy.giveName(ENTRY_VALUE_ACCESSOR_DEFINITION));
			Column<MAPTABLE, V> valueColumn = targetTable.addColumn(valueColumnName, mapRelation.getValueType(), mapRelation.getValueColumnSize());
			targetColumnMapping.put((ReadWritePropertyAccessPoint) VALUE_ACCESSOR, valueColumn);
		} else {
			Column<MAPTABLE, ?> valueColumn = addEntityIdentifierColumn(targetTable,
					nullable(mapRelation.getValueColumnName()).getOr(() -> columnNamingStrategy.giveName(ENTRY_VALUE_ACCESSOR_DEFINITION)),
					valuePrimaryKey,
					mapRelation.getValueColumnSize(),
					false,
					foreignKeyNamingStrategy);
			targetColumnMapping.put((ReadWritePropertyAccessPoint) VALUE_ACCESSOR, valueColumn);
		}
		return targetColumnMapping;
	}

	private <MAPTABLE extends Table<MAPTABLE>, OTHERTABLE extends Table<OTHERTABLE>, OTHERID>
	Column<MAPTABLE, OTHERID> addEntityIdentifierColumn(MAPTABLE targetTable,
	                                                   String columnName,
	                                                   PrimaryKey<OTHERTABLE, OTHERID> targetPrimaryKey,
	                                                   @javax.annotation.Nullable Size columnSize,
	                                                   boolean primaryKey,
	                                                   ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		Column<OTHERTABLE, OTHERID> targetPrimaryKeyColumn = (Column<OTHERTABLE, OTHERID>) first(targetPrimaryKey.getColumns());
		Column<MAPTABLE, OTHERID> relationColumn = targetTable.addColumn(columnName, targetPrimaryKeyColumn.getJavaType(), nullable(columnSize).getOr(targetPrimaryKeyColumn::getSize));
		if (primaryKey) {
			relationColumn.primaryKey();
		}
		targetTable.addForeignKey(foreignKeyNamingStrategy::giveName, Key.ofSingleColumn(relationColumn), targetPrimaryKey);
		return relationColumn;
	}
}


