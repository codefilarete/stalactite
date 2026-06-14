package org.codefilarete.stalactite.engine.configurer.dslresolver;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.MapTableNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMapping;
import org.codefilarete.stalactite.engine.configurer.dslresolver.InheritanceConfigurationResolver.ResolvedConfiguration;
import org.codefilarete.stalactite.engine.configurer.dslresolver.MetadataSolvingCache.EntitySource;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecord;
import org.codefilarete.stalactite.engine.configurer.map.MapRelation;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.IdentifierMapping;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation.CompositeMemberMapping;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation.EntryMemberMapping;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation.ScalarMemberMapping;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeySupport;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.Maps;

import static org.codefilarete.reflection.DefaultReadWritePropertyAccessPoint.fromMethodReference;
import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.bean.Objects.preventNull;
import static org.codefilarete.tool.collection.Iterables.first;

/**
 * Resolves Map DSL configuration to a {@link ResolvedMapRelation}.
 * Supports scalar keys/values and entity keys/values (single or composite identifiers).
 */
public class MapMetadataResolver {
	
	private static final AccessorDefinition RECORD_ID_ACCESSOR_DEFINITION = AccessorDefinition.giveDefinition(
			new AccessorByMethodReference<>(KeyValueRecord<Object, Object, Object>::getId));
	private static final AccessorDefinition MAP_ENTRY_KEY_ACCESSOR_DEFINITION = AccessorDefinition.giveDefinition(
			new AccessorByMethodReference<Map.Entry<Object, Object>, Object>(Map.Entry::getKey));
	private static final AccessorDefinition MAP_ENTRY_VALUE_ACCESSOR_DEFINITION = AccessorDefinition.giveDefinition(
			new AccessorByMethodReference<Map.Entry<Object, Object>, Object>(Map.Entry::getValue));
	
	private static final ReadWritePropertyAccessPoint<KeyValueRecord<Object, Object, Object>, Object> RECORD_KEY_ACCESSOR =
			fromMethodReference(KeyValueRecord::getKey, KeyValueRecord::setKey);
	private static final ReadWritePropertyAccessPoint<KeyValueRecord<Object, Object, Object>, Object> RECORD_VALUE_ACCESSOR =
			fromMethodReference(KeyValueRecord::getValue, KeyValueRecord::setValue);
	
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
	
	// X is either K or KID
	// Y is either V or VID
	private <X, Y, SRC, SRCID, K, KID, V, VID,
			M extends Map<K, V>,
			SRCTABLE extends Table<SRCTABLE>,
			MAPTABLE extends Table<MAPTABLE>,
			KTABLE extends Table<KTABLE>,
			VTABLE extends Table<VTABLE>>
	Set<EntitySource<?, ?>> resolve(Entity<SRC, SRCID, SRCTABLE> source,
	                                ResolvedConfiguration<SRC, SRCID> resolvedConfiguration,
	                                MapRelation<SRC, K, V, M> mapRelation) {
		
		AccessorDefinition mapAccessorDefinition = AccessorDefinition.giveDefinition(mapRelation.getMapProvider());
		PrimaryKey<SRCTABLE, SRCID> sourcePK = source.getTable().getPrimaryKey();
		NamingConfiguration namingConfiguration = resolvedConfiguration.getNamingConfiguration();
		
		MAPTABLE targetTable = determineMapTable(mapRelation, mapAccessorDefinition, namingConfiguration.getMapTableNamingStrategy());
		Map<Column<SRCTABLE, ?>, Column<MAPTABLE, ?>> primaryKeyForeignKeyColumnMapping = buildPrimaryKeyForeignKeyColumnMapping(
				mapRelation,
				targetTable,
				sourcePK,
				namingConfiguration.getColumnNamingStrategy(),
				namingConfiguration.getForeignKeyNamingStrategy());
		
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
		DirectRelationJoin<SRCTABLE, MAPTABLE, SRCID> mapJoin = new DirectRelationJoin<>(sourcePK, targetKeyBuilder.build());
		
		KeepOrderSet<EntitySource<?, ?>> targetEntities = new KeepOrderSet<>();
		ForeignKey<MAPTABLE, KTABLE, KID> keyEntityReferenceMapping = null;
		Entity<K, KID, KTABLE> keyEntity = null;
		EntryMemberMapping<X, MAPTABLE> keyEntityIdentifierMapping;
		if (mapRelation.getKeyEntityConfigurationProvider() != null) {
			EntitySource<K, KID> keyEntitySource = buildEntity((EntityMappingConfigurationProvider<K, KID>) mapRelation.getKeyEntityConfigurationProvider());
			keyEntity = keyEntitySource.getEntity();
			targetEntities.add(keyEntitySource);
			keyEntityReferenceMapping = buildForeignEntityColumnMapping(
					keyEntitySource,
					RECORD_KEY_ACCESSOR,
					mapRelation.getKeyColumnName(),
					mapRelation.getKeyColumnSize(),
					targetTable,
					namingConfiguration.getForeignKeyNamingStrategy(),
					namingConfiguration.getJoinColumnNamingStrategy());
			
			// building identifier mapping projected on Map Table 
			keyEntityIdentifierMapping = (EntryMemberMapping<X, MAPTABLE>) buildEntityMemberMapping(keyEntity.getIdentifierMapping(), keyEntityReferenceMapping);
			// entry key columns must be part of the Map Table primary key
			keyEntityReferenceMapping.getColumns().forEach(Column::primaryKey);
		} else if (mapRelation.getKeyEmbeddableConfigurationProvider() != null) {
			CompositeMemberMapping<K, MAPTABLE> compositeKeyMapping = buildCompositeMemberMapping(
					targetTable,
					namingConfiguration,
					mapRelation.getKeyEmbeddableConfigurationProvider().getConfiguration(),
					mapRelation.getOverriddenKeyColumnNames(),
					mapRelation.getOverriddenKeyColumnSizes()
			);
			compositeKeyMapping.getMapping().values().forEach(Column::primaryKey);
			keyEntityIdentifierMapping = (EntryMemberMapping<X, MAPTABLE>) compositeKeyMapping;
		} else {
			ScalarMemberMapping<K, MAPTABLE> scalarKeyMapping = buildScalarMemberMapping(targetTable,
					namingConfiguration,
					MAP_ENTRY_KEY_ACCESSOR_DEFINITION,
					mapRelation.getKeyColumnName(),
					mapRelation.getKeyType(),
					mapRelation.getKeyColumnSize());
			scalarKeyMapping.getColumn().primaryKey();
			keyEntityIdentifierMapping = (EntryMemberMapping<X, MAPTABLE>) scalarKeyMapping;
		}
		
		ForeignKey<MAPTABLE, VTABLE, VID> valueEntityReferenceMapping = null;
		Entity<V, VID, VTABLE> valueEntity = null;
		EntryMemberMapping<Y, MAPTABLE> valueEntityIdentifierMapping;
		if (mapRelation.getValueEntityConfigurationProvider() != null) {
			EntitySource<V, VID> valueEntitySource = buildEntity((EntityMappingConfigurationProvider<V, VID>) mapRelation.getValueEntityConfigurationProvider());
			valueEntity = valueEntitySource.getEntity();
			targetEntities.add(valueEntitySource);
			valueEntityReferenceMapping = buildForeignEntityColumnMapping(
					valueEntitySource,
					RECORD_VALUE_ACCESSOR,
					mapRelation.getValueColumnName(),
					mapRelation.getValueColumnSize(),
					targetTable,
					namingConfiguration.getForeignKeyNamingStrategy(),
					namingConfiguration.getJoinColumnNamingStrategy());
			// building identifier mapping projected on Map Table 
			valueEntityIdentifierMapping = (EntryMemberMapping<Y, MAPTABLE>) buildEntityMemberMapping(valueEntity.getIdentifierMapping(), valueEntityReferenceMapping);
		} else if (mapRelation.getValueEmbeddableConfigurationProvider() != null) {
			CompositeMemberMapping<V, MAPTABLE> compositeValueMapping = buildCompositeMemberMapping(
					targetTable,
					namingConfiguration,
					mapRelation.getValueEmbeddableConfigurationProvider().getConfiguration(),
					mapRelation.getOverriddenValueColumnNames(),
					mapRelation.getOverriddenValueColumnSizes()
			);
			valueEntityIdentifierMapping = (EntryMemberMapping<Y, MAPTABLE>) compositeValueMapping;
		} else {
			valueEntityIdentifierMapping = (EntryMemberMapping<Y, MAPTABLE>) buildScalarMemberMapping(targetTable,
					namingConfiguration,
					MAP_ENTRY_VALUE_ACCESSOR_DEFINITION,
					mapRelation.getValueColumnName(),
					mapRelation.getValueType(),
					mapRelation.getValueColumnSize());
		}
		
		ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, SRCTABLE, MAPTABLE, KTABLE, VTABLE> relation = new ResolvedMapRelation<>(
				mapRelation.getMapProvider(),
				mapRelation.isFetchSeparately(),
				mapJoin,
				relationFixer,
				mapFactory,
				primaryKeyForeignKeyColumnMapping,
				keyEntityReferenceMapping,
				keyEntity,
				keyEntityIdentifierMapping,
				mapRelation.getKeyEntityRelationMode(),
				valueEntityReferenceMapping,
				valueEntity,
				valueEntityIdentifierMapping,
				mapRelation.getValueEntityRelationMode()
		);
		source.addRelation(relation);
		
		return targetEntities;
	}
	
	private <X, XID, XTABLE extends Table<XTABLE>, MAPTABLE extends Table<MAPTABLE>>
	ForeignKey<MAPTABLE, XTABLE, XID> buildForeignEntityColumnMapping(EntitySource<X, XID> entitySource,
	                                                                  ReadWritePropertyAccessPoint<? extends KeyValueRecord<?, ?, ?>, ?> recordMemberAccessor,
	                                                                  @Nullable String columnName,
	                                                                  @Nullable Size columnSize,
	                                                                  MAPTABLE mapTable,
	                                                                  ForeignKeyNamingStrategy foreignKeyNamingStrategy,
																	  JoinColumnNamingStrategy joinColumnNamingStrategy) {
		PrimaryKey<XTABLE, XID> entityPrimaryKey = entitySource.<XTABLE>getEntity().getTable().getPrimaryKey();
		KeepOrderMap<JoinLink<MAPTABLE, ?>, JoinLink<XTABLE, ?>> mapping = new KeepOrderMap<>();
		if (!entityPrimaryKey.isComposed()) {
			Column<XTABLE, XID> entityKeyColumn = (Column<XTABLE, XID>) first(entityPrimaryKey.getColumns());
			String effectiveColumnName = nullable(columnName).getOr(() -> joinColumnNamingStrategy.giveName(recordMemberAccessor, entityKeyColumn));
			Column<MAPTABLE, XID> maptableKeyColumn = mapTable.addColumn(
					effectiveColumnName,
					entityKeyColumn.getJavaType(),
					nullable(columnSize).getOr(entityKeyColumn::getSize));
			mapping.put(maptableKeyColumn, entityKeyColumn);
		} else {
			entityPrimaryKey.getColumns().forEach(entityKeyColumn -> {
				Column<MAPTABLE, ?> maptableKeyColumn = mapTable.addColumn(entityKeyColumn.getName(), entityKeyColumn.getJavaType(), entityKeyColumn.getSize());
				mapping.put(maptableKeyColumn, entityKeyColumn);
			});
		}
		return mapTable.addForeignKey(foreignKeyNamingStrategy::giveName, new KeySupport<>(new KeepOrderSet<>(mapping.keySet())), new KeySupport<>(new KeepOrderSet<>(mapping.values())));
	}
	
	// X is K or V
	private <X, XID, XTABLE extends Table<XTABLE>, MAPTABLE extends Table<MAPTABLE>>
	EntryMemberMapping<XID, MAPTABLE> buildEntityMemberMapping(IdentifierMapping<X, XID> identifierMapping,
	                                                           ForeignKey<MAPTABLE, XTABLE, XID> foreignKeyEntityReferenceMapping) {
		if (identifierMapping instanceof CompositeIdentifierMapping) {
			CompositeIdentifierMapping<X, XID, XTABLE> compositeIdentifierMapping = (CompositeIdentifierMapping<X, XID, XTABLE>) identifierMapping;
			
			KeyMapping<XTABLE, MAPTABLE, ?> invertedMapping = new KeyMapping<>(foreignKeyEntityReferenceMapping.getReferencedKey(), foreignKeyEntityReferenceMapping);
			Map<ReadWritePropertyAccessPoint<XID, ?>, Column<MAPTABLE, ?>> identifierMappingInMapTable = Maps.innerJoinOnValuesAndKeys(compositeIdentifierMapping.getCompositeKeyMapping().getMapping(), invertedMapping.getMapping());
			
			return new CompositeMemberMapping<>(compositeIdentifierMapping.getCompositeKeyMapping().getBeanType(), identifierMappingInMapTable);
		} else if (identifierMapping instanceof SingleIdentifierMapping) {
			return new ScalarMemberMapping<>((Column<MAPTABLE, XID>) first(foreignKeyEntityReferenceMapping.getColumns()));
		} else if (identifierMapping instanceof AssignedByAnotherIdentifierMapping) {
			return buildEntityMemberMapping(((AssignedByAnotherIdentifierMapping<X, XID>) identifierMapping).getSource(), foreignKeyEntityReferenceMapping);
		} else {
			throw new IllegalArgumentException("Unknown identifier mapping type " + Reflections.toString(identifierMapping.getClass()));
		}
	}
	
	private <X, XID> EntitySource<X, XID> buildEntity(EntityMappingConfigurationProvider<X, XID> mappingConfigurationProvider) {
		EntityMappingConfiguration<X, XID> mappingConfiguration = mappingConfigurationProvider.getConfiguration();
		InheritanceConfigurationResolver<X, XID> inheritanceConfigurationResolver = new InheritanceConfigurationResolver<>();
		KeepOrderSet<ResolvedConfiguration<?, XID>> ancestorsConfigurations = inheritanceConfigurationResolver.resolveConfigurations(mappingConfiguration);
		InheritanceMetadataResolver<X, XID, ?> inheritanceMetadataResolver = new InheritanceMetadataResolver<>(dialect, connectionConfiguration);
		return inheritanceMetadataResolver.resolve(ancestorsConfigurations);
	}
	
	private <SRC, K, V, M extends Map<K, V>, MAPTABLE extends Table<MAPTABLE>>
	MAPTABLE determineMapTable(MapRelation<SRC, K, V, M> mapRelation,
	                           AccessorDefinition mapAccessorDefinition,
	                           MapTableNamingStrategy mapTableNamingStrategy) {
		MAPTABLE targetTable = (MAPTABLE) mapRelation.getTargetTable();
		if (targetTable == null) {
			String tableName = nullable(mapRelation.getTargetTableName()).getOr(
					() -> mapTableNamingStrategy.giveTableName(mapAccessorDefinition, mapRelation.getKeyType(), mapRelation.getValueType()));
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
						() -> columnNamingStrategy.giveName(RECORD_ID_ACCESSOR_DEFINITION));
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
	
	private <X, MAPTABLE extends Table<MAPTABLE>>
	CompositeMemberMapping<X, MAPTABLE>
	buildCompositeMemberMapping(MAPTABLE mapTable,
	                            NamingConfiguration namingStrategy,
	                            EmbeddableMappingConfiguration<X> embeddableMappingConfiguration,
								ValueAccessPointMap<X, String, ValueAccessPoint<X>> overriddenKeyColumnNames,
								ValueAccessPointMap<X, Size, ValueAccessPoint<X>> overriddenKeyColumnSizes) {
		// a special configuration was given, we compute a EmbeddedClassMapping from it
		PropertyMappingResolver<X, MAPTABLE> propertyMappingResolver = new PropertyMappingResolver<>(dialect.getColumnBinderRegistry());
		EmbeddableMapping<X, MAPTABLE> entryKeyMapping = propertyMappingResolver.resolveAsMapping(embeddableMappingConfiguration, overriddenKeyColumnNames, overriddenKeyColumnSizes, new ValueAccessPointMap<>(), mapTable, namingStrategy.getColumnNamingStrategy());
		return new CompositeMemberMapping<>(embeddableMappingConfiguration.getBeanType(), entryKeyMapping.getMapping());
	}
	
	private <X, MAPTABLE extends Table<MAPTABLE>>
	ScalarMemberMapping<X, MAPTABLE>
	buildScalarMemberMapping(MAPTABLE mapTable,
	                         NamingConfiguration namingStrategy,
	                         AccessorDefinition entryMemberAccessorDefinition,
	                         @Nullable String columnName,
	                         Class<X> scalarType,
	                         @Nullable Size columnSize) {
		String effectiveColumnName = nullable(columnName)
				.getOr(() -> namingStrategy.getColumnNamingStrategy().giveName(entryMemberAccessorDefinition));
		Column<MAPTABLE, X> maptableColumn = mapTable.addColumn(effectiveColumnName, scalarType, columnSize);
		return new ScalarMemberMapping<>(maptableColumn);
	}
}
