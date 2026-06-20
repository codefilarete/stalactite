package org.codefilarete.stalactite.engine.configurer.model;

import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecord;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

public class ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M extends Map<K, V>,
		LEFTTABLE extends Table<LEFTTABLE>,
		MAPTABLE extends Table<MAPTABLE>,
		KTABLE extends Table<KTABLE>,
		VTABLE extends Table<VTABLE>>
		extends ComponentRelation<SRC, KeyValueRecord<K, V, SRCID>, M, LEFTTABLE, MAPTABLE, SRCID> {
	
	private final Map<Column<LEFTTABLE, ?>, Column<MAPTABLE, ?>> primaryKeyForeignKeyColumnMapping;
	@Nullable
	private final EntryMemberMapping<K, MAPTABLE> keyMapping;
	@Nullable
	private final EntryMemberMapping<V, MAPTABLE> valueMapping;
	@Nullable
	private final MapMemberAsEntity<K, KID, MAPTABLE, KTABLE, ?> keyEntityDefinition;
	@Nullable
	private final MapMemberAsEntity<V, VID, MAPTABLE, VTABLE, ?> valueEntityDefinition;
	
	public <X, Y> ResolvedMapRelation(ReadWritePropertyAccessPoint<SRC, M> accessor,
	                                  boolean fetchSeparately,
	                                  DirectRelationJoin<LEFTTABLE, MAPTABLE, SRCID> join,
	                                  BeanRelationFixer<SRC, KeyValueRecord<K, V, SRCID>> beanRelationFixer,
	                                  Supplier<M> componentFactory,
	                                  Map<Column<LEFTTABLE, ?>, Column<MAPTABLE, ?>> primaryKeyForeignKeyColumnMapping,
	                                  @Nullable EntryMemberMapping<X, MAPTABLE> keyMapping,
	                                  @Nullable EntryMemberMapping<Y, MAPTABLE> valueMapping,
	                                  @Nullable MapMemberAsEntity<K, KID, MAPTABLE, KTABLE, X> keyEntityDefinition,
	                                  @Nullable MapMemberAsEntity<V, VID, MAPTABLE, VTABLE, Y> valueEntityDefinition) {
		// TODO: ALL shouldn't be used for RelationMode : we should extend another class or make the RelationMode not available by default in the super class
		super(accessor, RelationMode.ALL, fetchSeparately, join, beanRelationFixer, componentFactory);
		this.primaryKeyForeignKeyColumnMapping = primaryKeyForeignKeyColumnMapping;
		this.keyMapping = (EntryMemberMapping<K, MAPTABLE>) keyMapping;
		this.valueMapping = (EntryMemberMapping<V, MAPTABLE>) valueMapping;
		this.keyEntityDefinition = keyEntityDefinition;
		this.valueEntityDefinition = valueEntityDefinition;
	}
	
	@Override
	public DirectRelationJoin<LEFTTABLE, MAPTABLE, SRCID> getJoin() {
		return (DirectRelationJoin<LEFTTABLE, MAPTABLE, SRCID>) super.getJoin();
	}
	
	public Map<Column<LEFTTABLE, ?>, Column<MAPTABLE, ?>> getPrimaryKeyForeignKeyColumnMapping() {
		return primaryKeyForeignKeyColumnMapping;
	}
	
	@Nullable
	public EntryMemberMapping<K, MAPTABLE> getKeyMapping() {
		return keyMapping;
	}
	
	@Nullable
	public EntryMemberMapping<V, MAPTABLE> getValueMapping() {
		return valueMapping;
	}
	
	@Nullable
	public MapMemberAsEntity<K, KID, MAPTABLE, KTABLE, ?> getKeyEntityDefinition() {
		return keyEntityDefinition;
	}
	
	@Nullable
	public MapMemberAsEntity<V, VID, MAPTABLE, VTABLE, ?> getValueEntityDefinition() {
		return valueEntityDefinition;
	}
	
	public interface EntryMemberMapping<X, MAPTABLE extends Table<MAPTABLE>> {
	}
	
	public static class ScalarMemberMapping<X, MAPTABLE extends Table<MAPTABLE>>
			implements EntryMemberMapping<X, MAPTABLE> {
		
		private final Column<MAPTABLE, X> column;
		
		public ScalarMemberMapping(Column<MAPTABLE, X> column) {
			this.column = column;
		}
		
		public Column<MAPTABLE, X> getColumn() {
			return column;
		}
	}
	
	public static class CompositeMemberMapping<XID, MAPTABLE extends Table<MAPTABLE>>
			implements EntryMemberMapping<XID, MAPTABLE> {
		
		private final Class<XID> beanType;
		private final Map<ReadWritePropertyAccessPoint<XID, ?>, Column<MAPTABLE, ?>> mapping;
		
		public CompositeMemberMapping(Class<XID> beanType, Map<ReadWritePropertyAccessPoint<XID, ?>, Column<MAPTABLE, ?>> mapping) {
			this.beanType = beanType;
			this.mapping = mapping;
		}
		
		public Class<XID> getBeanType() {
			return beanType;
		}
		
		public Map<ReadWritePropertyAccessPoint<XID, ?>, Column<MAPTABLE, ?>> getMapping() {
			return mapping;
		}
	}
	
	public static class MapMemberAsEntity<ENTITY, ENTITY_ID, MAPTABLE extends Table<MAPTABLE>, ENTITY_TABLE extends Table<ENTITY_TABLE>, X /* is either ENTITY or ENTITY_ID */> {
		
		private final ForeignKey<MAPTABLE, ENTITY_TABLE, ENTITY_ID> foreignKey;
		private final Entity<ENTITY, ENTITY_ID, ENTITY_TABLE> entity;
		private final RelationMode relationMode;
		
		public MapMemberAsEntity(Entity<ENTITY, ENTITY_ID, ENTITY_TABLE> entity,
		                         ForeignKey<MAPTABLE, ENTITY_TABLE, ENTITY_ID> foreignKey,
		                         RelationMode relationMode) {
			this.foreignKey = foreignKey;
			this.entity = entity;
			this.relationMode = relationMode;
		}
		
		public ForeignKey<MAPTABLE, ENTITY_TABLE, ENTITY_ID> getForeignKey() {
			return foreignKey;
		}
		
		public Entity<ENTITY, ENTITY_ID, ENTITY_TABLE> getEntity() {
			return entity;
		}
		
		public RelationMode getRelationMode() {
			return relationMode;
		}
	}
}

