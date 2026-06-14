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
	private final ForeignKey<MAPTABLE, KTABLE, KID> keyEntityForeignKey;
	@Nullable
	private final Entity<K, KID, KTABLE> keyEntity;
	@Nullable
	private final EntryMemberMapping<K, MAPTABLE> keyEntityIdentifierMapping;
	@Nullable
	private final RelationMode keyEntityRelationMode;
	@Nullable
	private final ForeignKey<MAPTABLE, VTABLE, VID> valueEntityForeignKey;
	@Nullable
	private final Entity<V, VID, VTABLE> valueEntity;
	@Nullable
	private final EntryMemberMapping<V, MAPTABLE> valueEntityIdentifierMapping;
	@Nullable
	private final RelationMode valueEntityRelationMode;
	
	public <X, Y> ResolvedMapRelation(ReadWritePropertyAccessPoint<SRC, M> accessor,
	                                  boolean fetchSeparately,
	                                  DirectRelationJoin<LEFTTABLE, MAPTABLE, SRCID> join,
	                                  BeanRelationFixer<SRC, KeyValueRecord<K, V, SRCID>> beanRelationFixer,
	                                  Supplier<M> componentFactory,
	                                  Map<Column<LEFTTABLE, ?>, Column<MAPTABLE, ?>> primaryKeyForeignKeyColumnMapping,
	                                  @Nullable ForeignKey<MAPTABLE, KTABLE, KID> keyEntityForeignKey,
	                                  @Nullable Entity<K, KID, KTABLE> keyEntity,
	                                  @Nullable EntryMemberMapping<X, MAPTABLE> keyEntityIdentifierMapping,
	                                  @Nullable RelationMode keyEntityRelationMode,
	                                  @Nullable ForeignKey<MAPTABLE, VTABLE, VID> valueEntityForeignKey,
	                                  @Nullable Entity<V, VID, VTABLE> valueEntity,
	                                  @Nullable EntryMemberMapping<Y, MAPTABLE> valueEntityIdentifierMapping,
	                                  @Nullable RelationMode valueEntityRelationMode) {
		// TODO: ALL shouldn't be used for RelationMode : we should extend another class or make the RelationMode not available by default in the super class
		super(accessor, RelationMode.ALL, fetchSeparately, join, beanRelationFixer, componentFactory);
		this.primaryKeyForeignKeyColumnMapping = primaryKeyForeignKeyColumnMapping;
		this.keyEntityForeignKey = keyEntityForeignKey;
		this.keyEntity = keyEntity;
		this.keyEntityIdentifierMapping = (EntryMemberMapping<K, MAPTABLE>) keyEntityIdentifierMapping;
		this.keyEntityRelationMode = keyEntityRelationMode;
		this.valueEntityIdentifierMapping = (EntryMemberMapping<V, MAPTABLE>) valueEntityIdentifierMapping;
		this.valueEntityRelationMode = valueEntityRelationMode;
		this.valueEntityForeignKey = valueEntityForeignKey;
		this.valueEntity = valueEntity;
	}
	
	@Override
	public DirectRelationJoin<LEFTTABLE, MAPTABLE, SRCID> getJoin() {
		return (DirectRelationJoin<LEFTTABLE, MAPTABLE, SRCID>) super.getJoin();
	}
	
	public Map<Column<LEFTTABLE, ?>, Column<MAPTABLE, ?>> getPrimaryKeyForeignKeyColumnMapping() {
		return primaryKeyForeignKeyColumnMapping;
	}
	
	@Nullable
	public ForeignKey<MAPTABLE, KTABLE, KID> getKeyEntityForeignKey() {
		return keyEntityForeignKey;
	}
	
	@Nullable
	public Entity<K, KID, KTABLE> getKeyEntity() {
		return keyEntity;
	}
	
	@Nullable
	public <X> EntryMemberMapping<X, MAPTABLE> getKeyEntityIdentifierMapping() {
		return (EntryMemberMapping<X, MAPTABLE>) keyEntityIdentifierMapping;
	}
	
	@Nullable
	public RelationMode getKeyEntityRelationMode() {
		return keyEntityRelationMode;
	}
	
	@Nullable
	public ForeignKey<MAPTABLE, VTABLE, VID> getValueEntityForeignKey() {
		return valueEntityForeignKey;
	}
	
	@Nullable
	public Entity<V, VID, VTABLE> getValueEntity() {
		return valueEntity;
	}
	
	@Nullable
	public <Y> EntryMemberMapping<Y, MAPTABLE> getValueEntityIdentifierMapping() {
		return (EntryMemberMapping<Y, MAPTABLE>) valueEntityIdentifierMapping;
	}
	
	@Nullable
	public RelationMode getValueEntityRelationMode() {
		return valueEntityRelationMode;
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
	
	public static class CompositeMemberMapping<XID, MAPTABLE extends Table<MAPTABLE>, XTABLE extends Table<XTABLE>>
			implements EntryMemberMapping<XID, MAPTABLE> {
		
		private final Class<XID> beanType;
		private final Map<ReadWritePropertyAccessPoint<XID, ?>, Column<XTABLE, ?>> mapping;
		
		public CompositeMemberMapping(Class<XID> beanType, Map<ReadWritePropertyAccessPoint<XID, ?>, Column<XTABLE, ?>> mapping) {
			this.beanType = beanType;
			this.mapping = mapping;
		}
		
		public Class<XID> getBeanType() {
			return beanType;
		}
		
		public Map<ReadWritePropertyAccessPoint<XID, ?>, Column<XTABLE, ?>> getMapping() {
			return mapping;
		}
	}
}

