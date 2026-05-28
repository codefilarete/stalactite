package org.codefilarete.stalactite.engine.configurer.model;

import java.util.Map;
import java.util.function.Supplier;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecord;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

public class ResolvedMapRelation<SRC, K, V, M extends Map<K, V>, SRCID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
		extends ComponentRelation<SRC, KeyValueRecord<K, V, SRCID>, M, LEFTTABLE, RIGHTTABLE, SRCID> {

	private final Map<ReadWritePropertyAccessPoint<KeyValueRecord<K, V, SRCID>, ?>, Column<RIGHTTABLE, ?>> columnMapping;

	private final Map<Column<LEFTTABLE, ?>, Column<RIGHTTABLE, ?>> primaryKeyForeignKeyColumnMapping;

	public ResolvedMapRelation(ReadWritePropertyAccessPoint<SRC, M> accessor,
	                          RelationMode relationMode,
	                          boolean fetchSeparately,
	                          DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID> join,
	                          BeanRelationFixer<SRC, KeyValueRecord<K, V, SRCID>> beanRelationFixer,
	                          Supplier<M> componentFactory,
	                          Map<ReadWritePropertyAccessPoint<KeyValueRecord<K, V, SRCID>, ?>, Column<RIGHTTABLE, ?>> columnMapping,
	                          Map<Column<LEFTTABLE, ?>, Column<RIGHTTABLE, ?>> primaryKeyForeignKeyColumnMapping) {
		super(accessor, relationMode, fetchSeparately, join, beanRelationFixer, componentFactory);
		this.columnMapping = columnMapping;
		this.primaryKeyForeignKeyColumnMapping = primaryKeyForeignKeyColumnMapping;
	}

	@Override
	public DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID> getJoin() {
		return (DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID>) super.getJoin();
	}

	public Map<ReadWritePropertyAccessPoint<KeyValueRecord<K, V, SRCID>, ?>, Column<RIGHTTABLE, ?>> getColumnMapping() {
		return columnMapping;
	}

	public Map<Column<LEFTTABLE, ?>, Column<RIGHTTABLE, ?>> getPrimaryKeyForeignKeyColumnMapping() {
		return primaryKeyForeignKeyColumnMapping;
	}
}

