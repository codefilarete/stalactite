package org.codefilarete.stalactite.engine.configurer.model;

import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecord;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

public class ResolvedElementCollectionRelation<SRC, TRGT, S extends Collection<TRGT>, SRCID, SRCTABLE extends Table<SRCTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>, ER extends ElementRecord<TRGT, SRCID>>
		extends ComponentRelation<SRC, ER, S, SRCTABLE, COLLECTIONTABLE, SRCID> {
	
	private final Map<ReadWritePropertyAccessPoint<ER, ?>, Column<COLLECTIONTABLE, ?>> columnMapping;
	
	private final Map<Column<SRCTABLE, ?>, Column<COLLECTIONTABLE, ?>> primaryKeyForeignKeyColumnMapping;
	
	private final Class<S> collectionType;
	
	@Nullable
	private final Column<COLLECTIONTABLE, Integer> indexColumn;
	
	public ResolvedElementCollectionRelation(ReadWritePropertyAccessPoint<SRC, S> accessor,
	                                         RelationMode relationMode,
	                                         boolean fetchSeparately,
	                                         DirectRelationJoin<SRCTABLE, COLLECTIONTABLE, SRCID> join,
	                                         BeanRelationFixer<SRC, ER> relationFixer,
	                                         Supplier<S> componentFactory,
	                                         Map<ReadWritePropertyAccessPoint<ER, ?>, Column<COLLECTIONTABLE, ?>> columnMapping,
	                                         Map<Column<SRCTABLE, ?>, Column<COLLECTIONTABLE, ?>> primaryKeyForeignKeyColumnMapping,
											 Class<S> collectionType,
	                                         @Nullable Column<COLLECTIONTABLE, Integer> indexColumn) {
		super(accessor, relationMode, fetchSeparately, join, relationFixer, componentFactory);
		this.columnMapping = columnMapping;
		this.primaryKeyForeignKeyColumnMapping = primaryKeyForeignKeyColumnMapping;
		this.collectionType = collectionType;
		this.indexColumn = indexColumn;
	}
	
	@Override
	public DirectRelationJoin<SRCTABLE, COLLECTIONTABLE, SRCID> getJoin() {
		return (DirectRelationJoin<SRCTABLE, COLLECTIONTABLE, SRCID>) super.getJoin();
	}
	
	public Map<ReadWritePropertyAccessPoint<ER, ?>, Column<COLLECTIONTABLE, ?>> getColumnMapping() {
		return columnMapping;
	}
	
	public Map<Column<SRCTABLE, ?>, Column<COLLECTIONTABLE, ?>> getPrimaryKeyForeignKeyColumnMapping() {
		return primaryKeyForeignKeyColumnMapping;
	}
	
	public Class<S> getCollectionType() {
		return collectionType;
	}
	
	public boolean isOrdered() {
		return indexColumn != null;
	}
	
	@Nullable
	public Column<COLLECTIONTABLE, Integer> getIndexColumn() {
		return indexColumn;
	}
}
