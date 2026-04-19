package org.codefilarete.stalactite.engine.configurer.model;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class IntermediaryRelationJoin<
		LEFTTABLE extends Table<LEFTTABLE>,
		RIGHTTABLE extends Table<RIGHTTABLE>,
		ASSOCIATIONTABLE extends Table<ASSOCIATIONTABLE>,
		LEFTJOINTYPE,
		RIGHTJOINTYPE
		> extends RelationJoin<LEFTTABLE, ASSOCIATIONTABLE, LEFTJOINTYPE> {
	
	private final Key<ASSOCIATIONTABLE, LEFTJOINTYPE> leftAssociationKey;
	
	private final Key<ASSOCIATIONTABLE, RIGHTJOINTYPE> rightAssociationKey;
	
	private final Column<ASSOCIATIONTABLE, Integer> indexingColumn;
	
	public IntermediaryRelationJoin(Key<LEFTTABLE, LEFTJOINTYPE> leftKey,
									Key<ASSOCIATIONTABLE, LEFTJOINTYPE> leftAssociationKey,
									Key<ASSOCIATIONTABLE, RIGHTJOINTYPE> rightAssociationKey,
									Key<RIGHTTABLE, RIGHTJOINTYPE> rightKey,
									Column<ASSOCIATIONTABLE, Integer> indexingColumn) {
		super(leftKey, leftAssociationKey);
		this.leftAssociationKey = leftAssociationKey;
		this.rightAssociationKey = rightAssociationKey;
		this.indexingColumn = indexingColumn;
	}
	
	public Key<ASSOCIATIONTABLE, LEFTJOINTYPE> getLeftAssociationKey() {
		return leftAssociationKey;
	}
	
	public Key<ASSOCIATIONTABLE, RIGHTJOINTYPE> getRightAssociationKey() {
		return rightAssociationKey;
	}
	
	public Column<ASSOCIATIONTABLE, Integer> getIndexingColumn() {
		return indexingColumn;
	}
}
