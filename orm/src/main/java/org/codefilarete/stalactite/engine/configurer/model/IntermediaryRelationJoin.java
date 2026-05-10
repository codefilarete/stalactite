package org.codefilarete.stalactite.engine.configurer.model;

import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class IntermediaryRelationJoin<
		LEFTTABLE extends Table<LEFTTABLE>,
		RIGHTTABLE extends Table<RIGHTTABLE>,
		ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, LEFTJOINTYPE, RIGHTJOINTYPE>,
		LEFTJOINTYPE,
		RIGHTJOINTYPE
		> implements RelationJoin {
	
	
	private final KeyMapping<LEFTTABLE, ASSOCIATIONTABLE, LEFTJOINTYPE> leftKeyMapping;
	
	private final KeyMapping<ASSOCIATIONTABLE, RIGHTTABLE, RIGHTJOINTYPE> rightKeyMapping;
	
	/**
	 * Constructor that takes an {@link AssociationTable} as argument.
	 * A local type parameter {@code INTERMEDIARYTABLE} is introduced to satisfy both the {@code SELF}-referential
	 * bound of {@link AssociationTable} and the {@code ASSOCIATIONTABLE} type parameter of this class.
	 * The unchecked cast is safe because {@code INTERMEDIARYTABLE} is structurally the same table.
	 */
	public IntermediaryRelationJoin(ASSOCIATIONTABLE associationTable) {
		this(associationTable.getOneSideKey(),
				associationTable.getOneSideForeignKey(),
				associationTable.getManySideForeignKey(),
				associationTable.getManySideKey());
	}
	
	public IntermediaryRelationJoin(Key<LEFTTABLE, LEFTJOINTYPE> leftKey,
									Key<ASSOCIATIONTABLE, LEFTJOINTYPE> leftAssociationKey,
									Key<ASSOCIATIONTABLE, RIGHTJOINTYPE> rightAssociationKey,
									Key<RIGHTTABLE, RIGHTJOINTYPE> rightKey) {
		this.leftKeyMapping = new KeyMapping<>(leftKey, leftAssociationKey);
		this.rightKeyMapping = new KeyMapping<>(rightAssociationKey, rightKey);
	}
	
	public Key<LEFTTABLE, LEFTJOINTYPE> getLeftKey() {
		return leftKeyMapping.getLeftKey();
	}
	
	public Key<ASSOCIATIONTABLE, LEFTJOINTYPE> getLeftAssociationKey() {
		return leftKeyMapping.getRightKey();
	}
	
	public Key<ASSOCIATIONTABLE, RIGHTJOINTYPE> getRightAssociationKey() {
		return rightKeyMapping.getLeftKey();
	}
	
	public Key<RIGHTTABLE, RIGHTJOINTYPE> getRightKey() {
		return rightKeyMapping.getRightKey();
	}
	
	public ASSOCIATIONTABLE getJoinTable() {
		return leftKeyMapping.getRightKey().getTable();
	}
}
