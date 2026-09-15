package org.codefilarete.stalactite.engine.configurer.resolver.polymorphism;

import java.util.Collection;

import org.codefilarete.reflection.PropertyAccessPoint;
import org.codefilarete.stalactite.engine.configurer.model.AbstractEntity;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

public class GraftableRelation<SRC, TRGT, TRGTID, SRCTABLE extends Table<SRCTABLE>, TRGTTABLE extends Table<TRGTTABLE>, JOINID> {
	
	public static
	<SRC, TRGT, TRGTID, SRCTABLE extends Table<SRCTABLE>, TRGTTABLE extends Table<TRGTTABLE>, JOINID>
	GraftableRelation<SRC, TRGT, TRGTID, SRCTABLE, TRGTTABLE, JOINID> forOneToOneRelationOwnedByTarget(ResolvedOneToOneRelation<SRC, TRGT, SRCTABLE, TRGTTABLE, JOINID> relation) {
		return new GraftableRelation<>(relation.getJoin().getLeftKey(), relation.getJoin().getRightKey(), relation.getAccessor(), (BeanRelationFixer<Object, TRGT>) relation.getRelationFixer(), relation.getTargetEntity());
	}
	
	public static
	<SRC, TRGT, S extends Collection<TRGT>, TRGTID, SRCTABLE extends Table<SRCTABLE>, TRGTTABLE extends Table<TRGTTABLE>, JOINID>
	GraftableRelation<SRC, TRGT, TRGTID, SRCTABLE, TRGTTABLE, JOINID> forOneToManyRelationOwnedByTarget(ResolvedOneToManyRelation<SRC, TRGT, S, ?, TRGTID, SRCTABLE, TRGTTABLE> relation) {
		DirectRelationJoin<SRCTABLE, TRGTTABLE, JOINID> directRelationJoin = (DirectRelationJoin<SRCTABLE, TRGTTABLE, JOINID>) relation.getJoin();
		return new GraftableRelation<>(
				directRelationJoin.getLeftKey(),
				directRelationJoin.getRightKey(),
				relation.getAccessor(),
				(BeanRelationFixer<Object, TRGT>) relation.getRelationFixer(),
				relation.getTargetEntity());
	}
	
	public static
	<SRC, TRGT, S extends Collection<TRGT>, SRCID, TRGTID, SRCTABLE extends Table<SRCTABLE>, TRGTTABLE extends Table<TRGTTABLE>, ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, SRCTABLE, TRGTTABLE, SRCID, TRGTID>>
	GraftableRelation<SRC, TRGT, TRGTID, ASSOCIATIONTABLE, TRGTTABLE, TRGTID> forOneToManyRelationWithAssociationTable(ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, SRCTABLE, TRGTTABLE> relation) {
		IntermediaryRelationJoin<SRCTABLE, TRGTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> intermediaryRelationJoin = (IntermediaryRelationJoin<SRCTABLE, TRGTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID>) relation.getJoin();
		return new GraftableRelation<>(
				intermediaryRelationJoin.getRightAssociationKey(),
				intermediaryRelationJoin.getRightKey(),
				relation.getAccessor(),
				(BeanRelationFixer<Object, TRGT>) relation.getRelationFixer(),
				relation.getTargetEntity());
	}
	
	private final Key<SRCTABLE, JOINID> joinLeftKey;
	
	private final Key<TRGTTABLE, JOINID> joinRightKey;
	
	private final PropertyAccessPoint<?, ?> accessor;
	
	private final BeanRelationFixer<Object, TRGT> relationFixer;
	
	private final AbstractEntity<TRGT, TRGTID,TRGTTABLE> targetEntity;
	
	public GraftableRelation(Key<SRCTABLE, JOINID> leftKey,
							 Key<TRGTTABLE, JOINID> rightKey,
							 PropertyAccessPoint<?, ?> accessor,
							 BeanRelationFixer<Object, TRGT> relationFixer,
							 AbstractEntity<TRGT, TRGTID, TRGTTABLE> targetEntity) {
		this.joinLeftKey = leftKey;
		this.joinRightKey = rightKey;
		this.accessor = accessor;
		this.relationFixer = relationFixer;
		this.targetEntity = targetEntity;
	}
	
	public Key<SRCTABLE, JOINID> getLeftKey() {
		return joinLeftKey;
	}
	
	public Key<TRGTTABLE, JOINID> getRightKey() {
		return joinRightKey;
	}
	
	public PropertyAccessPoint<?, ?> getAccessor() {
		return accessor;
	}
	
	public BeanRelationFixer<Object, TRGT> getRelationFixer() {
		return relationFixer;
	}
	
	public AbstractEntity<TRGT, TRGTID, TRGTTABLE> getTargetEntity() {
		return targetEntity;
	}
}
