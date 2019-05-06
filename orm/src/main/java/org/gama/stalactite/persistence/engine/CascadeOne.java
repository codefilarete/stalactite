package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public class CascadeOne<SRC, TRGT, TRGTID> {
	
	/** The method that gives the target entity from the source one */
	private final IReversibleAccessor<SRC, TRGT> targetAccessor;
	
	/** Target entity {@link Persister} */
	private final Persister<TRGT, TRGTID, ? extends Table> targetPersister;
	
	private boolean nullable = true;
	
	/** the method that gets the "one" entity from the "many" entities */
	private SerializableFunction<TRGT, SRC> reverseGetter;
	
	/** the method that sets the "one" entity onto the "many" entities */
	private SerializableBiConsumer<TRGT, SRC> reverseSetter;
	
	private Column<Table, SRC> reverseColumn;
	
	private RelationshipMode relationshipMode = RelationshipMode.READ_ONLY;
	
	CascadeOne(IReversibleAccessor<SRC, TRGT> targetProvider, Persister<TRGT, TRGTID, ? extends Table> targetPersister) {
		this.targetPersister = targetPersister;
		targetAccessor = targetProvider;
	}
	
	/** Original method reference given for mapping */
	public IReversibleAccessor<SRC, TRGT> getTargetProvider() {
		return targetAccessor;
	}
	
	/** The {@link Persister} that will be used to persist the target of the relation */
	public Persister<TRGT, TRGTID, ?> getTargetPersister() {
		return targetPersister;
	}
	
	/** Nullable option, mainly for column join and DDL schema generation */
	public boolean isNullable() {
		return nullable;
	}
	
	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}
	
	public SerializableFunction<TRGT, SRC> getReverseGetter() {
		return reverseGetter;
	}
	
	public void setReverseGetter(SerializableFunction<TRGT, SRC> reverseGetter) {
		this.reverseGetter = reverseGetter;
	}
	
	public SerializableBiConsumer<TRGT, SRC> getReverseSetter() {
		return reverseSetter;
	}
	
	public void setReverseSetter(SerializableBiConsumer<TRGT, SRC> reverseSetter) {
		this.reverseSetter = reverseSetter;
	}
	
	public Column getReverseColumn() {
		return reverseColumn;
	}
	
	public void setReverseColumn(Column reverseSide) {
		this.reverseColumn = reverseSide;
	}
	
	public RelationshipMode getRelationshipMode() {
		return relationshipMode;
	}
	
	public void setRelationshipMode(RelationshipMode relationshipMode) {
		this.relationshipMode = relationshipMode;
	}
	
	/**
	 * Indicates if relation is owned by target entities table
	 * @return true if one of {@link #getReverseSetter()}, {@link #getReverseGetter()}, {@link #getReverseColumn()} is not null
	 */
	public boolean isOwnedByReverseSide() {
		return getReverseSetter() != null || getReverseGetter() != null || getReverseColumn() != null;
	}
}
