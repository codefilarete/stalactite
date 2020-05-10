package org.gama.stalactite.persistence.engine;

import javax.annotation.Nullable;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public class CascadeOne<SRC, TRGT, TRGTID> {
	
	/** The method that gives the target entity from the source one */
	private final IReversibleAccessor<SRC, TRGT> targetProvider;
	
	/** Configuration used for target beans persistence */
	private final EntityMappingConfiguration<TRGT, TRGTID> targetMappingConfiguration;
	
	private final Table targetTable;
	
	private boolean nullable = true;
	
	/** the method that gets the "one" entity from the "many" entities */
	private SerializableFunction<TRGT, SRC> reverseGetter;
	
	/** the method that sets the "one" entity onto the "many" entities */
	private SerializableBiConsumer<TRGT, SRC> reverseSetter;
	
	private Column<Table, SRC> reverseColumn;
	
	/** Default relation mode is {@link RelationMode#ALL} */
	private RelationMode relationMode = RelationMode.ALL;
	
	<T extends Table> CascadeOne(IReversibleAccessor<SRC, TRGT> targetProvider, EntityMappingConfiguration<TRGT, TRGTID> targetMappingConfiguration, T table) {
		this.targetMappingConfiguration = targetMappingConfiguration;
		this.targetProvider = targetProvider;
		this.targetTable = table;
	}
	
	/** Original method reference given for mapping */
	public IReversibleAccessor<SRC, TRGT> getTargetProvider() {
		return targetProvider;
	}
	
	/** @return the configuration used for target beans persistence */
	public EntityMappingConfiguration<TRGT, TRGTID> getTargetMappingConfiguration() {
		return targetMappingConfiguration;
	}
	
	@Nullable
	public Table getTargetTable() {
		return this.targetTable;
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
	
	public <T extends Table, O> Column<T, O> getReverseColumn() {
		return (Column<T, O>) reverseColumn;
	}
	
	public void setReverseColumn(Column reverseSide) {
		this.reverseColumn = reverseSide;
	}
	
	public RelationMode getRelationMode() {
		return relationMode;
	}
	
	public void setRelationMode(RelationMode relationMode) {
		this.relationMode = relationMode;
	}
	
	/**
	 * Indicates if relation is owned by target entities table
	 * @return true if one of {@link #getReverseSetter()}, {@link #getReverseGetter()}, {@link #getReverseColumn()} is not null
	 */
	public boolean isRelationOwnedByTarget() {
		return getReverseSetter() != null || getReverseGetter() != null || getReverseColumn() != null;
	}
}
