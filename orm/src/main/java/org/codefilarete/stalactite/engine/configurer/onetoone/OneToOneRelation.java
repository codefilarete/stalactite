package org.codefilarete.stalactite.engine.configurer.onetoone;

import javax.annotation.Nullable;
import java.util.function.BooleanSupplier;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * @author Guillaume Mary
 */
public class OneToOneRelation<SRC, TRGT, TRGTID> {
	
	/** The method that gives the target entity from the source one */
	private final ReversibleAccessor<SRC, TRGT> targetProvider;
	
	/** Configuration used for target beans persistence */
	private final EntityMappingConfigurationProvider<TRGT, TRGTID> targetMappingConfiguration;
	
	private final BooleanSupplier sourceTablePerClassPolymorphic;
	
	private boolean nullable = true;
	
	/** the method that gets the "one" entity from the "many" entities */
	@Nullable
	private SerializableFunction<TRGT, SRC> reverseGetter;
	
	/** the method that sets the "one" entity onto the "many" entities */
	@Nullable
	private SerializableBiConsumer<TRGT, SRC> reverseSetter;
	
	@Nullable
	private Column<Table, SRC> reverseColumn;
	
	@Nullable
	private String reverseColumnName;
	
	/** Default relation mode is {@link RelationMode#ALL} */
	private RelationMode relationMode = RelationMode.ALL;
	
	/**
	 * Indicates that relation must be loaded in same main query (through join) or in some separate query
	 */
	private boolean fetchSeparately;
	
	private boolean unique;
	
	public OneToOneRelation(ReversibleAccessor<SRC, TRGT> targetProvider,
							BooleanSupplier sourceTablePerClassPolymorphic,
							EntityMappingConfigurationProvider<? extends TRGT, TRGTID> targetMappingConfiguration) {
		this.sourceTablePerClassPolymorphic = sourceTablePerClassPolymorphic;
		this.targetMappingConfiguration = (EntityMappingConfigurationProvider<TRGT, TRGTID>) targetMappingConfiguration;
		this.targetProvider = targetProvider;
	}
	
	/** Original method reference given for mapping */
	public ReversibleAccessor<SRC, TRGT> getTargetProvider() {
		return targetProvider;
	}
	
	public boolean isSourceTablePerClassPolymorphic() {
		return sourceTablePerClassPolymorphic.getAsBoolean();
	}
	
	/** @return the configuration used for target beans persistence */
	public EntityMappingConfiguration<TRGT, TRGTID> getTargetMappingConfiguration() {
		return targetMappingConfiguration.getConfiguration();
	}
	
	public boolean isTargetTablePerClassPolymorphic() {
		return getTargetMappingConfiguration().getPolymorphismPolicy() instanceof PolymorphismPolicy.TablePerClassPolymorphism;
	}
	
	/** Nullable option, mainly for column join and DDL schema generation */
	public boolean isNullable() {
		return nullable;
	}
	
	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}
	
	public void setUnique(boolean unique) {
		this.unique = unique;
	}
	
	public boolean isUnique() {
		return unique;
	}
	
	@Nullable
	public SerializableFunction<TRGT, SRC> getReverseGetter() {
		return reverseGetter;
	}
	
	public void setReverseGetter(@Nullable SerializableFunction<? super TRGT, SRC> reverseGetter) {
		this.reverseGetter = (SerializableFunction<TRGT, SRC>) reverseGetter;
	}
	
	@Nullable
	public SerializableBiConsumer<TRGT, SRC> getReverseSetter() {
		return reverseSetter;
	}
	
	public void setReverseSetter(@Nullable SerializableBiConsumer<? super TRGT, SRC> reverseSetter) {
		this.reverseSetter = (SerializableBiConsumer<TRGT, SRC>) reverseSetter;
	}
	
	@Nullable
	public <T extends Table, O> Column<T, O> getReverseColumn() {
		return (Column<T, O>) reverseColumn;
	}
	
	public void setReverseColumn(Column reverseSide) {
		this.reverseColumn = reverseSide;
	}
	
	@Nullable
	public String getReverseColumnName() {
		return reverseColumnName;
	}
	
	public void setReverseColumn(@Nullable String reverseColumnName) {
		this.reverseColumnName = reverseColumnName;
	}
	
	public RelationMode getRelationMode() {
		return relationMode;
	}
	
	public void setRelationMode(RelationMode relationMode) {
		this.relationMode = relationMode;
	}
	
	/**
	 * Indicates if relation is owned by target entities table
	 * @return true if one of {@link #getReverseSetter()}, {@link #getReverseGetter()}, {@link #getReverseColumn()},
	 * {@link #getReverseColumnName()} is not null
	 */
	public boolean isRelationOwnedByTarget() {
		return getReverseSetter() != null || getReverseGetter() != null
				|| getReverseColumn() != null || getReverseColumnName() != null;
	}
	
	public boolean isFetchSeparately() {
		return fetchSeparately;
	}
	
	public void setFetchSeparately(boolean fetchSeparately) {
		this.fetchSeparately = fetchSeparately;
	}
	
	public void fetchSeparately() {
		setFetchSeparately(true);
	}
	
	/**
	 * Clones this object to create a new one with the given accessor as prefix of current one.
	 * Made to "slide" current instance with an accessor prefix. Used for embeddable objects with relation to make the relation being accessible
	 * from the "root" entity.
	 *
	 * @param accessor the prefix of the clone to be created
	 * @return a clones of this instance prefixed with the given accessor
	 * @param <C> the root entity type that owns the embeddable which has this relation
	 */
	public <C> OneToOneRelation<C, TRGT, TRGTID> embedInto(Accessor<C, SRC> accessor) {
		AccessorChain<C, TRGT> slidedTargetProvider = new AccessorChain<>(accessor, targetProvider);
		OneToOneRelation<C, TRGT, TRGTID> result = new OneToOneRelation<>(slidedTargetProvider, this::isSourceTablePerClassPolymorphic, this::getTargetMappingConfiguration);
		result.setRelationMode(this.getRelationMode());
		result.setNullable(this.isNullable());
		result.setFetchSeparately(this.isFetchSeparately());
		return result;
	}
}
