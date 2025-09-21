package org.codefilarete.stalactite.engine.configurer.manyToOne;

import javax.annotation.Nullable;
import java.util.function.BooleanSupplier;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * @author Guillaume Mary
 */
public class ManyToOneRelation<SRC, TRGT, TRGTID> {
	
	/** The method that gives the target entity from the source one */
	private final ReversibleAccessor<SRC, TRGT> targetProvider;
	
	/** Configuration used for target beans persistence */
	private final EntityMappingConfigurationProvider<TRGT, TRGTID> targetMappingConfiguration;
	
	private final BooleanSupplier sourceTablePerClassPolymorphic;
	
	@Nullable
	private final Table targetTable;
	
	private boolean nullable = true;
	
	/** the method that gets the "one" entity from the "many" entities */
	@Nullable
	private SerializableFunction<TRGT, SRC> reverseGetter;
	
	/** the method that sets the "one" entity onto the "many" entities */
	@Nullable
	private SerializableBiConsumer<TRGT, SRC> reverseSetter;
	
	@Nullable
	private Column<Table, SRC> reverseColumn;
	
	/** Default relation mode is {@link RelationMode#ALL} */
	private RelationMode relationMode = RelationMode.ALL;
	
	/**
	 * Indicates that relation must be loaded in same main query (through join) or in some separate query
	 */
	private boolean fetchSeparately;
	
	public <T extends Table> ManyToOneRelation(ReversibleAccessor<SRC, TRGT> targetProvider,
											   boolean sourceTablePerClassPolymorphic,
											   EntityMappingConfiguration<TRGT, TRGTID> targetMappingConfiguration,
											   T table) {
		this(targetProvider, () -> sourceTablePerClassPolymorphic, () -> targetMappingConfiguration, table);
	}
	
	public <T extends Table> ManyToOneRelation(ReversibleAccessor<SRC, TRGT> targetProvider,
											   BooleanSupplier sourceTablePerClassPolymorphic,
											   EntityMappingConfigurationProvider<? extends TRGT, TRGTID> targetMappingConfiguration,
											   T table) {
		this.sourceTablePerClassPolymorphic = sourceTablePerClassPolymorphic;
		this.targetMappingConfiguration = (EntityMappingConfigurationProvider<TRGT, TRGTID>) targetMappingConfiguration;
		this.targetProvider = targetProvider;
		this.targetTable = table;
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
	
	@Nullable
	public SerializableFunction<TRGT, SRC> getReverseGetter() {
		return reverseGetter;
	}
	
	public void setReverseGetter(@Nullable SerializableFunction<TRGT, SRC> reverseGetter) {
		this.reverseGetter = reverseGetter;
	}
	
	@Nullable
	public SerializableBiConsumer<TRGT, SRC> getReverseSetter() {
		return reverseSetter;
	}
	
	public void setReverseSetter(@Nullable SerializableBiConsumer<TRGT, SRC> reverseSetter) {
		this.reverseSetter = reverseSetter;
	}
	
	@Nullable
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
	
	public boolean isFetchSeparately() {
		return fetchSeparately;
	}
	
	public void setFetchSeparately(boolean fetchSeparately) {
		this.fetchSeparately = fetchSeparately;
	}
	
	public void fetchSeparately() {
		setFetchSeparately(true);
	}
}
