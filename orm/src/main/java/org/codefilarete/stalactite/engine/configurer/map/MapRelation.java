package org.codefilarete.stalactite.engine.configurer.map;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.function.Supplier;

import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.configurer.LambdaMethodUnsheller;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * Storage of configuration of a {@link Map} relation (kind of "element collection" but with a {@link Map}).
 * 
 * @param <SRC> entity type owning relation
 * @param <K> Map entry key type
 * @param <V> Map entry value key type
 * @param <M> Map type
 * @author Guillaume Mary
 */
public class MapRelation<SRC, K, V, M extends Map<K, V>> {
	
	/** The method that gives the entities from the "root" entity */
	private final ReversibleAccessor<SRC, M> mapProvider;
	private final Class<K> keyType;
	private final Class<V> valueType;
	/** Optional provider of {@link Map} instance to be used if collection value is null */
	private Supplier<M> mapFactory;
	
	private Table targetTable;
	private String targetTableName;
	private Column<Table, ?> reverseColumn;
	private String reverseColumnName;
	
	/** key column name override */
	private String keyColumnName;
	
	private String valueColumnName;
	
	/** Complex type mapping, optional */
	@Nullable
	private EntityMappingConfigurationProvider<K, ?> keyEntityConfigurationProvider;
	@Nullable
	private EmbeddableMappingConfigurationProvider<K> keyEmbeddableConfigurationProvider;
	
	@Nullable
	private EntityMappingConfigurationProvider<V, ?> valueEntityConfigurationProvider;
	@Nullable
	private EmbeddableMappingConfigurationProvider<V> valueEmbeddableConfigurationProvider;
	
	private boolean fetchSeparately;
	
	public MapRelation(SerializableFunction<SRC, M> getter,
					   Class<K> keyType,
					   Class<V> valueType,
					   LambdaMethodUnsheller lambdaMethodUnsheller) {
		this.keyType = keyType;
		this.valueType = valueType;
		AccessorByMethodReference<SRC, M> getterReference = Accessors.accessorByMethodReference(getter);
		this.mapProvider = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				getterReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<SRC, M>(lambdaMethodUnsheller.captureLambdaMethod(getter)).toMutator());
	}
	
	public ReversibleAccessor<SRC, M> getMapProvider() {
		return mapProvider;
	}
	
	public Class<K> getKeyType() {
		return keyType;
	}
	
	public Class<V> getValueType() {
		return valueType;
	}
	
	public Supplier<M> getMapFactory() {
		return mapFactory;
	}
	
	public void setMapFactory(Supplier<M> mapFactory) {
		this.mapFactory = mapFactory;
	}
	
	public Table getTargetTable() {
		return targetTable;
	}
	
	public void setTargetTable(Table targetTable) {
		this.targetTable = targetTable;
	}
	
	public String getTargetTableName() {
		return targetTableName;
	}
	
	public void setTargetTableName(String targetTableName) {
		this.targetTableName = targetTableName;
	}
	
	public <I> Column<Table, I> getReverseColumn() {
		return (Column<Table, I>) reverseColumn;
	}
	
	public void setReverseColumn(Column<Table, ?> reverseColumn) {
		this.reverseColumn = reverseColumn;
	}
	
	public String getReverseColumnName() {
		return reverseColumnName;
	}
	
	public void setReverseColumnName(String reverseColumnName) {
		this.reverseColumnName = reverseColumnName;
	}
	
	public void setKeyConfigurationProvider(@Nullable EntityMappingConfigurationProvider<K, ?> keyConfigurationProvider) {
		this.keyEntityConfigurationProvider = keyConfigurationProvider;
	}
	
	@Nullable
	public EmbeddableMappingConfigurationProvider<K> getKeyEmbeddableConfigurationProvider() {
		return keyEmbeddableConfigurationProvider;
	}
	
	public void setKeyConfigurationProvider(@Nullable EmbeddableMappingConfigurationProvider<K> keyConfigurationProvider) {
		this.keyEmbeddableConfigurationProvider = keyConfigurationProvider;
	}
	
	@Nullable
	public EntityMappingConfigurationProvider<K, ?> getKeyEntityConfigurationProvider() {
		return keyEntityConfigurationProvider;
	}
	
	public void setValueConfigurationProvider(@Nullable EntityMappingConfigurationProvider<V, ?> valueConfigurationProvider) {
		this.valueEntityConfigurationProvider = valueConfigurationProvider;
	}
	
	@Nullable
	public EntityMappingConfigurationProvider<V, ?> getValueEntityConfigurationProvider() {
		return valueEntityConfigurationProvider;
	}
	
	public void setValueConfigurationProvider(@Nullable EmbeddableMappingConfigurationProvider<V> valueConfigurationProvider) {
		this.valueEmbeddableConfigurationProvider = valueConfigurationProvider;
	}
	
	@Nullable
	public EmbeddableMappingConfigurationProvider<V> getValueEmbeddableConfigurationProvider() {
		return valueEmbeddableConfigurationProvider;
	}
	
	public void setKeyColumnName(String keyColumnName) {
		this.keyColumnName = keyColumnName;
	}
	
	public String getKeyColumnName() {
		return keyColumnName;
	}
	
	public void setValueColumnName(String valueColumnName) {
		this.valueColumnName = valueColumnName;
	}
	
	public String getValueColumnName() {
		return valueColumnName;
	}
	
	public boolean isFetchSeparately() {
		return fetchSeparately;
	}
	
	public void fetchSeparately() {
		this.fetchSeparately = true;
	}
}
