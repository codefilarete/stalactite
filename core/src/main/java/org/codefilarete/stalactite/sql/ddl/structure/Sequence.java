package org.codefilarete.stalactite.sql.ddl.structure;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.query.api.Fromable;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Database.Schema;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Representation of a database sequence.
 *
 * @author Guillaume Mary
 */
public class Sequence implements Fromable {
	
	@Nullable
	private final Schema schema;
	
	private final String name;
	
	private final String absoluteName;
	
	@Nullable
	private Integer initialValue;
	
	@Nullable
	private Integer batchSize;
	
	public Sequence(String name) {
		this(null, name);
	}
	
	public Sequence(@Nullable Schema schema, String name) {
		this.schema = schema;
		if (this.schema != null) {
			this.schema.addSequence(this);
		}
		this.name = name;
		this.absoluteName = nullable(schema).map(Schema::getName).map(s -> s + "." + name).getOr(name);
	}
	
	@Nullable
	public Schema getSchema() {
		return schema;
	}
	
	@Override
	public String getName() {
		return name;
	}
	
	@Override
	public String getAbsoluteName() {
		return absoluteName;
	}
	
	/**
	 * Returns an empty {@link Set} because sequences are quite erratic across database vendors and some have columns, others have some functions.
	 * @return an empty {@link Set}
	 */
	@Override
	public Set<? extends Selectable<?>> getColumns() {
		return Collections.emptySet();
	}
	
	/**
	 * @return an empty {@link Map}
	 */
	@Override
	public Map<Selectable<?>, String> getAliases() {
		return Collections.emptyMap();
	}
	
	@Nullable
	public Integer getInitialValue() {
		return initialValue;
	}
	
	public void setInitialValue(@Nullable Integer initialValue) {
		this.initialValue = initialValue;
	}
	
	public Sequence withInitialValue(@Nullable Integer initialValue) {
		this.initialValue = initialValue;
		return this;
	}
	
	@Nullable
	public Integer getBatchSize() {
		return batchSize;
	}
	
	public void setBatchSize(@Nullable Integer batchSize) {
		this.batchSize = batchSize;
	}
	
	public Sequence withBatchSize(@Nullable Integer batchSize) {
		this.batchSize = batchSize;
		return this;
	}
}
