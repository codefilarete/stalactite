package org.codefilarete.stalactite.sql.ddl.structure;

import javax.annotation.Nullable;

import org.codefilarete.stalactite.sql.ddl.structure.Database.Schema;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Representation of a database sequence.
 *
 * @author Guillaume Mary
 */
public class Sequence {
	
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
	
	public String getName() {
		return name;
	}
	
	public String getAbsoluteName() {
		return absoluteName;
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
