package org.gama.stalactite.persistence.id.sequence;

import java.util.Map;

import org.gama.lang.bean.Converter;
import org.gama.lang.bean.Objects;

/**
 * Classe de stockage de la configuration de {@link PooledHiLoSequence}
 * 
 * @author Guillaume Mary
 */
public class PooledHiLoSequenceOptions {
	
	private static final String POOL_SIZE_PARAM = "poolSize";
	private static final String SEQUENCE_NAME_PARAM = "sequenceName";
	private static final String TABLE_PARAM = "table";
	private static final String SEQUENCE_COLUMN_PARAM = "sequenceNameColumn";
	private static final String VALUE_COLUMN_PARAM = "valueColumn";
	
	private static final Converter CONVERTER = Converter.DEFAULT;
	
	private final int poolSize;
	private final String sequenceName;
	private final SequenceStorageOptions storageOptions;
	private final long initialValue;
	
	public PooledHiLoSequenceOptions(int poolSize, String sequenceName, SequenceStorageOptions storageOptions) {
		this(poolSize, sequenceName, Objects.preventNull(storageOptions, SequenceStorageOptions.DEFAULT), 0);
	}
	
	/**
	 * Detailed constructor.
	 * 
	 * @param poolSize the size of the pool
	 * @param sequenceName the name of the sequence, most likely unique for the given {@link SequenceStorageOptions}
	 * @param storageOptions options for storing the sequence in the database
	 * @param initialValue the initial value for the very first insertion, never used again
	 */
	public PooledHiLoSequenceOptions(int poolSize, String sequenceName, SequenceStorageOptions storageOptions, long initialValue) {
		this.poolSize = poolSize;
		this.sequenceName = sequenceName;
		this.storageOptions = Objects.preventNull(storageOptions, SequenceStorageOptions.DEFAULT);
		this.initialValue = initialValue;
	}
	
	public PooledHiLoSequenceOptions(Map<String, Object> configuration) {
		this(CONVERTER.asInteger(configuration.get(POOL_SIZE_PARAM)),
				CONVERTER.asString(configuration.get(SEQUENCE_NAME_PARAM)),
				new SequenceStorageOptions(CONVERTER.asString(configuration.get(TABLE_PARAM)),
						CONVERTER.asString(configuration.get(SEQUENCE_COLUMN_PARAM)),
						CONVERTER.asString(configuration.get(VALUE_COLUMN_PARAM))
				)
		);
	}
	
	public int getPoolSize() {
		return poolSize;
	}
	
	public String getSequenceName() {
		return sequenceName;
	}
	
	public SequenceStorageOptions getStorageOptions() {
		return storageOptions;
	}
	
	/**
	 * To be used only the very first time of insertion of the sequence in the database
	 * 
	 * @return the value passed at constructor
	 */
	public long getInitialValue() {
		return initialValue;
	}
}
