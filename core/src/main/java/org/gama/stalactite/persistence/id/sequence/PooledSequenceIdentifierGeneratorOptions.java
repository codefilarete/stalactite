package org.gama.stalactite.persistence.id.sequence;

import java.util.Map;

import org.gama.lang.bean.Converter;
import org.gama.lang.bean.Objects;

/**
 * Classe de stockage de la configuration de {@link PooledSequenceIdentifierGenerator}
 * 
 * @author mary
 */
public class PooledSequenceIdentifierGeneratorOptions {
	
	private static final String POOL_SIZE_PARAM = "poolSize";
	private static final String SEQUENCE_NAME_PARAM = "sequenceName";
	private static final String TABLE_PARAM = "table";
	private static final String SEQUENCE_COLUMN_PARAM = "sequenceNameColumn";
	private static final String VALUE_COLUMN_PARAM = "valueColumn";
	
	private static final Converter CONVERTER = Converter.DEFAULT;
	
	private final int poolSize;
	private final String sequenceName;
	private final SequencePersisterOptions storageOptions;
	
	public PooledSequenceIdentifierGeneratorOptions(int poolSize, String sequenceName, SequencePersisterOptions storageOptions) {
		this.poolSize = poolSize;
		this.sequenceName = sequenceName;
		this.storageOptions = Objects.preventNull(storageOptions, SequencePersisterOptions.DEFAULT);
	}
	
	public PooledSequenceIdentifierGeneratorOptions(Map<String, Object> configuration) {
		this(CONVERTER.asInteger(configuration.get(POOL_SIZE_PARAM)),
				CONVERTER.asString(configuration.get(SEQUENCE_NAME_PARAM)),
				new SequencePersisterOptions(CONVERTER.asString(configuration.get(TABLE_PARAM)),
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
	
	public SequencePersisterOptions getStorageOptions() {
		return storageOptions;
	}
}
