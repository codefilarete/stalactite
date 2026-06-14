package org.codefilarete.stalactite.engine.configurer.map;

import java.util.Map;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.VisibleForTesting;

/**
 * Mapping strategy dedicated to {@link KeyValueRecord}. Very close to {@link org.codefilarete.stalactite.engine.configurer.AssociationRecordMapping}
 * in its principle.
 *
 * @param <K> embedded key bean type
 * @param <V> embedded value bean type
 * @param <I> source identifier type
 * @param <T> relation table type
 * @author Guillaume Mary
 */
public class KeyValueRecordMapping<K, V, I, T extends Table<T>> extends DefaultEntityMapping<KeyValueRecord<K, V, I>, RecordId<K, I>, T> {
	
	@VisibleForTesting
	public KeyValueRecordMapping(T targetTable,
	                             Map<? extends ReadWritePropertyAccessPoint<KeyValueRecord<K, V, I>, ?>, Column<T, ?>> propertyToColumn,
	                             KeyValueRecordIdMapping<K, I, T> idMapping) {
		super((Class) KeyValueRecord.class,
				targetTable,
				propertyToColumn,
				// cast because idMapping has KeyValueRecord<K, ?, I> as generics instead of KeyValueRecord<K, V, I>
				(ComposedIdMapping<KeyValueRecord<K, V, I>, RecordId<K, I>>) (ComposedIdMapping) idMapping);
	}
}
