package org.stalactite.persistence.mapping;

import javax.annotation.Nonnull;

/**
 * @author mary
 */
public interface IMappingStrategy<T> {
	
	PersistentValues getInsertValues(@Nonnull T t);
	
	PersistentValues getUpdateValues(@Nonnull T modified, @Nonnull T unmodified);
	
	PersistentValues getDeleteValues(@Nonnull T t);
	
	PersistentValues getSelectValues(@Nonnull T t);
	
	PersistentValues getVersionedKeyValues(@Nonnull T t);
}
