package org.codefilarete.stalactite.engine.runtime.onetomany;

import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.PropertyMutator;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Container to store information of a one-to-many mapped relation (by a column on the reverse side)
 * 
 * @author Guillaume Mary
 */
public class MappedManyRelationDescriptor<SRC, TRGT, C extends Collection<TRGT>, SRCID> extends ManyRelationDescriptor<SRC, TRGT, C> {
	
	private final Key<Table<?>, SRCID> reverseColumn;
	
	/**
	 * @param collectionGetter collection accessor
	 * @param collectionFactory collection factory
	 * @param reverseSetter setter on the owning side for source bean, optional
	 * 		because it can be either a {@link org.danekja.java.util.function.serializable.SerializableFunction},
	 * 		or {@link Function} took from an {@link Accessor}
	 * @param reverseColumn column owning relation
	 */
	public MappedManyRelationDescriptor(ReadWritePropertyAccessPoint<SRC, C> collectionGetter,
										Supplier<C> collectionFactory,
										@Nullable PropertyMutator<TRGT, SRC> reverseSetter,
										Key<?, SRCID> reverseColumn) {
		super(collectionGetter, collectionFactory, reverseSetter);
		this.reverseColumn = (Key<Table<?>, SRCID>) reverseColumn;
	}
	
	public Key<Table<?>, SRCID> getReverseColumn() {
		return reverseColumn;
	}
}
