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
public class MappedManyRelationDescriptor<SRC, TRGT, C extends Collection<TRGT>, SRCID, TRGTTABLE extends Table<TRGTTABLE>> extends ManyRelationDescriptor<SRC, TRGT, C> {
	
	private final Key<TRGTTABLE, SRCID> reverseColumn;
	
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
										Key<TRGTTABLE, SRCID> reverseColumn,
										boolean maintainAssociationOnly,
										boolean orphanRemoval) {
		super(collectionGetter, collectionFactory, reverseSetter, maintainAssociationOnly, orphanRemoval);
		this.reverseColumn = reverseColumn;
	}
	
	public Key<TRGTTABLE, SRCID> getReverseColumn() {
		return reverseColumn;
	}
}
