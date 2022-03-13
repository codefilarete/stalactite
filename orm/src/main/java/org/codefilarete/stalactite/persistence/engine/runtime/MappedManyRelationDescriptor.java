package org.codefilarete.stalactite.persistence.engine.runtime;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Table;

/**
 * Container to store information of a one-to-many mapped relation (by a column on the reverse side)
 * 
 * @author Guillaume Mary
 */
public class MappedManyRelationDescriptor<I, O, C extends Collection<O>> extends ManyRelationDescriptor<I, O, C> {
	
	private final Column<Table, I> reverseColumn;
	
	/**
	 * @param collectionGetter collection accessor
	 * @param collectionSetter collection setter
	 * @param collectionFactory collection factory
	 * @param reverseSetter setter on the owning side for source bean, optional
	 * 		because it can be either a {@link org.danekja.java.util.function.serializable.SerializableFunction},
	 * 		or {@link Function} took from an {@link Accessor}
	 * @param reverseColumn
	 */
	public MappedManyRelationDescriptor(Function<I, C> collectionGetter,
										BiConsumer<I, C> collectionSetter,
										Supplier<C> collectionFactory,
										@Nullable BiConsumer<O, I> reverseSetter,
										Column<Table, I> reverseColumn) {
		super(collectionGetter, collectionSetter, collectionFactory, reverseSetter);
		this.reverseColumn = reverseColumn;
	}
	
	public Column<Table, I> getReverseColumn() {
		return reverseColumn;
	}
}
