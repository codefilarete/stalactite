package org.codefilarete.stalactite.engine.runtime.onetomany;

import java.util.Collection;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.PropertyMutator;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.bean.Objects;

/**
 * Container to store information of a one-to-many relation
 * 
 * @author Guillaume Mary
 */
public class ManyRelationDescriptor<I, O, C extends Collection<O>> {
	
	/** Empty setter for applying source entity to target entity (reverse side) */
	protected static final Mutator NOOP_REVERSE_SETTER = (o, i) -> {};
	
	private final ReadWritePropertyAccessPoint<I, C> collectionAccessPoint;
	
	private final Supplier<C> collectionFactory;
	
	private final PropertyMutator<O, I> reverseSetter;
	
	protected BeanRelationFixer<I, O> relationFixer;
	
	private final boolean maintainAssociationOnly;
	
	private final boolean orphanRemoval;
	
	/**
	 * @param collectionAccessPoint collection accessor
	 * @param collectionFactory collection factory
	 * @param reverseSetter
	 */
	public ManyRelationDescriptor(ReadWritePropertyAccessPoint<I, C> collectionAccessPoint,
	                              Supplier<C> collectionFactory,
	                              @Nullable PropertyMutator<O, I> reverseSetter,
	                              boolean maintainAssociationOnly,
								  boolean orphanRemoval) {
		this(collectionAccessPoint,
				collectionFactory,
				reverseSetter,
				BeanRelationFixer.of(collectionAccessPoint, collectionFactory, Objects.preventNull(reverseSetter, (Mutator<O, I>) NOOP_REVERSE_SETTER)), maintainAssociationOnly, orphanRemoval
		);
	}
	
	public ManyRelationDescriptor(ReadWritePropertyAccessPoint<I, C> collectionAccessPoint,
	                              Supplier<C> collectionFactory,
	                              PropertyMutator<O, I> reverseSetter,
	                              BeanRelationFixer<I, O> relationFixer,
	                              boolean maintainAssociationOnly,
								  boolean orphanRemoval) {
		this.collectionAccessPoint = collectionAccessPoint;
		this.collectionFactory = collectionFactory;
		this.reverseSetter = reverseSetter;
		this.relationFixer = relationFixer;
		this.maintainAssociationOnly = maintainAssociationOnly;
		this.orphanRemoval = orphanRemoval;
	}
	
	public ReadWritePropertyAccessPoint<I, C> getCollectionAccessPoint() {
		return collectionAccessPoint;
	}
	
	public Supplier<C> getCollectionFactory() {
		return collectionFactory;
	}
	
	/**
	 * Gives the setter for source bean on the owning side. 
	 *
	 * @return null if no setter given at construction time
	 */
	@Nullable
	public Mutator<O, I> getReverseSetter() {
		return reverseSetter;
	}
	
	public BeanRelationFixer<I, O> getRelationFixer() {
		return relationFixer;
	}
	
	public boolean isMaintainAssociationOnly() {
		return maintainAssociationOnly;
	}
	
	public boolean isOrphanRemoval() {
		return orphanRemoval;
	}
}
