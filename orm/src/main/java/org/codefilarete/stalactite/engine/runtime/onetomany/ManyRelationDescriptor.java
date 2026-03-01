package org.codefilarete.stalactite.engine.runtime.onetomany;

import java.util.Collection;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.Mutator;
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
	
	private final Accessor<I, C> collectionProvider;
	
	private final Mutator<I, C> collectionSetter;
	
	private final Supplier<C> collectionFactory;
	
	private final Mutator<O, I> reverseSetter;
	
	protected BeanRelationFixer<I, O> relationFixer;
	
	/**
	 * @param collectionProvider collection accessor
	 * @param collectionSetter collection setter
	 * @param collectionFactory collection factory
	 * @param reverseSetter
	 */
	public ManyRelationDescriptor(Accessor<I, C> collectionProvider,
								  Mutator<I, C> collectionSetter,
								  Supplier<C> collectionFactory,
								  @Nullable Mutator<O, I> reverseSetter) {
		this(collectionProvider,
				collectionSetter,
				collectionFactory,
				reverseSetter,
				BeanRelationFixer.of(collectionSetter, collectionProvider, collectionFactory, Objects.preventNull(reverseSetter, (Mutator<O, I>) NOOP_REVERSE_SETTER))
		);
	}
	
	public ManyRelationDescriptor(Accessor<I, C> collectionProvider,
								  Mutator<I, C> collectionSetter,
								  Supplier<C> collectionFactory,
								  Mutator<O, I> reverseSetter,
								  BeanRelationFixer<I, O> relationFixer) {
		this.collectionProvider = collectionProvider;
		this.collectionSetter = collectionSetter;
		this.collectionFactory = collectionFactory;
		this.reverseSetter = reverseSetter;
		this.relationFixer = relationFixer;
	}

	public Accessor<I, C> getCollectionProvider() {
		return collectionProvider;
	}
	
	public Accessor<I, C> getCollectionGetter() {
		return collectionProvider;
	}
	
	public Mutator<I, C> getCollectionSetter() {
		return collectionSetter;
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
}
