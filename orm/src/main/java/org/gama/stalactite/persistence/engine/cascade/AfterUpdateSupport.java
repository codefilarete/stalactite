package org.gama.stalactite.persistence.engine.cascade;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.gama.lang.Duo;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.Predicates;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;

/**
 * @author Guillaume Mary
 */
public class AfterUpdateSupport<TRIGGER, TARGET> implements UpdateListener<TRIGGER> {
	
	private final BiConsumer<Iterable<Duo<TARGET, TARGET>>, Boolean> afterUpdateAction;
	
	private final Function<TRIGGER, TARGET> targetProvider;
	
	private final Predicate<Duo<TARGET, TARGET>> targetFilter;
	
	public AfterUpdateSupport(BiConsumer<Iterable<Duo<TARGET, TARGET>>, Boolean> afterUpdateAction, Function<TRIGGER, TARGET> targetProvider) {
		this(afterUpdateAction, targetProvider, Predicates.acceptAll());
	}
	
	public AfterUpdateSupport(BiConsumer<Iterable<Duo<TARGET, TARGET>>, Boolean> afterUpdateAction, Function<TRIGGER, TARGET> targetProvider, Predicate<Duo<TARGET, TARGET>> targetFilter) {
		this.afterUpdateAction = afterUpdateAction;
		this.targetProvider = targetProvider;
		this.targetFilter = targetFilter;
	}
	
	@Override
	public void afterUpdate(Iterable<UpdatePayload<? extends TRIGGER, ?>> entities, boolean allColumnsStatement) {
		afterUpdateAction.accept(Iterables.stream(entities)
						.map(e -> new Duo<>(targetProvider.apply(e.getEntities().getLeft()), targetProvider.apply(e.getEntities().getRight())))
						.filter(targetFilter).collect(Collectors.toList()),
				allColumnsStatement);
	}
}
