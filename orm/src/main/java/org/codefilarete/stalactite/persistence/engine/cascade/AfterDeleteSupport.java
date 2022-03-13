package org.codefilarete.stalactite.persistence.engine.cascade;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Predicates;
import org.codefilarete.stalactite.persistence.engine.listener.DeleteListener;

/**
 * @author Guillaume Mary
 */
public class AfterDeleteSupport<TRIGGER, TARGET> implements DeleteListener<TRIGGER> {
	
	private final Consumer<Iterable<TARGET>> afterDeleteAction;
	
	private final Function<TRIGGER, TARGET> targetProvider;
	
	private final Predicate<TARGET> targetFilter;
	
	public AfterDeleteSupport(Consumer<Iterable<TARGET>> afterDeleteAction, Function<TRIGGER, TARGET> targetProvider) {
		this(afterDeleteAction, targetProvider, Predicates.acceptAll());
	}
	
	public AfterDeleteSupport(Consumer<Iterable<TARGET>> afterDeleteAction, Function<TRIGGER, TARGET> targetProvider, Predicate<TARGET> targetFilter) {
		this.afterDeleteAction = afterDeleteAction;
		this.targetProvider = targetProvider;
		this.targetFilter = targetFilter;
	}

	@Override
	public void afterDelete(Iterable<TRIGGER> entities) {
		afterDeleteAction.accept(Iterables.stream(entities).map(targetProvider).filter(targetFilter).collect(Collectors.toList()));
	}
}
