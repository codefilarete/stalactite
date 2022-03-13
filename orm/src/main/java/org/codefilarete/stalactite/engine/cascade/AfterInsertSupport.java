package org.codefilarete.stalactite.engine.cascade;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Predicates;
import org.codefilarete.stalactite.engine.listener.InsertListener;

/**
 * @author Guillaume Mary
 */
public class AfterInsertSupport<TRIGGER, TARGET> implements InsertListener<TRIGGER> {
	
	private final Consumer<Iterable<TARGET>> afterInsertAction;
	
	private final Function<TRIGGER, TARGET> targetProvider;
	
	private final Predicate<TARGET> targetFilter;
	
	public AfterInsertSupport(Consumer<Iterable<TARGET>> afterInsertAction, Function<TRIGGER, TARGET> targetProvider) {
		this(afterInsertAction, targetProvider, Predicates.acceptAll());
	}
	
	public AfterInsertSupport(Consumer<Iterable<TARGET>> afterInsertAction, Function<TRIGGER, TARGET> targetProvider, Predicate<TARGET> targetFilter) {
		this.afterInsertAction = afterInsertAction;
		this.targetProvider = targetProvider;
		this.targetFilter = targetFilter;
	}

	@Override
	public void afterInsert(Iterable<? extends TRIGGER> entities) {
		afterInsertAction.accept(Iterables.stream(entities).map(targetProvider).filter(targetFilter).collect(Collectors.toSet()));
	}
}
