package org.gama.stalactite.persistence.engine;

import java.util.concurrent.atomic.AtomicInteger;

import org.gama.stalactite.persistence.engine.BeforeInsertIdentifierManager.Sequence;

/**
 * Simple id gnerator for our tests : increments a in-memory counter.
 */
public class InMemoryCounterIdentifierGenerator implements Sequence<Integer> {
	
//	BeforeInsertIdentifierManager
	public static final InMemoryCounterIdentifierGenerator INSTANCE = new InMemoryCounterIdentifierGenerator();
	
	public InMemoryCounterIdentifierGenerator() {
		
	}
	
	private AtomicInteger idCounter = new AtomicInteger(0);
	
	@Override
	public Integer next() {
		return idCounter.addAndGet(1);
	}
	
	public void reset() {
		idCounter.set(0);
	}
}
