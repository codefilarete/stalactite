package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.id.generator.BeforeInsertIdentifierGenerator;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple id gnerator for our tests : increments a in-memory counter.
 */
public class InMemoryCounterIdentifierGenerator implements BeforeInsertIdentifierGenerator {
	
	private AtomicInteger idCounter = new AtomicInteger(0);
	
	@Override
	public Serializable generate() {
		return idCounter.addAndGet(1);
	}
	
	public void reset() {
		idCounter.set(0);
	}
	
	@Override
	public void configure(Map<String, Object> configuration) {
		
	}
}
