package org.gama.stalactite.persistence.id.provider;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.gama.lang.collection.Arrays;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Guillaume Mary
 */
public class PooledIdentifierProviderTest {
	
	@Test
	public void testAsynchronousLoad() throws InterruptedException {
		List<Long> starterKit = Arrays.asList(1L, 2L, 3L);
		List<Long> generated = new ArrayList<>(500);
		generated.addAll(starterKit);
		ExecutorService backgroundLoader = Executors.newSingleThreadExecutor();
		PooledIdentifierProvider<Long> testInstance = new PooledIdentifierProvider<Long>(starterKit, 2, backgroundLoader, Duration.ofSeconds(2)) {
			
			// let's start at the end of starter kit so will have a continuous serie of long
			private long seed = 4;
			
			@Override
			protected Collection<Long> retrieveSomeValues() {
				List<Long> result = new ArrayList<>();
				for (long i = 0; i < 10; i++) {
					result.add(seed++);
				}
				generated.addAll(result);
				return result;
			}
		};
		
		List<Long> result = new ArrayList<>();
		ExecutorService executorService = Executors.newFixedThreadPool(2);
		executorService.submit(new QueueConsumer(testInstance, result));
		executorService.submit(new QueueConsumer(testInstance, result));
		executorService.awaitTermination(10, TimeUnit.SECONDS);
		backgroundLoader.awaitTermination(2, TimeUnit.SECONDS);
		// Sort to prevent from Thread precedence making pop() not done in ascending order
		result.sort(Long::compareTo);
		// assertion for forbidden duplicates
		assertEquals(result, new ArrayList<>(new HashSet<>(result)));
		// assertion for unexpected identifier generation
		assertTrue(generated.containsAll(result));
	}
	
	private class QueueConsumer implements Runnable {
		
		private final List<Long> bagToFill;
		private final PooledIdentifierProvider<Long> testInstance;
		
		private QueueConsumer(PooledIdentifierProvider<Long> testInstance, List<Long> bagToFill) {
			this.bagToFill = bagToFill;
			this.testInstance = testInstance;
		}
		
		@Override
		public void run() {
			// poorly implemented thread consumption at fixed rate
			while (true) {
				try {
					PersistableIdentifier<Long> data = testInstance.giveNewIdentifier();
					bagToFill.add(data.getSurrogate());
					Thread.sleep(50);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}
}