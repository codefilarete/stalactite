package org.gama.sql;

import java.sql.Savepoint;

import org.gama.lang.trace.IncrementableInt;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
public class TransactionListenerCollectionTest {
	
	@Test
	public void testAfterCommit_temporaryListenersShouldNotBeCalledTwice() {
		TransactionListenerCollection testInstance = new TransactionListenerCollection();
		MutableBoolean isTemporary = new MutableBoolean();
		IncrementableInt incrementableInt = new IncrementableInt();
		CommitListener temporaryCommitListener = new CommitListener() {
			@Override
			public void beforeCommit() {
			}
			
			@Override
			public void afterCommit() {
				incrementableInt.increment();
			}
			
			@Override
			public boolean isTemporary() {
				return isTemporary.booleanValue();
			}
		};
		testInstance.addCommitListener(temporaryCommitListener);
		
		// With a non-temporary listener, our counter will be incremented as we call afterCommit()
		isTemporary.setFalse();
		testInstance.afterCommit();
		assertEquals(1, incrementableInt.getValue());
		// A second call to afterCommit() will still increment our counter
		testInstance.afterCommit();
		assertEquals(2, incrementableInt.getValue());
		
		// But if the listener is set as temporary then only next call to afterCommit() will be effective
		isTemporary.setTrue();
		testInstance.afterCommit();
		assertEquals(3, incrementableInt.getValue());
		// no more increment
		testInstance.afterCommit();
		assertEquals(3, incrementableInt.getValue());
	}
	
	@Test
	public void testAfterRolback_temporaryListenersShouldNotBeCalledTwice() {
		TransactionListenerCollection testInstance = new TransactionListenerCollection();
		MutableBoolean isTemporary = new MutableBoolean();
		IncrementableInt incrementableInt = new IncrementableInt();
		RollbackListener temporaryRollbackListener = new RollbackListener() {
			
			@Override
			public void beforeRollback() {
				
			}
			
			@Override
			public void afterRollback() {
				incrementableInt.increment();
			}
			
			@Override
			public void beforeRollback(Savepoint savepoint) {
				
			}
			
			@Override
			public void afterRollback(Savepoint savepoint) {
				
			}
			
			@Override
			public boolean isTemporary() {
				return isTemporary.booleanValue();
			}
		};
		testInstance.addRollbackListener(temporaryRollbackListener);
		
		// With a non-temporary listener, our counter will be incremented as we call afterRollback()
		isTemporary.setFalse();
		testInstance.afterRollback();
		assertEquals(1, incrementableInt.getValue());
		// A second call to afterRollback() will still increment our counter
		testInstance.afterRollback();
		assertEquals(2, incrementableInt.getValue());
		
		// But if the listener is set as temporary then only next call to afterRollback() will be effective
		isTemporary.setTrue();
		testInstance.afterRollback();
		assertEquals(3, incrementableInt.getValue());
		// no more increment
		testInstance.afterRollback();
		assertEquals(3, incrementableInt.getValue());
	}
	
	private static class MutableBoolean {
		
		private boolean value;
		
		public boolean booleanValue() {
			return value;
		}
		
		public void setTrue() {
			this.value = true;
		}
		
		public void setFalse() {
			this.value = false;
		}
	}
}