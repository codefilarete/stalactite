package org.gama.lang;

import org.gama.lang.bean.IDelegateWithReturnAndThrows;
import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Guillaume Mary
 */
public class RetryerTest {
	
	@Test
	public void testExecute_neverWorks() throws Throwable {
		Retryer testInstance = new Retryer(3, 5) {
			@Override
			protected boolean shouldRetry(Throwable t) {
				return true;
			}
		};
		
		final int[] callTimes = new int[1];
		try {
			testInstance.execute(new IDelegateWithReturnAndThrows<Object>() {
				@Override
				public Object execute() throws Throwable {
					callTimes[0]++;
					throw new RuntimeException("Never works !");
				}
			}, "test");
		} catch (Throwable t) {
			assertEquals("Action \"test\" has been executed 3 times every 5ms and always failed", t.getMessage());
			assertEquals("Never works !", t.getCause().getMessage());
		}
		assertEquals(3, callTimes[0]);
	}
	
	@Test
	public void testExecute_worksLastAttempt() throws Throwable {
		Retryer testInstance = new Retryer(3, 5) {
			@Override
			protected boolean shouldRetry(Throwable t) {
				return true;
			}
		};
		
		final int[] callTimes = new int[1];
		try {
			testInstance.execute(new IDelegateWithReturnAndThrows<Object>() {
				@Override
				public Object execute() throws Throwable {
					callTimes[0]++;
					if (callTimes[0] < 2) {
						throw new RuntimeException("Never works !");
					}
					return null;
				}
			}, "test");
		} catch (Throwable t) {
			fail("No exception should be thrown");
		}
		assertEquals(2, callTimes[0]);
	}
	
	@Test
	public void testExecute_worksFirstAttempt() throws Throwable {
		Retryer testInstance = new Retryer(3, 5) {
			@Override
			protected boolean shouldRetry(Throwable t) {
				return true;
			}
		};
		
		final int[] callTimes = new int[1];
		try {
			testInstance.execute(new IDelegateWithReturnAndThrows<Object>() {
				@Override
				public Object execute() throws Throwable {
					callTimes[0]++;
					return null;
				}
			}, "test");
		} catch (Throwable t) {
			fail("No exception should be thrown");
		}
		assertEquals(1, callTimes[0]);
	}
	
	@Test
	public void testExecute_throwUnexpected() throws Throwable {
		Retryer testInstance = new Retryer(3, 5) {
			@Override
			protected boolean shouldRetry(Throwable t) {
				return t.getMessage().equals("retry");
			}
		};
		
		final int[] callTimes = new int[1];
		try {
			testInstance.execute(new IDelegateWithReturnAndThrows<Object>() {
				@Override
				public Object execute() throws Throwable {
					callTimes[0]++;
					if (callTimes[0] < 3) {
						throw new RuntimeException("retry");
					} else {
						throw new RuntimeException("Unepected error");
					}
				}
			}, "test");
		} catch (Throwable t) {
			assertEquals("Unepected error", t.getMessage());
		}
		assertEquals(3, callTimes[0]);
	}
	
	@Test
	public void testExecute_noRetryAsked() throws Throwable {
		Retryer testInstance = Retryer.NO_RETRY;
		
		final int[] callTimes = new int[1];
		String result = null;
		try {
			result = (String) testInstance.execute(new IDelegateWithReturnAndThrows<Object>() {
				@Override
				public Object execute() throws Throwable {
					callTimes[0]++;
					return "OK";
				}
			}, "test");
		} catch (Throwable t) {
			fail("No exception should be thrown");
		}
		assertEquals(1, callTimes[0]);
		assertEquals("OK", result);
	}
}