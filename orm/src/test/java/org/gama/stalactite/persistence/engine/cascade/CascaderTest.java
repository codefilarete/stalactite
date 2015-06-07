package org.gama.stalactite.persistence.engine.cascade;

/**
 * @author Guillaume Mary
 */
public class CascaderTest {
	
	protected static class Toto {
		
		protected Tata tata;
		
		protected Toto() {
		}
		
		protected Toto(Tata tata) {
			this.tata = tata;
		}
	}
	
	protected static class Tata {
		
		protected Long id;
		
		protected Tata() {
		}
	}
}