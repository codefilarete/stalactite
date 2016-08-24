package org.gama.lang.bean;

/**
* @author Guillaume Mary
*/
@FunctionalInterface
public interface IConverter<I, O> {
	
	O convert(I input);
	
	abstract class NullAwareConverter<I, O> implements IConverter<I, O> {
		
		@Override
		public O convert(I input) {
			return input == null ? convertNull() : convertNotNull(input);
		}
		
		/**
		 * Called for returning a value when input is null.
		 * This implementation returns null
		 * 
		 * @return whatever needed
		 */
		protected O convertNull() {
			return null;
		}
		
		protected abstract O convertNotNull(I input);
	}
}
