package org.gama.lang.bean;

/**
* @author mary
*/
public interface IConverter<I, O> {
	
	O convert(I input);
	
	abstract class NullAwareConverter<I, O> implements IConverter<I, O> {
		
		@Override
		public O convert(I input) {
			return input == null ? null : convertNotNull(input);
		}
		
		protected abstract O convertNotNull(I input);
	}
}
