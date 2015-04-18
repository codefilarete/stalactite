package org.stalactite.lang;

/**
 * @author guillaume.mary
 */
public abstract class Strings {
	
	public static boolean isEmpty(CharSequence charSequence) {
		return charSequence == null || charSequence.length() == 0;
	}
	
	public static String capitalize(final CharSequence cs) {
		return (String) doWithDelegate(cs, new DefaultNullOrEmptyDelegate() {
			@Override
			public CharSequence onNotNullNotEmpty() {
				return Character.toUpperCase(cs.charAt(0)) + tail(cs, cs.length() - 1).toString();
			}
		});
	}
	
	public static CharSequence head(final CharSequence cs, final int headSize) {
		return doWithDelegate(cs, new DefaultNullOrEmptyDelegate() {
			@Override
			public CharSequence onNotNullNotEmpty() {
				return cs.subSequence(0, Math.min(cs.length(), preventNegative(headSize)));
			}
		});
	}
	
	public static CharSequence tail(final CharSequence cs, final int tailSize) {
		return doWithDelegate(cs, new DefaultNullOrEmptyDelegate() {
			@Override
			public CharSequence onNotNullNotEmpty() {
				return cs.subSequence(Math.max(0, cs.length() - preventNegative(tailSize)), cs.length());
			}
		});
	}
	
	private static int preventNegative(int i) {
		return i < 0 ? 0 : i;
	}
	
	private static CharSequence doWithDelegate(CharSequence cs, INullOrEmptyDelegate emptyDelegate) {
		if (cs == null) {
			return emptyDelegate.onNull();
		} else if (cs.length() == 0) {
			return emptyDelegate.onEmpty();
		} else {
			return emptyDelegate.onNotNullNotEmpty();
		}
	}
	
	private interface INullOrEmptyDelegate {
		CharSequence onNull();
		CharSequence onEmpty();
		CharSequence onNotNullNotEmpty();
	}
	
	private static abstract class DefaultNullOrEmptyDelegate implements INullOrEmptyDelegate {
		@Override
		public CharSequence onNull() {
			return null;
		}
		
		@Override
		public CharSequence onEmpty() {
			return "";
		}
	}
	
	private Strings() {}
}
