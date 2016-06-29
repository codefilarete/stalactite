package org.gama.lang;

/**
 * @author Guillaume Mary
 */
public abstract class Strings {
	
	public static boolean isEmpty(CharSequence charSequence) {
		return charSequence == null || charSequence.length() == 0;
	}
	
	public static String capitalize(final CharSequence cs) {
		return (String) doWithDelegate(cs, new DefaultNullOrEmptyDelegate() {
			@Override
			public CharSequence onNotNullNotEmpty() {
				return Character.toUpperCase(cs.charAt(0)) + cs.subSequence(1, cs.length()).toString();
			}
		});
	}
	
	public static String uncapitalize(final CharSequence cs) {
		return (String) doWithDelegate(cs, new DefaultNullOrEmptyDelegate() {
			@Override
			public CharSequence onNotNullNotEmpty() {
				return Character.toLowerCase(cs.charAt(0)) + cs.subSequence(1, cs.length()).toString();
			}
		});
	}
	
	/**
	 * Concatenate count (positive) times parameter s.
	 * Optional Strings in prebuildStrings are used to speed concatenation for large count numbers if you already have
	 * large snippets of s pre-concatenated. For instance, you want 3456 times "a" and you already got constants with
	 * a*500, a*100, a*10, then this method will only cat 6*a*500, 4*a*100, 5*a*10 and 6*a. Instead of 3456 times "a".
	 *
	 * @param count expected repeatition of s
	 * @param s the String to be concatenated
	 * @param prebuiltStrings optional pre-concatenated "s" strings, <b>in descent size order</b>.
	 * @return s repeated count times
	 */
	public static StringBuilder repeat(int count, CharSequence s, String... prebuiltStrings) {
		StringBuilder result = new StringBuilder(count * s.length());
		repeat(result, count, s, prebuiltStrings);
		return result;
	}
	
	/**
	 * Concatenate count (positive) times parameter s.
	 * Optional Strings in prebuildStrings are used to speed concatenation for large count numbers if you already have
	 * large snippets of s pre-concatenated. For instance, you want 3456 times "a" and you already got constants with
	 * a*500, a*100, a*10, then this method will only cat 6*a*500, 4*a*100, 5*a*10 and 6*a. Instead of 3456 times "a".
	 *
	 * @param result destination of the concatenation
	 * @param count expected repeatition of s
	 * @param s the String to be concatenated
	 * @param prebuiltStrings optional pre-concatenated "s" strings, <b>in descent size order</b>.
	 * @return s repeated count times
	 */
	public static StringBuilder repeat(StringBuilder result, int count, CharSequence s, String... prebuiltStrings) {
		int snippetCount, remainer = count;
		for (String snippet : prebuiltStrings) {
			int snippetLength = snippet.length();
			snippetCount = remainer / snippetLength;
			for (int i = 0; i < snippetCount; i++) {
				result.append(snippet);
			}
			remainer = remainer % snippetLength;
		}
		for (int i = 0; i < remainer; i++) {
			result.append(s);
		}
		return result;
	}
		
	public static CharSequence head(CharSequence cs, int headSize) {
		return doWithDelegate(cs, new DefaultNullOrEmptyDelegate() {
			@Override
			public CharSequence onNotNullNotEmpty() {
				return cs.subSequence(0, Math.min(cs.length(), preventNegative(headSize)));
			}
		});
	}
	
	public static CharSequence cutHead(CharSequence cs, int headSize) {
		return doWithDelegate(cs, new DefaultNullOrEmptyDelegate() {
			@Override
			public CharSequence onNotNullNotEmpty() {
				return cs.subSequence(Math.min(preventNegative(headSize), cs.length()), cs.length());
			}
		});
	}
	
	public static CharSequence tail(CharSequence cs, int tailSize) {
		return doWithDelegate(cs, new DefaultNullOrEmptyDelegate() {
			@Override
			public CharSequence onNotNullNotEmpty() {
				return cs.subSequence(Math.max(0, cs.length() - preventNegative(tailSize)), cs.length());
			}
		});
	}
	
	public static CharSequence cutTail(CharSequence cs, int tailSize) {
		return doWithDelegate(cs, new DefaultNullOrEmptyDelegate() {
			@Override
			public CharSequence onNotNullNotEmpty() {
				return cs.subSequence(0, preventNegative(cs.length() - preventNegative(tailSize)));
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
