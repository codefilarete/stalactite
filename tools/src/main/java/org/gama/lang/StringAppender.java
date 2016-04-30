package org.gama.lang;

import java.io.Serializable;

/**
 * Kind of StringBuilder aimed at being simpler by its API.
 *
 * @author Guillaume Mary
 */
public class StringAppender implements Serializable, CharSequence {
	
	private final StringBuilder appender;
	
	public StringAppender() {
		appender = new StringBuilder();
	}

	public StringAppender(int capacity) {
		appender = new StringBuilder(capacity);
	}
	
	public StringAppender(Object ... s) {
		this();
		cat(s);
	}

	public StringAppender cat(Object s) {
		appender.append(toString(s));
		return this;
	}
	
	public StringAppender cat(Object s1, Object s2) {
		return cat(s1).cat(s2);
	}
	
	public StringAppender cat(Object s1, Object s2, Object s3) {
		return cat(s1,s2).cat(s3);
	}
	
	public StringAppender cat(Object ... ss) {
		for (Object s : ss) {
			cat(toString(s));
		}
		return this;
	}
	
	public StringAppender catAt(int index, Object ... ss) {
		for (Object s : ss) {
			CharSequence cs = toString(s);
			appender.insert(index, cs);
			index+=cs.length();
		}
		return this;
	}
	
	public StringAppender catIf(boolean condition, Object ... ss) {
		if (condition) {
			cat(ss);
		}
		return this;
	}
	
	public StringAppender ccat(Object ... s) {
		Object[] dest = new Object[s.length - 1];
		System.arraycopy(s, 0, dest, 0, s.length - 1);
		return ccat(dest, (CharSequence) s[s.length-1]);
	}
	
	public StringAppender ccat(Object[] s, CharSequence sep) {
		if (s.length>0) {
			for (Object arg : s) {
				appender.append(toString(arg)).append(sep);
			}
			cutTail(sep.length());
		}
		return this;
	}
	
	public StringAppender wrap(Object open, Object close) {
		catAt(0, open).cat(close);
		return this;
	}
	
	@Override
	public String toString() {
		return appender.toString();
	}
	
	protected CharSequence toString(Object o) {
		// Concatenated objects are not supposed to be null, so a toString() on it is sufficient and skip a "if null" check
		return o.toString();
	}

	public StringAppender cutTail(int nbChar) {
		int newLength = length() - nbChar;
		if (newLength > -1) {
			appender.setLength(newLength);
		}
		return this;
	}
	
	public StringAppender cutHead(int nbChar) {
		appender.delete(0, nbChar);
		return this;
	}
	
	/**
	 * Gives access to delegate appender, because it can be usefull to append directly to the standat API StringBuilder
	 * @return the underlying appender
	 */
	public StringBuilder getAppender() {
		return appender;
	}
	
	@Override
	public int length() {
		return appender.length();
	}
	
	@Override
	public char charAt(int index) {
		return appender.charAt(index);
	}
	
	@Override
	public CharSequence subSequence(int start, int end) {
		return appender.subSequence(start, end);
	}
	
}
