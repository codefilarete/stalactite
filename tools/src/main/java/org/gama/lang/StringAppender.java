package org.gama.lang;

import java.io.Serializable;

/**
 * Sorte de StringBuilder en plus pratique ... ou pas !
 *
 * @author mary
 */
public class StringAppender implements Serializable, CharSequence {
	
	private final StringBuilder appender;
	
	public StringAppender() {
		appender = new StringBuilder();
	}

	public StringAppender(int capacity) {
		appender = new StringBuilder(capacity);
	}
	
	public StringAppender(CharSequence ... s) {
		this();
		cat(s);
	}

	public StringAppender cat(CharSequence s) {
		appender.append(s);
		return this;
	}
	
	public StringAppender cat(CharSequence s1, CharSequence s2) {
		return cat(s1).cat(s2);
	}
	
	public StringAppender cat(CharSequence s1, CharSequence s2, CharSequence s3) {
		return cat(s1,s2).cat(s3);
	}
	
	public StringAppender cat(CharSequence ... ss) {
		for (CharSequence s : ss) {
			cat(s);
		}
		return this;
	}
	
	public StringAppender cat(int index, CharSequence ... ss) {
		for (CharSequence s : ss) {
			appender.insert(index, s);
			index+=s.length();
		}
		return this;
	}
	
	public StringAppender catIf(boolean condition, CharSequence ... ss) {
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
				appender.append(arg).append(sep);
			}
			cutTail(sep.length());
		}
		return this;
	}
	
	public StringAppender wrap(CharSequence open, CharSequence close) {
		appender.insert(0, open).append(close);
		return this;
	}
	
	@Override
	public String toString() {
		return appender.toString();
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
