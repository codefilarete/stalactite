package org.stalactite.lang;

import java.io.Serializable;

/**
 * @author mary
 */
public class StringAppender implements Serializable {
	
	private StringBuilder appender;
	
	public StringAppender() {
		appender = new StringBuilder();
	}

	public StringAppender(int capacity) {
		appender = new StringBuilder(capacity);
	}
	
	public StringAppender(String ... s) {
		this();
		cat(s);
	}

	public StringAppender cat(String s) {
		appender.append(s);
		return this;
	}
	
	public StringAppender cat(String ... ss) {
		for (String s : ss) {
			cat(s);
		}
		return this;
	}
	
	public StringAppender catIf(boolean condition, String ... ss) {
		if (condition) {
			cat(ss);
		}
		return this;
	}
	
	public StringAppender ccat(Object ... s) {
		Object[] dest = new Object[s.length - 1];
		System.arraycopy(s, 0, dest, 0, s.length - 1);
		return ccat(dest, (String) s[s.length-1]);
	}
	
	public StringAppender ccat(Object[] s, String sep) {
		if (s.length>0) {
			for (Object arg : s) {
				appender.append(arg).append(sep);
			}
			cutTail(sep.length());
		}
		return this;
	}
	
	public StringAppender wrap(String open, String close) {
		appender.insert(0, open).append(close);
		return this;
	}

	@Override
	public String toString() {
		return appender.toString();
	}

	public int getLength() {
		return appender.length();
	}

	public StringAppender cutTail(int nbChar) {
		appender.setLength(getLength()-nbChar);
		return this;
	}
	
	public StringAppender cutHead(int nbChar) {
		appender.delete(0, nbChar);
		return this;
	}
}
