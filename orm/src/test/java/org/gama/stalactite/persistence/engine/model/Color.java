package org.codefilarete.stalactite.persistence.engine.model;

import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.codefilarete.stalactite.persistence.engine.FluentEntityMappingConfigurationSupportInheritanceTest;

/**
 * @author Guillaume Mary
 */
public class Color {
	
	private int rgb;
	
	public Color() {
	}
	
	public Color(int rgb) {
		this.rgb = rgb;
	}
	
	public int getRgb() {
		return rgb;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Color color = (Color) o;
		return rgb == color.rgb;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(rgb);
	}
	
	/**
	 * Implemented for easier debug
	 *
	 * @return a simple representation of this
	 */
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
