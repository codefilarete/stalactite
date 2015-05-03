package org.gama.lang.trace;

import java.util.AbstractMap.SimpleEntry;
import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.gama.lang.StringAppender;

/**
 * Classe pour mesurer l'écoulement du temps entre 2 appels de méthodes.
 *
 * @author Guillaume Mary
 */
public class Chrono {

	private static final int MILLIS_MAX = 1000;
	private static final int SEC_MAX = 60*MILLIS_MAX;
	private static final int MIN_MAX = 60*SEC_MAX;
	private static final int H_MAX = 24*MIN_MAX;

	private enum TimeConstant {
		DAY(H_MAX, "j"),
		HOUR(MIN_MAX, ":"),
		MINUTE(SEC_MAX, "min"),
		SECOND(MILLIS_MAX, "s"),
		MILLIS(1, "ms");
		
		private int millisCount;
		private String timeUnit;
		
		TimeConstant(int millisCount, String timeUnit) {
			this.millisCount = millisCount;
			this.timeUnit = timeUnit;
		}
		
		private static final EnumSet<TimeConstant> MILLIS_TIME = EnumSet.of(MILLIS);
		private static final EnumSet<TimeConstant> SECOND_TIME = EnumSet.of(SECOND, MILLIS);
		private static final EnumSet<TimeConstant> MINUTE_TIME = EnumSet.of(MINUTE, SECOND, MILLIS);
		private static final EnumSet<TimeConstant> HOUR_TIME = EnumSet.of(HOUR, MINUTE, SECOND, MILLIS);
		private static final EnumSet<TimeConstant> DAY_TIME = EnumSet.of(DAY, HOUR, MINUTE, SECOND, MILLIS);
		
		/**
		 * Donne les constantes (dans l'ordre) à utiliser pour formater des millisecondes.
		 * Utilisé comme optimisation car si on les parcourt toutes systématiquement les perf de formatage sont moins bonnes
		 * dans la plupart des cas vu qu'on mesure très souvent des secondes ou millisecondes
		 * @param millis
		 * @return
		 */
		private static Set<TimeConstant> getTimeConstantsToUse(long millis) {
			Set<TimeConstant> constantsToUse;
			if (millis < MILLIS_MAX) {
				constantsToUse = TimeConstant.MILLIS_TIME;
			} else if (millis < SEC_MAX) {
				constantsToUse = TimeConstant.SECOND_TIME;
			} else if (millis < MIN_MAX) {
				constantsToUse = TimeConstant.MINUTE_TIME;
			} else if (millis < H_MAX) {
				constantsToUse = TimeConstant.HOUR_TIME;
			} else {
				constantsToUse = TimeConstant.DAY_TIME;
			}
			return constantsToUse;
		}
	
	}
	
	/**
	 * Formatte un instant sous la forme "1:02min 24s 103ms" (chaque valeur est optionnelle si elle n'a pas de valeur)
	 * 
	 * @param millis
	 * @return le temps affichable
	 */
	public static String format(long millis) {
		StringAppender sb = new StringAppender();
		Set<TimeConstant> constantsToUse = TimeConstant.getTimeConstantsToUse(millis);
		for (TimeConstant timeConstant : constantsToUse) {
			Entry<Long, Long> divide = divide(millis, timeConstant.millisCount);
			long quotient = divide.getKey();
			// on affiche que ce qui est nécessaire
			sb.catIf(quotient != 0, Long.toString(quotient), timeConstant.timeUnit, " ");
			millis = divide.getValue();
		}
		sb.cutTail(1);
		return sb.toString();
	}
	
	private static Map.Entry<Long, Long> divide(long millis, int divisor) {
		return new SimpleEntry<>(millis / divisor, millis%divisor);
	}
	
	/**
	 * Renvoie l'instant présent
	 * 
	 * @return le temps en ms
	 */
	public static long now() {
		return System.currentTimeMillis();
	}

	private long startTime;
	
	/** Creates new Chrono */
	public Chrono () {
		start();
	}
	
	/**
	 * Renvoie l'instant de départ
	 * @return le temps en milliseconde
	 */
	public long getStartTime() {
		return this.startTime;
	}
	
	/**
	 * Démarre le chronomètre
	 * @return l'instant de démarrage
	 */
	public long start() {
		return startTime = now();
	}
	
	/**
	 * Donne le temps écoulé depuis le démarrage du chronomètre
	 * @return la différence de millisecondes entre maintenant et l'instant de démarrage
	 */
	public long getElapsedTime() {
		return now() - startTime;
	}
	
	/**
	 * Formatte le temps écoulé depuis l'instant de départ
	 * @see Chrono#format
	 * @see Chrono#getElapsedTime
	 */
	@Override
	public String toString() {
		return format(getElapsedTime());
	}
}
