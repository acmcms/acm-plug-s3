/*
 * Created on 14.01.2005
 */
package ru.myx.xstore.s3.concept;

import java.util.HashMap;
import java.util.Map;

import ru.myx.ae1.storage.ModuleInterface;
import java.util.function.Function;
import ru.myx.ae3.help.Convert;
import ru.myx.query.OneCondition;
import ru.myx.query.OneConditionSimple;

final class ReplacerConditionState implements Function<OneCondition, OneCondition> {
	
	private static final Map<String, String> REPLACEMENTS = ReplacerConditionState.createReplacements();

	private static final Map<String, String> createReplacements() {
		
		final Map<String, String> result = new HashMap<>();
		result.put("publish", String.valueOf(ModuleInterface.STATE_PUBLISH));
		result.put("published", String.valueOf(ModuleInterface.STATE_PUBLISHED));
		result.put("archive", String.valueOf(ModuleInterface.STATE_ARCHIVE));
		result.put("archieve", String.valueOf(ModuleInterface.STATE_ARCHIEVE));
		result.put("archived", String.valueOf(ModuleInterface.STATE_ARCHIVED));
		result.put("archieved", String.valueOf(ModuleInterface.STATE_ARCHIEVED));
		result.put("dead", String.valueOf(ModuleInterface.STATE_DEAD));
		result.put("draft", String.valueOf(ModuleInterface.STATE_DRAFT));
		result.put("system", String.valueOf(ModuleInterface.STATE_SYSTEM));
		result.put("ready", String.valueOf(ModuleInterface.STATE_READY));
		return result;
	}

	@Override
	public OneCondition apply(final OneCondition condition) {
		
		final String value = condition.getValue();
		final String state = Convert.MapEntry.toString(ReplacerConditionState.REPLACEMENTS, value, value);
		if (state == value) {
			return condition;
		}
		return new OneConditionSimple(condition.isExact(), condition.getField(), condition.getOperator(), state);

	}
}
