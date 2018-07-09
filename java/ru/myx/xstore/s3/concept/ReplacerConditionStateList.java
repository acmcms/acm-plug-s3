/*
 * Created on 14.01.2005
 */
package ru.myx.xstore.s3.concept;

import java.util.function.Function;
import ru.myx.ae3.help.Convert;
import ru.myx.query.OneCondition;
import ru.myx.query.OneConditionSimple;

final class ReplacerConditionStateList implements Function<OneCondition, OneCondition> {
	
	private final String stateList;

	ReplacerConditionStateList(final String stateList) {
		this.stateList = stateList;
	}

	@Override
	public OneCondition apply(final OneCondition condition) {
		
		return new OneConditionSimple(condition.isExact(), "o.objState", Convert.Any.toInt(condition.getValue(), 0) == 0
			? " NOT IN "
			: " IN ", this.stateList);
	}
}
