package com.github.snowindy.sql;

public abstract class ParamsExtractor {
	protected boolean applied;

	abstract Object toSimpleType(Object input);

	public boolean wasApplied() {
		return applied;
	}
}
