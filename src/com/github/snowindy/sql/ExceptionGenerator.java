package com.github.snowindy.sql;

interface ExceptionGenerator {
	RuntimeException wrap(Exception e);
}
