/*
 * Copyright (c) 2013 Rob Eden.
 * All Rights Reserved.
 */

package com.logicartisan.io.log;

import com.starlight.ValidationKit;

import java.io.Serializable;
import java.util.regex.Pattern;


/**
 *
 */
public class SearchParams {
	/**
	 * Creates parameters for a simple token matching search.
	 *
	 * @param token             The token to search for.
	 * @param case_sensitive    Whether or not the search should be case sensitive.
	 */
	public static SearchParams createSimple( String token, boolean case_sensitive ) {
		ValidationKit.checkNonnull( token, "token" );

		return new SimpleSearchParams( token, case_sensitive );
	}


	/**
	 * Creates parameters for a regex search.
	 *
	 * @param pattern           The regex pattern to search for.
	 */
	public static SearchParams createRegex( Pattern pattern ) {
		ValidationKit.checkNonnull( pattern, "pattern" );

		return new RegexSearchParams( pattern );
	}


	protected SearchParams() {}


	static class RegexSearchParams extends SearchParams implements Serializable {
		private final Pattern pattern;

		private RegexSearchParams( Pattern pattern ) {
			this.pattern = pattern;
		}

		public Pattern getPattern() {
			return pattern;
		}
	}

	static class SimpleSearchParams extends SearchParams implements Serializable {
		private final String token;
		private final boolean case_sensitive;

		SimpleSearchParams( String token, boolean case_sensitive ) {
			this.token = token;
			this.case_sensitive = case_sensitive;
		}

		public boolean isCaseSensitive() {
			return case_sensitive;
		}

		public String getToken() {
			return token;
		}
	}
}
