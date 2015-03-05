package com.logicartisan.io.log;

import java.io.Serializable;


/**
 *
 */
public class SearchMatch implements Serializable {
	private final int row;
	private final int start_offset;
	private final int length;


	public SearchMatch( int row, int start_offset, int length ) {
		this.row = row;
		this.start_offset = start_offset;
		this.length = length;
	}


	/**
	 * Index of the matching row.
	 */
	public int getRow() {
		return row;
	}


	/**
	 * Position of a start of the match.
	 */
	public int getStartOffset() {
		return start_offset;
	}

	/**
	 * Length of the match.
	 */
	public int getLength() {
		return length;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append( "SearchMatch" );
		sb.append( "{row=" ).append( row );
		sb.append( ", start_offset=" ).append( start_offset );
		sb.append( ", length=" ).append( length );
		sb.append( '}' );
		return sb.toString();
	}

	@Override
	public boolean equals( Object o ) {
		if ( this == o ) return true;
		if ( o == null || getClass() != o.getClass() ) return false;

		SearchMatch that = ( SearchMatch ) o;

		if ( length != that.length ) return false;
		if ( row != that.row ) return false;
		if ( start_offset != that.start_offset ) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = row;
		result = 31 * result + start_offset;
		result = 31 * result + length;
		return result;
	}
}
