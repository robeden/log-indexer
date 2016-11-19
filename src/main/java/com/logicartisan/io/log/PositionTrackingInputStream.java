package com.logicartisan.io.log;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;


/**
 * An InputStream that tracks the current position in the file.
 */
class PositionTrackingInputStream extends FilterInputStream {
	private long position = 0;

	private long mark_position = 0;
	
	
	PositionTrackingInputStream( InputStream stream ) {
		super( stream );
	}


	/**
	 * Return the current position in the file.
	 */
	public long position() {
		return position;
	}


	@Override
	public int read() throws IOException {
		int result = super.read();
		
		if ( result != -1 ) position++;
		
		return result;
	}

	@Override
	public int read( byte[] bytes ) throws IOException {
		return read( bytes, 0, bytes.length );
	}

	@Override
	public int read( byte[] bytes, int offset, int length ) throws IOException {
		int result = super.read( bytes, offset, length );
		
		if ( result > 0 ) position += result;
		
		return result;
	}

	@Override
	public long skip( long l ) throws IOException {
		long actual = super.skip( l );
		
		if ( actual > 0 ) position += actual;
		
		return actual;
	}


	@Override
	public void mark( int read_limit ) {
		super.mark( read_limit );
		
		mark_position = position;
	}

	@Override
	public void reset() throws IOException {
		// Reset first, so we know if it works
		super.reset();
		
		position = mark_position;
	}
}
