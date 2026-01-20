package com.logicartisan.io.log;

import com.logicartisan.common.core.IOKit;
import com.logicartisan.common.core.listeners.ErrorCountDeliveryErrorHandler;
import com.logicartisan.common.core.listeners.ListenerSupport;
import com.logicartisan.common.core.thread.SharedThreadPool;
import gnu.trove.map.TIntLongMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TObjectIntProcedure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Watches and indexes text files (typically log files) for notifications of changes
 * and tracking of line position. The benefit to callers is that it allows fast
 * retrieval of lines and notification of how many lines are available.
 */
public class LogIndexer<A> implements LogAccess<A> {
	private static final Logger LOG = LoggerFactory.getLogger( LogIndexer.class );

	private static final AtomicInteger SEARCH_ID_COUNTER = new AtomicInteger( 0 );

	private static final boolean DEBUG_ROW_INDEX_MAP = false;

	private final File file;
	private final A attachment;
	private final ListenerSupport<LogIndexListener,?> listeners;
	private final int max_index_size;
	private final int max_search_hits;

	
	private final TIntLongMap row_index_map = new TIntLongHashMap();
	private final Lock row_index_map_lock = new ReentrantLock();

	private volatile int row_skip_mod = 0;
	
	
	// The number of rows found in the file
	private volatile int num_lines = 0;

	private volatile long last_file_length = 0;
	private volatile long last_last_modified = 0;

	private volatile boolean full_index_after_modified = false;
	private volatile boolean pending_same_length_modified = false;
	private volatile long pending_modified_value = 0;


	private final AtomicBoolean indexer_scheduled;

	private final ScheduledFuture file_change_future;

	private final Lock search_lock = new ReentrantLock();
	private final TIntObjectMap<Searcher> searchers = new TIntObjectHashMap<>();


	/**
	 * Create the indexer and do an initial indexing of the file.
	 *
	 * @param file              The file to index.
	 * @param listener          If non-null, this listener will be notified when indexes
	 *                          are running.
	 * @param max_index_size    The maximum size of the row index. The larger this number
	 *                          the more memory will be used with large files. If the
	 *                          size if smaller than the number of rows in the file, only
	 *                          a certain portion of the row indexes will be kept which
	 *                          will require more seeking when looking for a particular
	 *                          line.
	 * @param max_search_hits   Maximum number of matches allowed for a search.
	 * @param full_index_after_modified   When true, full indexing of the file will occur
	 *                                    whenever the last modified time is updated. When
	 *                                    false, full indexing occurs when the file shrinks,
	 *                                    and partial indexing from the last indexed
	 *                                    position occurs when the file grows.
	 * @param all_listeners_removed     Runnable that is called when/if the last listener
	 *                          if removed due to a message delivery problem.
	 */
	public LogIndexer( @Nonnull File file, @Nullable A attachment,
					   @Nullable LogIndexListener<A> listener,
					   int max_index_size, int max_search_hits, boolean full_index_after_modified,
					   @Nullable final Runnable all_listeners_removed ) {

		Objects.requireNonNull( file );

		this.file = file;
		this.attachment = attachment;
		this.listeners = ListenerSupport.forType( LogIndexListener.class )
				.errorHandler(
						new ErrorCountDeliveryErrorHandler<LogIndexListener>( 3, true ) {
							@Override
							public void lastListenerRemoved() {
								if ( all_listeners_removed != null ) all_listeners_removed.run();
							}
						} )
				.asynchronous()
				.build();
		this.max_index_size = max_index_size;
		this.max_search_hits = max_search_hits;
		this.full_index_after_modified = full_index_after_modified;

		if ( listener != null ) listeners.add( listener );

		// Start an index right away
		indexer_scheduled = new AtomicBoolean( true );
		SharedThreadPool.INSTANCE.execute( new Indexer( 0, 0 ) );

		// String checking for changes after a couple seconds
		file_change_future = SharedThreadPool.INSTANCE.scheduleAtFixedRate(
				new FileChangeChecker(), 2, 1, TimeUnit.SECONDS );
	}

	/**
	 * Create the indexer and do an initial indexing of the file.
	 * 
	 * @param file              The file to index.
	 * @param listener          If non-null, this listener will be notified when indexes
	 *                          are running.
	 * @param max_index_size    The maximum size of the row index. The larger this number
	 *                          the more memory will be used with large files. If the
	 *                          size if smaller than the number of rows in the file, only
	 *                          a certain portion of the row indexes will be kept which
	 *                          will require more seeking when looking for a particular
	 *                          line.
	 * @param max_search_hits   Maximum number of matches allowed for a search.
	 * @param all_listeners_removed     Runnable that is called when/if the last listener
	 *                          if removed due to a message delivery problem.
	 */
	public LogIndexer( @Nonnull File file, @Nullable A attachment,
		@Nullable LogIndexListener<A> listener,
		int max_index_size, int max_search_hits,
		@Nullable final Runnable all_listeners_removed ) {
		this( file, attachment, listener, max_index_size, max_search_hits, false, all_listeners_removed );
	}


	/**
	 * Close the indexer and free all resources. Continuing to use the object after
	 * closing it will result in undefined results.
	 */
	public void close() {
		// Halt any running searchers
		search_lock.lock();
		try {
			searchers.forEachValue( searcher -> {
				searcher.halt();
				return true;
			} );
		}
		finally {
			search_lock.unlock();
		}

		// Prevent indexer from being restarted
		indexer_scheduled.set( true );

		// Cancel the scheduled task
		file_change_future.cancel( false );
	}


	/**
	 * Indicates whether or not the indexer has {@link LogIndexListener LogIndexListeners}.
	 */
	@SuppressWarnings( "UnusedDeclaration" )
	public boolean hasLogIndexListeners() {
		return listeners.hasListeners();
	}


	/**
	 * Retrieve a number of lines. Any lines that are unavailable will be set to null.
	 *
	 * @param start     The starting line index (zero-based).
	 * @param count     The number of lines to retrieve.
	 *
	 * @return  The array of lines. Will always be non-null and of length <tt>count</tt>.
	 */
	public String[] readLines( final int start, final int count ) throws IOException {
		final String[] to_return = new String[ count ];

		streamLines( start, ( line, line_index ) -> {
			if ( LOG.isTraceEnabled() ) {
				LOG.trace("Reading {} lines starting at {}, got line {}: {}", count, start,
						line_index, line);
			}
//				if ( processed == 0 ) {
//					System.out.println( "Read line: " + start + "->" + line );
//				}
			to_return[ line_index - start ] = line;

			return ( ( line_index - start ) + 1 ) < count;
		} );

		return to_return;
	}


	@Override
	public A getAttachment() {
		return attachment;
	}

	/**
	 * Returns the current number of lines known in the file.
	 */
	public int getLineCount() {
		return num_lines;
	}


	/**
	 * Add an index listener.
	 *
	 * @return      The number of lines currently known in the file.
	 */
	@Override
	public int addListener( LogIndexListener listener ) {
		listeners.add( listener );
		return num_lines;
	}

	@Override
	public void removeListener( LogIndexListener listener ) {
		listeners.remove( listener );
	}


	/**
	 * Starts a new search. Searches will continue to run until cancelled, or until
	 * communication errors occur to the listener.
	 *
	 * @param params        Parameters for the search.
	 * @param listener      The SearchListener that will be notified of search results.
	 *
	 * @return              The search ID.
	 */
	@Override
	public int startSearch( SearchParams params, SearchListener listener ) {
		Objects.requireNonNull( params );
		Objects.requireNonNull( listener );

		search_lock.lock();
		try {
			int id = SEARCH_ID_COUNTER.getAndIncrement();

			Searcher searcher =
				new Searcher( this, id, params, listener, max_search_hits );
			searchers.put( id, searcher );

			return id;
		}
		finally {
			search_lock.unlock();
		}
	}


	/**
	 * Cancels a currently running search.
	 */
	@Override
	public void stopSearch( int search_id ) {
		search_lock.lock();
		try {
			Searcher<?> searcher = searchers.remove(search_id);
			if ( searcher != null ) {
				searcher.halt();
				searcher.close();
			}
		}
		finally {
			search_lock.unlock();
		}
	}


	/**
	 * Resumes a search that was halted or stopped after having reached the maximum number of matches
	 * allowed for a search. Resets tracked number of matches, allowing the resumed search to find
	 * another maximum number of matches.
	 *
	 * @param search_id     The ID for the search to resume
	 *
	 * @return              True if the search was resumed
	 */
	@Override
	public boolean resumeSearch( int search_id ) {
		search_lock.lock();
		try {
			Searcher searcher = searchers.get( search_id );
			if ( searcher != null ) {
				return searcher.resume();
			}
		}
		finally {
			search_lock.unlock();
		}
		return false;
	}


	/**
	 * Opens the file in InputStream format. This can be overridden to support alternate
	 * file formats.
	 */
	@SuppressWarnings( "WeakerAccess" )
	protected InputStream openStreamForFile( File file ) throws IOException {
		return new FileInputStream( file );
	}

	/**
	 * Sets the full/partial index handling for file changes.
	 * @param full_index_after_modified When true, full indexing of the file will occur
	 *                                  whenever the last modified time is updated. When
	 *                                  false, full indexing occurs when the file shrinks,
	 *                                  and partial indexing from the last indexed
	 *                                  position occurs when the file grows.
	 */
	public void setFullIndexAfterModified( boolean full_index_after_modified ) {
		this.full_index_after_modified = full_index_after_modified;
	}


	/**
	 * Read lines from a file until instructed to stop.
	 *
	 * @param start             Index of the first line to read.
	 * @param line_processor    Calls {@link gnu.trove.procedure.TObjectProcedure#execute(Object)}
	 *                          to process a line. The read will stop when that method
	 *                          returns false or the end of file is reached. The integer
	 *                          argument is the line index.
	 *
	 * @return                  The number of lines processed.
	 */
	int streamLines( int start, TObjectIntProcedure<String> line_processor )
		throws IOException {

		// If it's outside the bounds, just return null entries
		if ( start > num_lines ) return 0;

		int lines_processed = 0;

		InputStream in = null;
		InputStreamReader inr = null;
		BufferedReader bin = null;
		try {
			in = openStreamForFile( file );

			int current_line = start;

			if ( start != 0 ) {
				// Find the start position
				row_index_map_lock.lock();
				try {
					// We may not have information for the give line, so step back
					// until we do
					while( !row_index_map.containsKey( current_line ) ) {
						current_line--;
						if ( current_line == 0 ) {
							break;
						}
					}

					// Skip to the starting point
					if ( current_line > 0 ) {
						long location = row_index_map.get( current_line );
						if ( LOG.isDebugEnabled() ) {
							LOG.debug( "Skipping to location {} for line {} to read {}",
								location, current_line, start );
						}

						long to_skip = location;
						while ( to_skip > 0 ) {
							long skipped = in.skip( to_skip );
							if ( skipped <= 0 ) {
								// fallback: read+discard one byte or break with error to avoid infinite loop
								if ( in.read() == -1 ) break;
								skipped = 1;
							}
							to_skip -= skipped;
						}
					}
				}
				finally {
					row_index_map_lock.unlock();
				}
			}

			// Create the BufferedReader, now starting at the correct position
			inr = new InputStreamReader( in );
			bin = new BufferedReader( inr );

			String line;
			while( ( line = bin.readLine() ) != null ) {

				if ( LOG.isTraceEnabled() ) {
					LOG.trace("Current line \"{}\": {} (start: {})", current_line, line,
							start);
				}

//				System.out.println( "current_line " + current_line + ": " + line +
//					" (start: " + start + ")" );
				if ( current_line < start ) {
					current_line++;
					continue;
				}

//				System.out.println( "Line " + current_line + ": " + line );
				boolean keep_going = line_processor.execute( line, current_line );

				current_line++;
				lines_processed++;

				if ( !keep_going ) {
//					System.out.println( "Done at line: " + current_line +
//						" (line: " + line + ")");
					return lines_processed;
				}
			}
//			System.out.println( "Done at line: " + current_line + " (line: " + line + ")");

			// We'll get here if the processed told us to stop
			return lines_processed;
		}
		finally {
			IOKit.close( bin );
			IOKit.close( inr );
			IOKit.close( in );
		}
	}


	private enum NewlineState {
		WAS_NOT_NEWLINE,
		WAS_NEWLINE,
		WAS_CR
	}

	/**
	 * Class that does indexing, both full and partial.
	 */
	private class Indexer implements Runnable {

		private final int starting_line;
		private final long starting_position;
		
		Indexer( int starting_line, long starting_position ) {
			this.starting_line = starting_line;
			this.starting_position = starting_position;
		}
		
		
		@Override
		public void run() {
			Thread.currentThread().setName( file.getName() + " indexer" );

			//noinspection unchecked
			listeners.dispatch().indexingStarting( attachment, starting_line == 0 );

			InputStream root_stream = null;
			PositionTrackingInputStream in = null;
			try {
				root_stream = openStreamForFile( file );
				in = new PositionTrackingInputStream(
					new BufferedInputStream( root_stream ) );

				
				int line = 0;
				
				// If we're skipping some data, do it now
				if ( starting_position > 0 ) {
					long actual = in.skip( starting_position );
					
					// If we weren't able to skip the desired number of bytes, we'll need
					// to do a full index because we don't know what line we're at.
					if ( actual != starting_position ) {
						// If mark is support, we can just reset to the beginning and
						// go on
						if ( in.markSupported() ) {
							in.reset(); // pop to beginning (no mark set)

							// Indicate that we're starting a full index
							//noinspection unchecked
							listeners.dispatch().indexingStarting( attachment, true );
						}
						// If it's not supported, we'll need to close the files are run
						// a new instance.
						else {
							// Close out the files
//							IOKit.close( line_reader );
							IOKit.close( in );
							
							// Run new instance
							Indexer sub = new Indexer( 0, 0 );
							sub.run();
							return;
						}
					}
					else line = starting_line;
				}

				int bytes_since_newline = 0;
				// At this point, ready to do the work, so grab a lock...
				row_index_map_lock.lock();
				try {
					// If this is a full index, clear the existing map
					if ( line == 0 ) {
						row_index_map.clear();
						row_skip_mod = 1;
					}

					// NOTE: Use the same logic as BufferedReader for new lines:
					//       "A line is considered to be terminated by any one
					//       of a line feed ('\n'), a carriage return ('\r'), or a
					//       carriage return followed immediately by a linefeed."
					NewlineState pending_newline_state =
						NewlineState.WAS_NOT_NEWLINE;

					byte[] buf = new byte[8192];
					int read;
					while ( ( read = in.read( buf ) ) != -1 ) {
						final long base_pos = in.position() - read;

						for ( int i = 0; i < read; i++ ) {
							final int bite = buf[i] & 0xFF;
							final long byte_pos = base_pos + i; // absolute position of this byte

							bytes_since_newline++;

							boolean mark_as_new_line = false;

							switch ( pending_newline_state ) {
								case WAS_NOT_NEWLINE:
									break;
								case WAS_NEWLINE:
									mark_as_new_line = true;
									break;
								case WAS_CR:
									// If the current bite is a newline and a CR preceded us,
									// we'll mark the newline on the next character.
									if ( bite == '\n' ) {
										pending_newline_state = NewlineState.WAS_NEWLINE;
										continue;
									} else {
										mark_as_new_line = true;
									}
									break;
							}

							switch ( bite ) {
								case '\r':
									pending_newline_state = NewlineState.WAS_CR;
									break;
								case '\n':
									pending_newline_state = NewlineState.WAS_NEWLINE;
									break;
								default:
									pending_newline_state = NewlineState.WAS_NOT_NEWLINE;
									break;
							}

							if ( !mark_as_new_line ) continue;

							bytes_since_newline = 0;

							if ( LOG.isTraceEnabled() ) {
								LOG.trace("Newline marked at: {}", byte_pos);
							}
							line++;
							if ( line % row_skip_mod == 0 ) {
								row_index_map.put( line, byte_pos );
								if ( row_index_map.size() > max_index_size ) {
									row_skip_mod = increaseRowSkipMod( row_skip_mod, line );
									printRowIndexMap( "after grow" );
								}
							}
						}
					}
				}
				finally {
					row_index_map_lock.unlock();
				}
				
				num_lines = line + ( bytes_since_newline > 0 ? 1 : 0 );

				// Try to read both length and last modified in one operation
				// If we read separately and get an old length and a new last modified, it will be handled
				// on the next check.
				try {
					BasicFileAttributes a = Files.readAttributes( file.toPath(), BasicFileAttributes.class );
					last_file_length = a.size();
					last_last_modified = a.lastModifiedTime().toMillis();
				} catch ( IOException ex ) {
					if ( LOG.isWarnEnabled() ) {
						LOG.warn( "Error reading file attributes for file: {} ", file.toPath(), ex );
					}
					last_file_length = file.length();
					last_last_modified = file.lastModified();
				}

				if ( LOG.isDebugEnabled() ) {
					LOG.debug( "Found {} lines (starting position was {})", num_lines,
						starting_position );
					LOG.debug( "Map size: {}", row_index_map.size() );
					if ( LOG.isTraceEnabled() ) {
						LOG.trace( "Map:" );
						for ( int i = 0; i <= num_lines; i++ ) {
							if ( !row_index_map.containsKey( i ) ) continue;
							LOG.trace( "  {}: {}", i, row_index_map.get( i ) );
						}
					}
				}
			}
			catch( IOException ex ) {
				// TODO?
				ex.printStackTrace();
			}
			finally {
//				IOKit.close( line_reader );
				IOKit.close( in );
				IOKit.close( root_stream );

				indexer_scheduled.set( false );

				//noinspection unchecked
				listeners.dispatch().indexingFinished( attachment, num_lines );
			}
		}
		
		
		private int increaseRowSkipMod( int old_mod, int current_line ) {
			int new_mod = old_mod * 2;

			int old_size = row_index_map.size();

			// NOTE: skip 0, it's never in the map
			for( int i = 1; i <= current_line; i++ ) {
				// If it matched the old mod and now doesn't match, drop it
				if ( i % old_mod == 0 && i % new_mod != 0 ) {
					row_index_map.remove( i );
				}
			}

			if ( DEBUG_ROW_INDEX_MAP ) {
				System.out.println( " Row skip modified. Map size: " + old_size + "->" +
					row_index_map.size() + "  Mod: " + old_mod + "->" + new_mod );
			}

			return new_mod;
		}
	}

	private void printRowIndexMap( String reason ) {
		//noinspection PointlessBooleanExpression
		if ( !DEBUG_ROW_INDEX_MAP ) return;

		StringBuilder buf = new StringBuilder( "Map " );
		buf.append( reason );
		buf.append( ": " );

		int[] keys = row_index_map.keys();
		Arrays.sort( keys );
		boolean first = true;
		for( int key : keys ) {
			if ( first ) first = false;
			else buf.append( ", " );

			buf.append( key );
			buf.append( "=" );
			buf.append( row_index_map.get( key ) );
		}
		System.out.println( buf.toString() );
	}


	/**
	 * Checks the file to see if the size has changed (indicating we need to do additional
	 * indexing). Also checks for in-place edits to a file in which the size does not change.
	 *
	 * NOTE: this can be MUCH more efficient with Java 7's file system watcher mechanism.
	 */
	private class FileChangeChecker implements Runnable {
		@Override
		public void run() {
			// Don't check for a change if we're currently indexing
			if ( indexer_scheduled.get() ) return;

			long file_length;
			long last_modified;

			// Try to read both length and last modified in one operation
			// Since it's still possible that we could get the old length value and new last modified value,
			// which would look like an in-place edit until the length is updated,
			// we handle that with a second poll to confirm.
			try {
				BasicFileAttributes a = Files.readAttributes( file.toPath(), BasicFileAttributes.class );
				file_length = a.size();
				last_modified = a.lastModifiedTime().toMillis();
			} catch ( IOException ex ) {
				if ( LOG.isWarnEnabled() ) {
					LOG.warn( "Error reading file attributes for file: {} ", file.toPath(), ex );
				}
				file_length = file.length();
				last_modified = file.lastModified();
			}
			
			long last_file_length = LogIndexer.this.last_file_length;
			long last_last_modified = LogIndexer.this.last_last_modified;

			int starting_line;
			long starting_position;

			boolean length_same = ( file_length == last_file_length );
			boolean modified_same = ( last_modified == last_last_modified );

			// When full_index_after_modified==false, we assume any updates will also increase the
			// file length as new lines are added. But do not ignore any in-place edits even in this mode.
			if ( length_same ) {
				if ( modified_same ) {
					//				System.out.println( "File unchanged" );
					pending_same_length_modified = false;
					return;
				}

				// Handle lagging length updates and confirm on second consecutive poll
				// before treating as in-place edit (full index)

				// treat as in-place edit
				if ( pending_same_length_modified && pending_modified_value == last_modified) {
					pending_same_length_modified = false;
					starting_line = 0;
					starting_position = 0;

				// record and wait for next poll
				} else {
					pending_same_length_modified = true;
					pending_modified_value = last_modified;
					return;
				}

			// A change in length can be handled immediately
			} else {
				// clear pending marker
				pending_same_length_modified = false;

				// If the file size has increased, we perform either a full or partial index
				if ( file_length > last_file_length ) {
					// Full index
					if ( full_index_after_modified ) {
						starting_line = 0;
						starting_position = 0;

					// Partial index
					} else {
						starting_line = num_lines;
						starting_position = last_file_length;
					}

				// If the file size has decreased, the file has likely rotated.
				// If the file was updated without changing its length, then the file likely had an in place edit.
				// Do a full index for either case.
				} else {
					starting_line = 0;
					starting_position = 0;
				}
			}

			if ( indexer_scheduled.compareAndSet( false, true ) ) {
				SharedThreadPool.INSTANCE.execute(
					new Indexer( starting_line, starting_position ) );
			}
		}
	}


//	public static void main( String[] args ) {
//		final AtomicReference<LogIndexer> indexer_slot = new AtomicReference<LogIndexer>();
//		LogIndexListener listener = new LogIndexListener() {
//			long start;
//
//			@Override
//			public void indexingStarting( File file, boolean full ) {
//				System.out.println( "Index starting: " + ( full ? "full" : "partial" ) );
//				start = System.currentTimeMillis();
//			}
//
//			@Override
//			public void indexingFinished( File file, int total_rows ) {
//				System.out.println( "Index finished: " + total_rows + " (" +
//					( System.currentTimeMillis() - start ) + " ms)" );
//
//				LogIndexer indexer = indexer_slot.get();
//				if ( indexer == null ) return;
//
//				int i = 0;
//				if ( total_rows > 10 ) i = total_rows - 10;
//				for( ; i < total_rows; i++ ) {
//					if ( i != 0 ) System.out.println();
//
//					int count = Math.min( 5, total_rows - i );
//
//					try {
//						long start = System.currentTimeMillis();
//						String[] lines = indexer.readLines( i, count );
//						long time = System.currentTimeMillis() - start;
//						System.out.println( "-- Rows " + i + " - " + ( i + count ) +
//							"(lookup: " + time + " ms):" );
//						for( String line : lines ) {
//							System.out.println( "--   " + line );
//						}
//					}
//					catch( IOException ex ) {
//						ex.printStackTrace();
//					}
//				}
//			}
//		};
//
//		LogIndexer indexer = new LogIndexer( new File( args[ 0 ] ), listener, 25000 );
//		indexer_slot.set( indexer );
//
//		CountDownLatch exit_latch = new CountDownLatch( 1 );
//		try {
//			exit_latch.await();
//		}
//		catch ( InterruptedException e ) {
//			// ignore
//		}
//	}
}
