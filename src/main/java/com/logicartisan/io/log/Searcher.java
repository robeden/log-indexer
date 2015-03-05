package com.logicartisan.io.log;

import com.starlight.listeners.ListenerSupport;
import com.starlight.listeners.ListenerSupportFactory;
import com.starlight.listeners.MessageDeliveryErrorHandler;
import com.starlight.thread.SharedThreadPool;
import gnu.trove.procedure.TObjectProcedure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 *
 */
class Searcher<A>
	implements Runnable, MessageDeliveryErrorHandler<SearchListener>, LogIndexListener<A> {

	private static final Logger LOG = LoggerFactory.getLogger( Searcher.class );

	private static final int MATCH_NOTIFY_BATCH_SIZE = 50;

	private static final int MAX_ERRORS_TO_CANCEL = 3;

	private final LogIndexer parent;
	private final int id;
	private final int max_search_hits;

	private final Matcher matcher;
	private final String token;
	private final boolean case_sensitive;

	private final ListenerSupport<SearchListener,?> listeners;

	private final AtomicBoolean running = new AtomicBoolean( false );
	private final AtomicInteger known_lines;
	private final AtomicBoolean index_reset = new AtomicBoolean( false );
	private volatile int processed_lines = 0;
	private volatile boolean completed_initial_search = false;

	private volatile boolean keep_going = true;

	private final AtomicInteger hit_counter = new AtomicInteger( 0 );


	Searcher( LogIndexer parent, int id, SearchParams params,
		SearchListener listener, int max_search_hits ) {

//		System.out.println( "Listener for searcher: " + listener );

		this.parent = parent;
		this.id = id;
		this.max_search_hits = max_search_hits;

		if ( params instanceof SearchParams.RegexSearchParams ) {
			Pattern pattern = ( ( SearchParams.RegexSearchParams ) params ).getPattern();
			matcher = pattern.matcher( "" );
			token = null;
			case_sensitive = false;
		}
		else if ( params instanceof SearchParams.SimpleSearchParams ) {
			SearchParams.SimpleSearchParams sparams =
				( SearchParams.SimpleSearchParams ) params;
			matcher = null;
			case_sensitive = sparams.isCaseSensitive();
			// NOTE: if not case sensitive, store token lower case
			token = case_sensitive ? sparams.getToken() : sparams.getToken().toLowerCase();
		}
		else throw new UnsupportedOperationException( "Unknown param type: " +
			params.getClass().getName() );

		// NOTE: this will only ever have one listener, but simplifies dispatching and
		//       preserving message order.
		listeners = ListenerSupportFactory.create( SearchListener.class,
			SharedThreadPool.INSTANCE, this );
		listeners.add( listener );

		// Listen for changes from the parent
		// NOTE: using compare here because there's a very slim chance the listener could
		//       fire before the original value has been set. In that case, it is the
		//       authoritative value.
		known_lines = new AtomicInteger( 0 );
		known_lines.compareAndSet( 0, parent.addListener( this ) );

		// If there is data, start processing
		if ( known_lines.get() != 0 ) {
			SharedThreadPool.INSTANCE.execute( this );
		}
	}


	/**
	 * Stop searching.
	 */
	void halt() {
		keep_going = false;
	}


	///////////////////////////////////////////////
	// From Runnable

	@Override
	public void run() {
		Thread current_thread = Thread.currentThread();
		current_thread.setName( "LogIndexer-Searcher-" + id );
		current_thread.setPriority( Thread.MIN_PRIORITY + 2 );

		try {
			inner_run();
		}
		finally {
			// Indicate we're no longer running
			running.set( false );
		}
	}

	private void inner_run() {
		while( keep_going ) {
			// First, check to see if we're been reset
			if ( index_reset.compareAndSet( true, false ) ) {
				processed_lines = 0;
			}

			// See how many lines are known and compare to number we've processed
			int known_lines = this.known_lines.get();
			if ( processed_lines == known_lines ) return;   // done

			final Queue<SearchMatch> match_notification_queue = new LinkedList<>();

			final AtomicBoolean hit_max = new AtomicBoolean( false );
			try {
				parent.streamLines( processed_lines,
					new TObjectProcedure<String>() {
						@Override
						public boolean execute( String line ) {
							try {
								// NOTE: according to LogIndexer API, unavailable lines
								//       will have a null value.
								if ( line == null ) {
									return keep_going;
								}

								try {
									if ( searchLine( line, processed_lines,
										match_notification_queue, hit_counter ) ) {

										sendNotifications( match_notification_queue,
											false );
									}
								}
								catch( SearchLimitReachedException ex ) {
									hit_max.set( true );
									keep_going = false;
									return false;
								}
							}
							finally {
								processed_lines++;
							}
							return keep_going;
						}
					} );
//				System.out.println( "Streamed lines: " + streamed_lines );
			}
			catch ( IOException e ) {
				LOG.warn( "Error searching log file: {}", parent.getAttachment(), e );
				return;     // retry when a change is made
			}

			sendNotifications( match_notification_queue, true );    // flush

			if ( hit_max.get() || ( !completed_initial_search && keep_going &&
				processed_lines == known_lines ) ) {

				listeners.dispatch().searchScanFinished( id, hit_max.get() );
				completed_initial_search = true;
			}
		}
	}

	/**
	 * @return      True if the line matched
	 */
	private boolean searchLine( String line, int row_index,
		Queue<SearchMatch> match_notification_queue, AtomicInteger hit_counter )
		throws SearchLimitReachedException {

		if ( line.length() == 0 ) return false;

		boolean hit = false;
		if ( matcher != null ) {
			matcher.reset( line );

			int start_position = 0;
			while( matcher.find( start_position ) ) {
				hit = true;

				int start = matcher.start();
				SearchMatch match =
					new SearchMatch( row_index, start, matcher.end() - start );
				match_notification_queue.add( match );

				// See if we've exceeded the max number of hits
				int hit_count = hit_counter.incrementAndGet();
				if ( hit_count >= max_search_hits ) {
					throw new SearchLimitReachedException();
				}

				start_position = matcher.end() + 1;
				if ( start_position >= line.length() ) break;
			}
		}
		else {
			// Make line lower case (matching how the token is stored) is matching case
			// in-sensitively.
			String search_line = case_sensitive ? line : line.toLowerCase();

			int start_search_index = 0;
			int match_index;
			while( ( match_index =
				search_line.indexOf( token, start_search_index ) ) >= 0 ) {

				hit = true;

				SearchMatch match =
					new SearchMatch( row_index, match_index, token.length() );
				match_notification_queue.add( match );

				// See if we've exceeded the max number of hits
				int hit_count = hit_counter.incrementAndGet();
				if ( hit_count >= max_search_hits ) {
					throw new SearchLimitReachedException();
				}

				start_search_index = match_index + token.length();
			}
		}

		return hit;
	}

	private void sendNotifications( Queue<SearchMatch> match_notification_queue,
		boolean flush ) {

		int queue_size = match_notification_queue.size();

		// If we're not sending everything (flushing) and we have less than the batch
		// size, then there's nothing to do right now.
		if ( !flush && queue_size < MATCH_NOTIFY_BATCH_SIZE ) return;

		// Keep going while either:
		//  1) We're flushing and there's still data
		//  2) We're not flushing and there's still a batch or more data left
		while( ( flush && queue_size != 0 ) ||
			( !flush && queue_size >= MATCH_NOTIFY_BATCH_SIZE ) ) {

			SearchMatch[] matches =
				new SearchMatch[ Math.min( queue_size, MATCH_NOTIFY_BATCH_SIZE ) ];
			for( int i = 0; i < matches.length; i++ ) {
				matches[ i ] = match_notification_queue.poll();
				assert matches[ i ] != null :
					"Null return from poll? " + match_notification_queue +
					" (flush=" + flush + ", queue_size=" + queue_size +
					", matches.length=" + matches.length + ", i=" + i +", queue=" +
					match_notification_queue + ")";
			}

			// Fire to listeners
			listeners.dispatch().searchTermMatches( id, matches );

			queue_size = match_notification_queue.size();
		}

		assert !flush || match_notification_queue.isEmpty() :
			"Queue=" + match_notification_queue;
	}

	///////////////////////////////////////////////
	// From LogIndexListener

	@Override
	public void indexingStarting( A attachment, boolean full ) {
		// If a full re-index is happening, reset the known lines and then indicate the
		// reset.
		if ( full ) {
			known_lines.set( 0 );
			index_reset.set( true );
		}
	}

	@Override
	public void indexingFinished( A attachment, int total_rows ) {
		known_lines.set( total_rows );

		if ( hit_counter.get() >= max_search_hits ) return;

		// If the thread isn't processing, start it
		if ( running.compareAndSet( false, true ) ) {
			SharedThreadPool.INSTANCE.execute( this );
		}
	}


	///////////////////////////////////////////////
	// From SearchListener

	@Override
	public ErrorResponse deliveryError(
		SearchListener listener, Throwable throwable, int overall_error_count,
		int overall_success_count, int consecutive_errors, boolean fatal ) {

		if ( throwable instanceof AssertionError ) {
			throwable.printStackTrace();
			return ErrorResponse.DROP_MESSAGE;
		}

		// If it's a fatal
		if ( fatal || consecutive_errors >= MAX_ERRORS_TO_CANCEL ) {
			halt();
			return ErrorResponse.REMOVE_LISTENER;
		}

		return ErrorResponse.RETRY_MESSAGE;
	}

	@Override
	public ErrorResponse excessiveBacklog(
		SearchListener listener, int backlog_size, int overall_error_count,
		int overall_success_count, int consecutive_errors ) {

		return ErrorResponse.REMOVE_LISTENER;
	}

	@Override
	public void lastListenerRemoved() {}
}
