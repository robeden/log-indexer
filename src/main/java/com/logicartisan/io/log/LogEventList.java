package com.logicartisan.io.log;

import ca.odell.glazedlists.AbstractEventList;
import ca.odell.glazedlists.event.ListEventPublisher;
import ca.odell.glazedlists.util.concurrent.LockFactory;
import ca.odell.glazedlists.util.concurrent.ReadWriteLock;
import com.logicartisan.common.core.thread.SharedThreadPool;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 *
 */
public class LogEventList<A> extends AbstractEventList<String>
	implements LogIndexListener<A>, Closeable {

	private static final Logger LOG =
		LoggerFactory.getLogger( LogEventList.class );

	private static final int CHUNK_SIZE = 25;

	private final ThreadPoolExecutor query_thread_pool;

	private final LogAccess<A> access;

	private final TIntObjectMap<Chunk> line_map =
		new TIntObjectHashMap<>();
	private final ReferenceQueue chunk_ref_queue =
		new ReferenceQueue();

	private final TIntSet active_query_set = new TIntHashSet();
	private final Lock active_query_set_lock = new ReentrantLock();

	private final AtomicInteger rows_slot = new AtomicInteger( 0 );



	/**
	 * This constructor should be used if the model HAS NOT already been added as a
	 * listener.
	 */
	public LogEventList( @Nonnull LogAccess<A> access ) {
		this( access, null, null );
	}

	/**
	 * This constructor should be used if the model HAS NOT already been added as a
	 * listener.
	 */
	public LogEventList( final @Nonnull LogAccess<A> access,
		@Nullable ListEventPublisher publisher,
		@Nullable ReadWriteLock read_write_lock ) {

		super( publisher );
		this.readWriteLock = read_write_lock == null ?
			LockFactory.DEFAULT.createReadWriteLock() : read_write_lock;

		this.access = access;
		query_thread_pool = createThreadPool();

		// Add listener
		SharedThreadPool.INSTANCE.execute( () -> {
			int line_count = access.addListener( LogEventList.this );
			processNewRowCount( line_count );
		} );
	}



	/**
	 * This constructor should be used if the model HAS already been added as a listener.
	 */
	public LogEventList( @Nonnull LogAccess<A> access, int line_count ) {
		this( access, line_count, null, null );
	}

	/**
	 * This constructor should be used if the model HAS already been added as a listener.
	 */
	public LogEventList( @Nonnull LogAccess<A> access, int line_count,
		@Nullable ListEventPublisher publisher,
		@Nullable ReadWriteLock read_write_lock ) {

		super( publisher );
		this.readWriteLock = read_write_lock == null ?
			LockFactory.DEFAULT.createReadWriteLock() : read_write_lock;

		this.access = access;
		query_thread_pool = createThreadPool();
	}



	@Override
	public void close() {
		LOG.debug( "close" );

		try {
			access.removeListener( this );
		}
		catch( Exception ex ) {
			// ignore
		}

		query_thread_pool.shutdownNow();
	}

	/**
	 * Equivalent to {@link #close()}.
	 */
	@Override
	public void dispose() {
		close();
	}


	@Override
	public int size() {
		return rows_slot.get();
	}


	@Override
	public String get( int index ) {
		// Clean up map to remove GC'ed chunks
		Chunk ref;
		while( ( ref = ( Chunk ) chunk_ref_queue.poll() ) != null ) {
			if ( LOG.isDebugEnabled() ) {
				LOG.debug( "Chunk {} GC'ed", ref.index );
			}
			line_map.remove( ref.index );
		}


		// Find index to query within the chunk size
		int distance_from_main_index = index % CHUNK_SIZE;
		int main_index;
		if ( distance_from_main_index == 0 ) main_index = index;
		else main_index = index - distance_from_main_index;

		SoftReference<String[]> chunk_ref = line_map.get( main_index );
		String[] chunk = null;
		if ( chunk_ref != null ) chunk = chunk_ref.get();

		String line = null;
		if ( chunk != null ) line = chunk[ distance_from_main_index ];

		if ( line == null ) {
			active_query_set_lock.lock();
			try {
				// If a load isn't already in progress, start it now
				if ( !active_query_set.contains( main_index ) ) {
					if ( LOG.isDebugEnabled() ) {
						LOG.debug( "Begin query for chunk {} (line index {})", main_index,
							index );
					}
					query_thread_pool.execute( new DataFetcher( main_index ) );
//					System.out.println( "Active queries: " + active_query_set.size() );
					active_query_set.add( main_index );
				}
				else if ( LOG.isTraceEnabled() ) {
					LOG.trace( "Request for row {} found no data. Chunk {} already " +
						"being fetched.", index, main_index );
				}
			}
			finally {
				active_query_set_lock.unlock();
			}

			return "Loading...";
//			return Resources.MESSAGE_LOADING;
		}
		else {
			if ( LOG.isTraceEnabled() ) {
				LOG.trace( "Found data for row {}: {}", index, line );
			}
			return line;
		}
	}



	@Override
	public void indexingFinished( final A attachment, final int total_rows ) {
		if ( LOG.isDebugEnabled() ) {
			LOG.debug( "indexingFinished({},{})", attachment, total_rows );
		}
		processNewRowCount( total_rows );
	}

	@Override
	public void indexingStarting( A attachment, boolean full ) {
		if ( LOG.isDebugEnabled() ) {
			LOG.debug( "indexingStarting({},{})", attachment, full );
		}
	}


	/**
	 * A thread-safe method that will process a new row count.
	 */
	private void processNewRowCount( final int rows ) {
		readWriteLock.writeLock().lock();
		try {
			if ( LOG.isDebugEnabled() ) {
				LOG.debug( "New row count: {}", rows );
			}

			int previous_rows = rows_slot.get();
			rows_slot.set( rows );

			// See if there was previously a partial cache
			int distance_from_main_index = ( previous_rows - 1 ) % CHUNK_SIZE;
			if ( distance_from_main_index != ( CHUNK_SIZE - 1 ) ) {
				int main_index;
				if ( distance_from_main_index == 0 ) main_index = ( previous_rows - 1 );
				else main_index = ( previous_rows - 1 ) - distance_from_main_index;

				line_map.remove( main_index );
			}


			if ( rows < previous_rows ) {
				// If the file got smaller, reset everything
				updates.beginEvent();
				updates.addDelete( rows, previous_rows );
				updates.addUpdate( 0, rows );
				if ( LOG.isDebugEnabled() ) {
					LOG.debug( "Fire delete ({} - {}), and update (0 - {})", rows,
						previous_rows, rows );
				}
				updates.commitEvent();
			}
			else {
				updates.beginEvent();
				updates.addInsert( previous_rows, rows );
				if ( LOG.isDebugEnabled() ) {
					LOG.debug( "Fire insert ({} - {})", previous_rows, rows );
				}
				updates.commitEvent();
			}

		}
		finally {
			readWriteLock.writeLock().unlock();
		}
	}

	private ThreadPoolExecutor createThreadPool() {
		return new ThreadPoolExecutor( 1, 1, Long.MAX_VALUE,
			TimeUnit.DAYS, new ArrayBlockingQueue<>( 5 ),
			// This is a modified version of DiscardOldestPolicy that removes the index
			// that's no longer being queried for when a task is thrown away.
			( r, e ) -> {
	            if (!e.isShutdown()) {
	                Runnable to_discard = e.getQueue().poll();
		            if ( to_discard != null ) {
			            DataFetcher fetcher = ( DataFetcher ) to_discard;
			            active_query_set_lock.lock();
			            try {
				            active_query_set.remove( fetcher.index );
			            }
			            finally {
				            active_query_set_lock.unlock();
			            }
		            }

	                e.execute(r);
	            }
			} );
	}


	private int countNonNullLines( String[] lines ) {
		int count = 0;
		for( String line : lines ) {
			if ( line == null ) break;
			count++;
		}
		return count;
	}




	private class DataFetcher implements Runnable {
		private final int index;

		DataFetcher( int index ) {
			this.index = index;
		}

		@Override
		public void run() {
			try {
				Thread.sleep( 500 );
			}
			catch ( InterruptedException e ) {
				// ignore
			}

			Thread.currentThread().setName( "DataFetcher: " + index );
			try {
				String[] lines;
				try {
	//				System.out.println( "Fetch " + index );
					lines = access.readLines( index, CHUNK_SIZE );

					if ( lines[ 0 ] == null ) {
						if ( LOG.isInfoEnabled() ) {
							LOG.info( "Query for chunk {} gave all nulls",
								index );
						}
						return;
					}
				}
				catch( Throwable ex ) {
					if ( LOG.isWarnEnabled() ) {
						LOG.warn( "Error reading lines: {} (count: {})", index,
							CHUNK_SIZE, ex );
					}
					lines = new String[ CHUNK_SIZE ];
					String line = "Error reading: " + ex;
                    Arrays.fill(lines, line);
				}


		        // create the change event
		        updates.beginEvent();
				updates.addUpdate( index, index + lines.length );

				readWriteLock.writeLock().lock();
				try {
					if ( LOG.isDebugEnabled() ) {
						LOG.debug( "Put data for chunk {}. Non-null lines: {}",
							index,
							countNonNullLines( lines ) );
					}


					line_map.put( index, new Chunk(lines, chunk_ref_queue, index));

				}
				finally {
					readWriteLock.writeLock().unlock();
				}

		        // fire the event
		        updates.commitEvent();
			}
			finally {
				active_query_set_lock.lock();
				try {
					active_query_set.remove( index );
				}
				finally {
					active_query_set_lock.unlock();
				}
			}
		}
	}



	private static class Chunk extends SoftReference<String[]> {
		private final int index;

		public Chunk( String[] chunk, ReferenceQueue<? super String[]> q,
			int index ) {

			super( chunk, q );
			this.index = index;
		}
	}
}
