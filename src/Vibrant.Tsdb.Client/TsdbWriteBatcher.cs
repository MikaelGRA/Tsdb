using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client
{
   public class TsdbWriteBatcher<TKey, TEntry> : IDisposable
      where TEntry : IEntry
   {
      private static readonly TimeSpan MinWarningInterval = TimeSpan.FromMinutes( 1 );

      private object _sync = new object();
      private TsdbClient<TKey, TEntry> _client;
      private BatchWrite<TKey, TEntry> _currentBatch;
      private Queue<BatchWrite<TKey, TEntry>> _batches;
      private TimeSpan _writeInterval;
      private PublicationType _publish;
      private bool _disposed = false;
      private CancellationTokenSource _cts;
      private ITsdbLogger _logger;
      private int _maxBatchSize;
      private DateTime? _lastWarnTime;

      public TsdbWriteBatcher( 
         TsdbClient<TKey, TEntry> client, 
         PublicationType publish, 
         TimeSpan writeInterval, 
         int maxBatchSize,
         ITsdbLogger logger )
      {
         _client = client;
         _writeInterval = writeInterval;
         _publish = publish;
         _maxBatchSize = maxBatchSize;
         _batches = new Queue<BatchWrite<TKey, TEntry>>();
         _cts = new CancellationTokenSource();
         _logger = logger;
      }

      public void Handle( CancellationToken cancel = default( CancellationToken ) )
      {
         WriteLoop( cancel );
      }

      public Task Write( ISerie<TKey, TEntry> serie )
      {
         lock( _sync )
         {
            if( _currentBatch == null )
            {
               _currentBatch = new BatchWrite<TKey, TEntry>();
            }
            if( _currentBatch.Count + serie.GetEntries().Count > _maxBatchSize )
            {
               _batches.Enqueue( _currentBatch );
               _currentBatch = new BatchWrite<TKey, TEntry>();
            }

            _currentBatch.Add( serie );

            return _currentBatch.Task;
         }
      }

      private BatchWrite<TKey, TEntry> GetBatchToWrite()
      {
         lock( _sync )
         {
            BatchWrite<TKey, TEntry> batch = null;
            if( _batches.Count != 0 )
            {
               batch = _batches.Dequeue();
            }
            else if( _currentBatch != null )
            {
               batch = _currentBatch;
               _currentBatch = null;
            }
            return batch;
         }
      }

      private async void WriteLoop( CancellationToken cancel )
      {
         while( !_disposed )
         {
            var write = GetBatchToWrite();
            if( write != null )
            {
               try
               {
                  await _client.WriteAsync( write, _publish ).ConfigureAwait( false );
                  write.Complete();
               }
               catch( Exception e )
               {
                  write.Fail( e );
               }
            }

            try
            {
               if( _batches.Count == 0 )
               {
                  await Task.Delay( _writeInterval, _cts.Token ).ConfigureAwait( false );
               }
               else if( _batches.Count >= 10 )
               {
                  if( !_lastWarnTime.HasValue || ( DateTime.UtcNow - _lastWarnTime ) > MinWarningInterval )
                  {
                     _logger.Warn( $"There are {_batches.Count} batches of ~{_maxBatchSize} entries queued batches waiting to be written. This is an indication that the storage is too slow to handle the ingestion." );
                     _lastWarnTime = DateTime.UtcNow;
                  }
               }
            }
            catch( OperationCanceledException )
            {
               // simply ignore
            }
         }
      }

      #region IDisposable Support

      protected virtual void Dispose( bool disposing )
      {
         if( !_disposed )
         {
            if( disposing )
            {
               // TODO: dispose managed state (managed objects).
            }

            _disposed = true;
            _cts.Cancel();
         }
      }

      // This code added to correctly implement the disposable pattern.
      public void Dispose()
      {
         // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
         Dispose( true );
         // TODO: uncomment the following line if the finalizer is overridden above.
         // GC.SuppressFinalize(this);
      }

      #endregion
   }
}
