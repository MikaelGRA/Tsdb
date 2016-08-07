using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TsdbWriteBatcher<TEntry> : IDisposable
      where TEntry : IEntry
   {
      private object _sync = new object();
      private TsdbClient<TEntry> _client;
      private BatchWrite<TEntry> _currentBatch;
      private Queue<BatchWrite<TEntry>> _batches;
      private TimeSpan _writeInterval;
      private PublicationType _publish;
      private bool _disposed = false;
      private CancellationTokenSource _cts;
      private int _maxBatchSize;

      public TsdbWriteBatcher( TsdbClient<TEntry> client, PublicationType publish, TimeSpan writeInterval, int maxBatchSize )
      {
         _client = client;
         _writeInterval = writeInterval;
         _publish = publish;
         _maxBatchSize = maxBatchSize;
         _batches = new Queue<BatchWrite<TEntry>>();
         _cts = new CancellationTokenSource();

         ThreadPool.QueueUserWorkItem( WriteLoop );
      }

      public Task Write( IEnumerable<TEntry> entries )
      {
         lock( _sync )
         {
            if( _currentBatch == null )
            {
               _currentBatch = new BatchWrite<TEntry>();
            }
            if( _currentBatch.Entries.Count + entries.Count() > _maxBatchSize )
            {
               _batches.Enqueue( _currentBatch );
               _currentBatch = new BatchWrite<TEntry>();
            }

            _currentBatch.Add( entries );

            return _currentBatch.Task;
         }
      }

      private BatchWrite<TEntry> GetBatchToWrite()
      {
         lock( _sync )
         {
            BatchWrite<TEntry> batch = null;
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

      private async void WriteLoop( object state )
      {
         while( !_disposed )
         {
            BatchWrite<TEntry> write = GetBatchToWrite();

            if( write != null )
            {
               try
               {
                  await _client.Write( write.Entries, _publish ).ConfigureAwait( false );
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
