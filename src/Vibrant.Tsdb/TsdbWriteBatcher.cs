using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TsdbWriteBatcher : IDisposable
   {
      private object _sync = new object();
      private TsdbClient _client;
      private BatchWrite _currentBatch;
      private TimeSpan _writeInterval;
      private Publish _publish;
      private bool _disposed = false;
      private CancellationTokenSource _cts;

      public TsdbWriteBatcher( TsdbClient client, Publish publish, TimeSpan writeInterval )
      {
         _client = client;
         _writeInterval = writeInterval;
         _publish = publish;
         _cts = new CancellationTokenSource();

         Task.Run( new Action( WriteLoop ), _cts.Token );
      }

      public Task Write( IEnumerable<IEntry> entries )
      {
         lock( _sync )
         {
            if( _currentBatch == null )
            {
               _currentBatch = new BatchWrite();
            }
            _currentBatch.Add( entries );

            return _currentBatch.Task;
         }
      }

      private async void WriteLoop()
      {
         while( !_disposed )
         {
            BatchWrite write;
            lock( _sync )
            {
               write = _currentBatch;
               _currentBatch = null;
            }

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
               await Task.Delay( _writeInterval, _cts.Token ).ConfigureAwait( false );
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
