using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TsdbEngine<TEntry> : IDisposable
      where TEntry : IEntry
   {
      public event EventHandler<ExceptionEventArgs> MoveTemporaryDataFailed;
      public event EventHandler<ExceptionEventArgs> MoveToVolumeStorageFailed;

      private EventScheduler _scheduler;
      private TsdbClient<TEntry> _client;
      private IWorkProvider _workProvider;
      private Dictionary<string, TsdbScheduledMoval<TEntry>> _scheduledWork;
      private Func<bool> _unsubscribe;
      private bool _disposed = false;

      public TsdbEngine( IWorkProvider workProvider, TsdbClient<TEntry> client )
      {
         _client = client;
         _workProvider = workProvider;
         _scheduler = new EventScheduler();
         _scheduledWork = new Dictionary<string, TsdbScheduledMoval<TEntry>>();
      }

      internal TsdbClient<TEntry> Client
      {
         get
         {
            return _client;
         }
      }

      internal EventScheduler Scheduler
      {
         get
         {
            return _scheduler;
         }
      }

      internal IWorkProvider Work
      {
         get
         {
            return _workProvider;
         }
      }

      public async Task StartAsync()
      {
         var movals = await _workProvider.GetAllMovalsAsync( DateTime.UtcNow ).ConfigureAwait( false );

         lock( _scheduledWork )
         {
            foreach( var moval in movals )
            {
               var work = new TsdbScheduledMoval<TEntry>( this, moval );
               _scheduledWork.Add( moval.Id, work );
               work.Schedule();
            }
         }

         var scheduleTime = DateTime.UtcNow + _workProvider.GetTemporaryMovalInterval();
         _scheduler.AddCommand( scheduleTime, MoveTemporaryData );

         _workProvider.MovalChangedOrAdded += WorkProvider_MovalChangedOrAdded;
         _workProvider.MovalRemoved += WorkProvider_MovalRemoved;
      }

      private async void MoveTemporaryData( DateTime timestamp )
      {
         if( !_disposed )
         {
            try
            {
               await _client.MoveFromTemporaryStorage( _workProvider.GetTemporaryMovalBatchSize() ).ConfigureAwait( false );
            }
            catch( Exception e )
            {
               RaiseMoveTemporaryDataFailed( e );
            }

            var scheduleTime = DateTime.UtcNow + _workProvider.GetTemporaryMovalInterval();
            _unsubscribe = _scheduler.AddCommand( scheduleTime, MoveTemporaryData );
         }
      }

      private void WorkProvider_MovalRemoved( string id )
      {
         lock( _scheduledWork )
         {
            TsdbScheduledMoval<TEntry> work;
            if( _scheduledWork.TryGetValue( id, out work ) )
            {
               _scheduledWork.Remove( id );

               work.Unschedule();
            }
         }
      }

      private void WorkProvider_MovalChangedOrAdded( TsdbVolumeMoval moval )
      {
         lock( _scheduledWork )
         {
            TsdbScheduledMoval<TEntry> work;
            if( _scheduledWork.TryGetValue( moval.Id, out work ) )
            {
               work.Reschedule( moval );
            }
            else
            {
               work = new TsdbScheduledMoval<TEntry>( this, moval );
               _scheduledWork.Add( moval.Id, work );
               work.Schedule();
            }
         }
      }

      internal void RaiseMoveTemporaryDataFailed( Exception exception )
      {
         MoveTemporaryDataFailed?.Invoke( this, new ExceptionEventArgs( exception ) );
      }

      internal void RaiseMoveToVolumeStorageFailed( Exception exception )
      {
         MoveToVolumeStorageFailed?.Invoke( this, new ExceptionEventArgs( exception ) );
      }

      protected virtual void Dispose( bool disposing )
      {
         if( !_disposed )
         {
            if( disposing )
            {
               _scheduler.Dispose();
            }

            _unsubscribe?.Invoke();
            _workProvider.MovalChangedOrAdded -= WorkProvider_MovalChangedOrAdded;
            _workProvider.MovalRemoved -= WorkProvider_MovalRemoved;

            _disposed = true;
         }
      }

      public void Dispose()
      {
         Dispose( true );
      }
   }
}
