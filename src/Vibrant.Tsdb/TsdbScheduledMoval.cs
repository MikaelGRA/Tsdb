using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TsdbScheduledMoval<TEntry>
      where TEntry : IEntry
   {
      private TsdbVolumeMoval _moval;
      private TsdbEngine<TEntry> _engine;
      private Func<bool> _unsubscribe;

      public TsdbScheduledMoval( TsdbEngine<TEntry> engine, TsdbVolumeMoval moval )
      {
         _engine = engine;
         _moval = moval;
      }

      public async void Execute( DateTime timestamp )
      {
         try
         {
            await _engine.Client.MoveToVolumeStorage( new[] { _moval.Id }, _engine.Work.GetDynamicMovalBatchSize(), _moval.To ).ConfigureAwait( false );
         }
         catch( Exception e )
         {
            _engine.RaiseMoveToVolumeStorageFailed( e );
         }

         var newMoval = await _engine.Work.GetMovalAsync( _moval ).ConfigureAwait( false );

         if( newMoval != null )
         {
            _moval = newMoval;

            // schedule again
            Schedule();
         }
      }

      public void Schedule()
      {
         _unsubscribe = _engine.Scheduler.AddCommand( _moval.Timestamp, Execute );
      }

      public void Reschedule( TsdbVolumeMoval newMoval )
      {
         _unsubscribe?.Invoke();
         _moval = newMoval;
         Schedule();
      }

      public void Unschedule()
      {
         _unsubscribe?.Invoke();
      }
   }
}
