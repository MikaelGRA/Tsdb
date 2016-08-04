using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TsdbScheduledMoval
   {
      private TsdbVolumeMoval _moval;
      private TsdbEngine _engine;
      private Func<bool> _unsubscribe;

      public TsdbScheduledMoval( TsdbEngine engine, TsdbVolumeMoval moval )
      {
         _engine = engine;
         _moval = moval;
      }

      public async void Execute( DateTime timestamp )
      {
         try
         {
            await _engine.Client.MoveToVolumeStorage( new[] { _moval.Id }, _moval.To ).ConfigureAwait( false );
         }
         catch( Exception )
         {
            // TODO: How to handle???
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
