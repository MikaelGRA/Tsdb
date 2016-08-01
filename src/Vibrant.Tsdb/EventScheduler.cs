using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class EventScheduler : IDisposable
   {
      private SortedDictionary<DateTime, HashSet<Action<DateTime>>> _sortedCommands = new SortedDictionary<DateTime, HashSet<Action<DateTime>>>();
      private Timer _timer;
      private DateTime? _nextCallbackTimestamp;
      private object _sync;
      private bool _disposed = false;

      public EventScheduler()
      {
         _sync = new object();
         _timer = new Timer( Callback, null, Timeout.Infinite, Timeout.Infinite );
      }

      public Func<bool> AddCommand( DateTime timestamp, Action<DateTime> action )
      {
         lock( _sync )
         {
            bool updateTimer = false;
            HashSet<Action<DateTime>> existingHashSet;
            if( !_sortedCommands.TryGetValue( timestamp, out existingHashSet ) )
            {
               existingHashSet = new HashSet<Action<DateTime>>();
               _sortedCommands.Add( timestamp, existingHashSet );
               updateTimer = _nextCallbackTimestamp == null || _nextCallbackTimestamp == DateTime.MinValue || timestamp < _nextCallbackTimestamp.Value;
            }

            existingHashSet.Add( action );

            if( updateTimer )
            {
               // we need to update timer
               CalculateNewTimer( timestamp );
            }
         }

         return () => RemoveCommand( timestamp, action );
      }

      public bool RemoveCommand( DateTime timestamp, Action<DateTime> action )
      {
         lock( _sync )
         {
            if( _sortedCommands.Count > 0 )
            {
               HashSet<Action<DateTime>> existingHashSet;
               if( _sortedCommands.TryGetValue( timestamp, out existingHashSet ) )
               {
                  bool wasRemoved = existingHashSet.Remove( action );

                  if( existingHashSet.Count == 0 )
                  {
                     _sortedCommands.Remove( timestamp );

                     // we are changing the next command
                     bool updateTimer = _nextCallbackTimestamp.Value == timestamp;
                     if( updateTimer )
                     {
                        var nextCommand = _sortedCommands.FirstOrDefault();

                        // if we still have a scheduled callback
                        if( nextCommand.Key != default( DateTime ) )
                        {
                           CalculateNewTimer( nextCommand.Key );
                        }
                        else
                        {
                           CalculateNewTimer( null );
                        }
                     }
                  }

                  return wasRemoved;
               }
            }
            return false;
         }
      }

      private void Callback( object state )
      {
         DateTime? executeTimestamp = null;
         var actionToBeExecuted = new List<Action<DateTime>>();
         lock( _sync )
         {
            if( _sortedCommands.Count > 0 )
            {
               HashSet<Action<DateTime>> actions;
               if( _sortedCommands.TryGetValue( _nextCallbackTimestamp.Value, out actions ) )
               {
                  executeTimestamp = _nextCallbackTimestamp.Value;
                  actionToBeExecuted.AddRange( actions );
                  _sortedCommands.Remove( _nextCallbackTimestamp.Value );

                  // UPDATE next timestamp
                  var nextCommand = _sortedCommands.FirstOrDefault();

                  // if we still have a scheduled callback
                  if( nextCommand.Key != default( DateTime ) )
                  {
                     CalculateNewTimer( nextCommand.Key );
                  }
                  else
                  {
                     CalculateNewTimer( null );
                  }
               }
               else
               {
                  // No need for any action, handled in remove/add functions
               }
            }

         }
         foreach( var action in actionToBeExecuted )
         {
            action( executeTimestamp.Value );
         }
      }

      private void CalculateNewTimer( DateTime? nextCallback )
      {
         if( nextCallback.HasValue )
         {
            var currentTime = DateTime.UtcNow;

            var timeToNextIteration = nextCallback.Value - currentTime > TimeSpan.Zero
               ? nextCallback.Value - currentTime
               : TimeSpan.Zero;

            _timer.Change( timeToNextIteration, Timeout.InfiniteTimeSpan );
         }
         else
         {
            _timer.Change( Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan );
         }

         _nextCallbackTimestamp = nextCallback;
      }

      #region IDisposable Support

      protected virtual void Dispose( bool disposing )
      {
         if( !_disposed )
         {
            if( disposing )
            {
               _timer.Dispose();
            }

            _disposed = true;
         }
      }

      public void Dispose()
      {
         Dispose( true );
      }

      #endregion
   }
}
