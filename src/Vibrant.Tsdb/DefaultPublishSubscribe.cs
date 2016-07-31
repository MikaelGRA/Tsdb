using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class DefaultPublishSubscribe : IPublishSubscribe
   {
      private Task _completed = Task.FromResult( 0 );
      private IDictionary<string, HashSet<Action<List<IEntry>>>> _callbacks;
      private readonly TaskFactory _taskFactory;

      public DefaultPublishSubscribe( bool continueOnCapturedSynchronizationContext )
      {
         _callbacks = new ConcurrentDictionary<string, HashSet<Action<List<IEntry>>>>();

         if( continueOnCapturedSynchronizationContext )
         {
            if( SynchronizationContext.Current == null )
            {
               _taskFactory = new TaskFactory( TaskScheduler.Default );
            }
            else
            {
               var scheduler = TaskScheduler.FromCurrentSynchronizationContext();
               _taskFactory = new TaskFactory( scheduler );
            }
         }
         else
         {
            _taskFactory = new TaskFactory( TaskScheduler.Default );
         }
      }

      public Task Publish( IEnumerable<IEntry> entries )
      {
         foreach( var entriesById in entries.GroupBy( x => x.GetId() ) )
         {
            HashSet<Action<List<IEntry>>> subscribers;
            if( _callbacks.TryGetValue( entriesById.Key, out subscribers ) )
            {
               var entriesForSubscriber = entriesById.ToList(); // order by ??
               foreach( var callback in subscribers )
               {
                  _taskFactory.StartNew( () => callback( entriesForSubscriber ) ); // dont wait for this
               }
            }
         }

         return _completed;
      }

      public Task<Func<Task>> Subscribe( IEnumerable<string> ids, Action<List<IEntry>> callback )
      {
         bool performSubscription = false;

         lock( _callbacks )
         {
            foreach( var id in ids )
            {
               HashSet<Action<List<IEntry>>> subscribers;
               if( !_callbacks.TryGetValue( id, out subscribers ) )
               {
                  subscribers = new HashSet<Action<List<IEntry>>>();
                  _callbacks.Add( id, subscribers );
                  performSubscription = true;
               }

               subscribers.Add( callback );
            }
         }

         if( performSubscription )
         {
            // Do networky stuff
         }

         // handle errors, etc.


         return Task.FromResult<Func<Task>>( () => Unsubscribe( ids, callback ) );
      }

      public Task<Func<Task>> SubscribeToAll( Func<List<IEntry>> callback )
      {
         throw new NotImplementedException();
      }

      private Task Unsubscribe( IEnumerable<string> ids, Action<List<IEntry>> callback )
      {
         bool performUnsubscription = false;

         lock( _callbacks )
         {
            foreach( var id in ids )
            {
               HashSet<Action<List<IEntry>>> subscribers;
               if( _callbacks.TryGetValue( id, out subscribers ) )
               {
                  subscribers.Remove( callback );

                  if( subscribers.Count == 0 )
                  {
                     _callbacks.Remove( id );
                     performUnsubscription = true; // ONLY for the current ID though!
                  }
               }
            }
         }

         if( performUnsubscription )
         {
            // Do networky stuff
         }

         return _completed;
      }
   }
}
