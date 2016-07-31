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
      private IDictionary<Action<List<IEntry>>, byte> _allCallbacks;

      private IDictionary<string, HashSet<Action<List<IEntry>>>> _callbacks;
      private readonly TaskFactory _taskFactory;

      public DefaultPublishSubscribe( bool continueOnCapturedSynchronizationContext )
      {
         _callbacks = new ConcurrentDictionary<string, HashSet<Action<List<IEntry>>>>();
         _allCallbacks = new ConcurrentDictionary<Action<List<IEntry>>, byte>();

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

      public virtual Task WaitWhileDisconnected()
      {
         return _completed;
      }

      public virtual Task Publish( IEnumerable<IEntry> entries )
      {
         return OnPublished( entries );
      }

      public async Task<Func<Task>> Subscribe( IEnumerable<string> ids, Action<List<IEntry>> callback )
      {
         var newSubscriptions = AddSubscriptions( ids, callback );
         if( newSubscriptions.Count > 0 )
         {
            try
            {
               await OnSubscribed( newSubscriptions ).ConfigureAwait( false );
            }
            catch( Exception )
            {
               RemoveSubscriptions( ids, callback );

               throw;
            }
         }

         return () => Unsubscribe( ids, callback );
      }

      private async Task Unsubscribe( IEnumerable<string> ids, Action<List<IEntry>> callback )
      {
         var removedSubscriptions = RemoveSubscriptions( ids, callback );
         if( removedSubscriptions.Count > 0 )
         {
            try
            {
               await OnUnsubscribed( removedSubscriptions ).ConfigureAwait( false );
            }
            catch( Exception )
            {
               AddSubscriptions( ids, callback );

               throw;
            }
         }
      }

      public async Task<Func<Task>> SubscribeToAll( Action<List<IEntry>> callback )
      {
         bool subscribeToAll = false;

         lock( _allCallbacks )
         {
            if( _allCallbacks.Count == 0 )
            {
               subscribeToAll = true;
            }

            _allCallbacks.Add( callback, 0 );
         }

         if( subscribeToAll )
         {
            try
            {
               await OnSubscribedToAll().ConfigureAwait( false );
            }
            catch( Exception )
            {
               lock( _allCallbacks )
               {
                  _allCallbacks.Remove( callback );
               }

               throw;
            }
         }

         return () => UnsubscribeFromAll( callback );
      }

      public async Task UnsubscribeFromAll( Action<List<IEntry>> callback )
      {
         bool unsubscribeFromAll = false;

         lock( _allCallbacks )
         {
            _allCallbacks.Remove( callback );

            if( _allCallbacks.Count == 0 )
            {
               unsubscribeFromAll = true;
            }
         }

         if( unsubscribeFromAll )
         {
            try
            {
               await OnUnsubscribedFromAll().ConfigureAwait( false );
            }
            catch( Exception )
            {
               lock( _allCallbacks )
               {
                  _allCallbacks.Add( callback, 0 );
               }

               throw;
            }
         }
      }

      private List<string> AddSubscriptions( IEnumerable<string> ids, Action<List<IEntry>> callback )
      {
         List<string> newSubscriptions = new List<string>();
         lock( _callbacks )
         {
            foreach( var id in ids )
            {
               HashSet<Action<List<IEntry>>> subscribers;
               if( !_callbacks.TryGetValue( id, out subscribers ) )
               {
                  subscribers = new HashSet<Action<List<IEntry>>>();
                  _callbacks.Add( id, subscribers );
                  newSubscriptions.Add( id );
               }

               subscribers.Add( callback );
            }
         }
         return newSubscriptions;
      }

      private List<string> RemoveSubscriptions( IEnumerable<string> ids, Action<List<IEntry>> callback )
      {
         List<string> subscriptionsRemoved = new List<string>();
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
                     subscriptionsRemoved.Add( id );
                  }
               }
            }
         }
         return subscriptionsRemoved;
      }

      protected virtual Task OnSubscribed( IEnumerable<string> ids )
      {
         return _completed;
      }

      protected virtual Task OnUnsubscribed( IEnumerable<string> ids )
      {
         return _completed;
      }

      protected virtual Task OnSubscribedToAll()
      {
         return _completed;
      }

      protected virtual Task OnUnsubscribedFromAll()
      {
         return _completed;
      }

      protected Task OnPublished( IEnumerable<IEntry> entries )
      {
         foreach( var entriesById in entries.GroupBy( x => x.GetId() ) )
         {
            List<IEntry> entriesForSubscriber = null;

            HashSet<Action<List<IEntry>>> subscribers;
            if( _callbacks.TryGetValue( entriesById.Key, out subscribers ) )
            {
               entriesForSubscriber = entriesById.ToList();
               foreach( var callback in subscribers )
               {
                  _taskFactory.StartNew( () => callback( entriesForSubscriber ) ); // dont wait for this
               }
            }

            // then handle all
            foreach( var callback in _allCallbacks )
            {
               if( entriesForSubscriber == null )
               {
                  entriesForSubscriber = entriesById.ToList();
               }
               callback.Key( entriesForSubscriber );
            }
         }

         return _completed;
      }

      protected void OnPublishedById( List<IEntry> entries )
      {
         var id = entries[ 0 ].GetId();

         HashSet<Action<List<IEntry>>> subscribers;
         if( _callbacks.TryGetValue( id, out subscribers ) )
         {
            foreach( var callback in subscribers )
            {
               _taskFactory.StartNew( () => callback( entries ) ); // dont wait for this
            }
         }
      }
   }
}
