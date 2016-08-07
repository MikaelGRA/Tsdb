using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   /// <summary>
   /// Simple publish subscribe implementation that only works locally.
   /// </summary>
   public class DefaultPublishSubscribe<TEntry> : IPublishSubscribe<TEntry>
      where TEntry : IEntry
   {
      private Task _completed = Task.FromResult( 0 );
      private IDictionary<string, HashSet<Action<List<TEntry>>>> _latestCallbacksForSingle;
      private IDictionary<Action<List<TEntry>>, byte> _latestCallbacksForAll;
      private IDictionary<string, HashSet<Action<List<TEntry>>>> _allCallbacksForSingle;
      private IDictionary<Action<List<TEntry>>, byte> _allCallbacksForAll;
      private IDictionary<string, DateTime> _latest;
      private readonly TaskFactory _taskFactory;
      private bool _continueOnCapturedSynchronizationContext;

      public DefaultPublishSubscribe( bool continueOnCapturedSynchronizationContext )
      {
         _latestCallbacksForSingle = new ConcurrentDictionary<string, HashSet<Action<List<TEntry>>>>();
         _latestCallbacksForAll = new ConcurrentDictionary<Action<List<TEntry>>, byte>();
         _allCallbacksForSingle = new ConcurrentDictionary<string, HashSet<Action<List<TEntry>>>>();
         _allCallbacksForAll = new ConcurrentDictionary<Action<List<TEntry>>, byte>();
         _latest = new ConcurrentDictionary<string, DateTime>();

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
               _continueOnCapturedSynchronizationContext = true;
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

      public async Task Publish( IEnumerable<TEntry> entries, PublicationType publish )
      {
         if( publish == PublicationType.None )
         {
            return;
         }

         await OnPublished( entries, publish ).ConfigureAwait( false );
      }

      public async Task<Func<Task>> Subscribe( IEnumerable<string> ids, SubscriptionType subscribe, Action<List<TEntry>> callback )
      {
         IDictionary<string, HashSet<Action<List<TEntry>>>> single;
         switch( subscribe )
         {
            case SubscriptionType.LatestPerCollection:
               single = _latestCallbacksForSingle;
               break;
            case SubscriptionType.AllFromCollections:
               single = _allCallbacksForSingle;
               break;
            default:
               throw new ArgumentException( nameof( subscribe ) );
         }

         var newSubscriptions = AddSubscriptionsToLatest( ids, callback, single );
         if( newSubscriptions.Count > 0 )
         {
            try
            {
               await OnSubscribed( newSubscriptions, subscribe ).ConfigureAwait( false );
            }
            catch( Exception )
            {
               RemoveSubscriptionsFromLatest( ids, callback, single );

               throw;
            }
         }

         return () => Unsubscribe( ids, subscribe, callback );
      }

      private async Task Unsubscribe( IEnumerable<string> ids, SubscriptionType subscribe, Action<List<TEntry>> callback )
      {
         IDictionary<string, HashSet<Action<List<TEntry>>>> single;
         switch( subscribe )
         {
            case SubscriptionType.LatestPerCollection:
               single = _latestCallbacksForSingle;
               break;
            case SubscriptionType.AllFromCollections:
               single = _allCallbacksForSingle;
               break;
            default:
               throw new ArgumentException( nameof( subscribe ) );
         }

         var removedSubscriptions = RemoveSubscriptionsFromLatest( ids, callback, single );
         if( removedSubscriptions.Count > 0 )
         {
            try
            {
               await OnUnsubscribed( removedSubscriptions, subscribe ).ConfigureAwait( false );
            }
            catch( Exception )
            {
               AddSubscriptionsToLatest( ids, callback, single );

               throw;
            }
         }
      }

      public async Task<Func<Task>> SubscribeToAll( SubscriptionType subscribe, Action<List<TEntry>> callback )
      {
         IDictionary<Action<List<TEntry>>, byte> all;
         switch( subscribe )
         {
            case SubscriptionType.LatestPerCollection:
               all = _latestCallbacksForAll;
               break;
            case SubscriptionType.AllFromCollections:
               all = _allCallbacksForAll;
               break;
            default:
               throw new ArgumentException( nameof( subscribe ) );
         }

         bool subscribeToAll = false;
         lock( all )
         {
            if( all.Count == 0 )
            {
               subscribeToAll = true;
            }

            all.Add( callback, 0 );
         }

         if( subscribeToAll )
         {
            try
            {
               await OnSubscribedToAll( subscribe ).ConfigureAwait( false );
            }
            catch( Exception )
            {
               lock( all )
               {
                  all.Remove( callback );
               }

               throw;
            }
         }

         return () => UnsubscribeFromAll( callback, subscribe );
      }

      public async Task UnsubscribeFromAll( Action<List<TEntry>> callback, SubscriptionType subscribe )
      {
         IDictionary<Action<List<TEntry>>, byte> all;
         switch( subscribe )
         {
            case SubscriptionType.LatestPerCollection:
               all = _latestCallbacksForAll;
               break;
            case SubscriptionType.AllFromCollections:
               all = _allCallbacksForAll;
               break;
            default:
               throw new ArgumentException( nameof( subscribe ) );
         }

         bool unsubscribeFromAll = false;
         lock( all )
         {
            all.Remove( callback );

            if( all.Count == 0 )
            {
               unsubscribeFromAll = true;
            }
         }

         if( unsubscribeFromAll )
         {
            try
            {
               await OnUnsubscribedFromAll( subscribe ).ConfigureAwait( false );
            }
            catch( Exception )
            {
               lock( all )
               {
                  all.Add( callback, 0 );
               }

               throw;
            }
         }
      }

      private List<string> AddSubscriptionsToLatest(
         IEnumerable<string> ids,
         Action<List<TEntry>> callback,
         IDictionary<string, HashSet<Action<List<TEntry>>>> single )
      {
         List<string> newSubscriptions = new List<string>();
         lock( single )
         {
            foreach( var id in ids )
            {
               HashSet<Action<List<TEntry>>> subscribers;
               if( !single.TryGetValue( id, out subscribers ) )
               {
                  subscribers = new HashSet<Action<List<TEntry>>>();
                  single.Add( id, subscribers );
                  newSubscriptions.Add( id );
               }

               subscribers.Add( callback );
            }
         }
         return newSubscriptions;
      }

      private List<string> RemoveSubscriptionsFromLatest(
         IEnumerable<string> ids,
         Action<List<TEntry>> callback,
         IDictionary<string, HashSet<Action<List<TEntry>>>> single )
      {
         List<string> subscriptionsRemoved = new List<string>();
         lock( single )
         {
            foreach( var id in ids )
            {
               HashSet<Action<List<TEntry>>> subscribers;
               if( single.TryGetValue( id, out subscribers ) )
               {
                  subscribers.Remove( callback );

                  if( subscribers.Count == 0 )
                  {
                     single.Remove( id );
                     subscriptionsRemoved.Add( id );
                  }
               }
            }
         }
         return subscriptionsRemoved;
      }

      protected virtual Task OnSubscribed( IEnumerable<string> ids, SubscriptionType subscribe )
      {
         return _completed;
      }

      protected virtual Task OnUnsubscribed( IEnumerable<string> ids, SubscriptionType subscribe )
      {
         return _completed;
      }

      protected virtual Task OnSubscribedToAll( SubscriptionType subscribe )
      {
         return _completed;
      }

      protected virtual Task OnUnsubscribedFromAll( SubscriptionType subscribe )
      {
         return _completed;
      }

      protected virtual Task OnPublished( IEnumerable<TEntry> entries, PublicationType publish )
      {
         IEnumerable<TEntry> latest = null;
         if( publish.HasFlag( PublicationType.LatestPerCollection ) )
         {
            latest = FindLatestForEachId( entries );
         }

         _taskFactory.StartNew( () =>
         {
            if( publish.HasFlag( PublicationType.LatestPerCollection ) )
            {
               PublishForLatest( latest );
            }
            if( publish.HasFlag( PublicationType.AllFromCollections ) )
            {
               PublishForAll( entries );
            }
         } );

         return _completed;
      }

      private void PublishForLatest( IEnumerable<TEntry> entries )
      {
         PublishFor( entries, _latestCallbacksForSingle, _latestCallbacksForAll );
      }

      private void PublishForAll( IEnumerable<TEntry> entries )
      {
         PublishFor( entries, _allCallbacksForSingle, _allCallbacksForAll );
      }

      private void PublishFor( IEnumerable<TEntry> entries, IDictionary<string, HashSet<Action<List<TEntry>>>> single, IDictionary<Action<List<TEntry>>, byte> all )
      {
         foreach( var entriesById in entries.GroupBy( x => x.GetId() ) )
         {
            List<TEntry> entriesForSubscriber = null;

            HashSet<Action<List<TEntry>>> subscribers;
            if( single.TryGetValue( entriesById.Key, out subscribers ) )
            {
               entriesForSubscriber = entriesById.ToList();
               foreach( var callback in subscribers )
               {
                  try
                  {
                     callback( entriesForSubscriber );
                  }
                  catch( Exception )
                  {

                  }
               }
            }

            // then handle all
            foreach( var callback in all )
            {
               if( entriesForSubscriber == null )
               {
                  entriesForSubscriber = entriesById.ToList();
               }
               try
               {
                  callback.Key( entriesForSubscriber );
               }
               catch( Exception )
               {

               }
            }
         }
      }

      protected void PublishToSingleForLatestEntriesWithSameId( List<TEntry> entries )
      {
         if( _continueOnCapturedSynchronizationContext )
         {
            _taskFactory.StartNew( () => PublishToSingleForEntriesWithSameId( entries, _latestCallbacksForSingle, _latestCallbacksForAll ) );
         }
         else
         {
            PublishToSingleForEntriesWithSameId( entries, _latestCallbacksForSingle, _latestCallbacksForAll );
         }
      }

      protected void PublishToSingleForAllEntriesWithSameId( List<TEntry> entries )
      {
         if( _continueOnCapturedSynchronizationContext )
         {
            _taskFactory.StartNew( () => PublishToSingleForEntriesWithSameId( entries, _allCallbacksForSingle, _allCallbacksForAll ) );
         }
         else
         {
            PublishToSingleForEntriesWithSameId( entries, _allCallbacksForSingle, _allCallbacksForAll );
         }
      }

      protected void PublishToAllForLatestEntriesWithSameId( List<TEntry> entries )
      {
         if( _continueOnCapturedSynchronizationContext )
         {
            _taskFactory.StartNew( () => PublishToAllForEntriesWithSameId( entries, _latestCallbacksForSingle, _latestCallbacksForAll ) );
         }
         else
         {
            PublishToAllForEntriesWithSameId( entries, _latestCallbacksForSingle, _latestCallbacksForAll );
         }
      }

      protected void PublishToAllForAllEntriesWithSameId( List<TEntry> entries )
      {
         if( _continueOnCapturedSynchronizationContext )
         {
            _taskFactory.StartNew( () => PublishToAllForEntriesWithSameId( entries, _allCallbacksForSingle, _allCallbacksForAll ) );
         }
         else
         {
            PublishToAllForEntriesWithSameId( entries, _allCallbacksForSingle, _allCallbacksForAll );
         }
      }

      private void PublishToSingleForEntriesWithSameId( List<TEntry> entries, IDictionary<string, HashSet<Action<List<TEntry>>>> single, IDictionary<Action<List<TEntry>>, byte> all )
      {
         var id = entries[ 0 ].GetId();

         HashSet<Action<List<TEntry>>> subscribers;
         if( single.TryGetValue( id, out subscribers ) )
         {
            foreach( var callback in subscribers )
            {
               try
               {
                  callback( entries );
               }
               catch( Exception )
               {

               }
            }
         }
      }

      private void PublishToAllForEntriesWithSameId( List<TEntry> entries, IDictionary<string, HashSet<Action<List<TEntry>>>> single, IDictionary<Action<List<TEntry>>, byte> all )
      {
         foreach( var callback in all )
         {
            try
            {
               callback.Key( entries );
            }
            catch( Exception )
            {

            }
         }
      }

      protected IEnumerable<TEntry> FindLatestForEachId( IEnumerable<TEntry> entries )
      {
         var foundEntries = new Dictionary<string, TEntry>();
         foreach( var entry in entries )
         {
            var id = entry.GetId();

            TEntry existingEntry;
            if( !foundEntries.TryGetValue( id, out existingEntry ) )
            {
               DateTime currentLatest;
               if( _latest.TryGetValue( id, out currentLatest ) )
               {
                  var timestamp = entry.GetTimestamp();
                  if( timestamp > currentLatest )
                  {
                     foundEntries.Add( id, entry );
                     _latest[ id ] = timestamp;
                  }
               }
               else
               {
                  foundEntries.Add( id, entry );
                  _latest[ id ] = entry.GetTimestamp();
               }
            }
            else
            {
               if( entry.GetTimestamp() > existingEntry.GetTimestamp() )
               {
                  foundEntries[ id ] = entry;
               }
            }
         }

         return foundEntries.Values;
      }
   }
}
