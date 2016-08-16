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
   public class DefaultPublishSubscribe<TKey, TEntry> : IPublishSubscribe<TKey, TEntry>
      where TEntry : IEntry
   {
      private Task _completed = Task.FromResult( 0 );
      private IDictionary<TKey, HashSet<Action<ISerie<TKey, TEntry>>>> _latestCallbacksForSingle;
      private IDictionary<Action<ISerie<TKey, TEntry>>, byte> _latestCallbacksForAll;
      private IDictionary<TKey, HashSet<Action<ISerie<TKey, TEntry>>>> _allCallbacksForSingle;
      private IDictionary<Action<ISerie<TKey, TEntry>>, byte> _allCallbacksForAll;
      private IDictionary<TKey, DateTime> _latest;
      private readonly TaskFactory _taskFactory;
      private bool _continueOnCapturedSynchronizationContext;

      public DefaultPublishSubscribe( bool continueOnCapturedSynchronizationContext )
      {
         _latestCallbacksForSingle = new ConcurrentDictionary<TKey, HashSet<Action<ISerie<TKey, TEntry>>>>();
         _latestCallbacksForAll = new ConcurrentDictionary<Action<ISerie<TKey, TEntry>>, byte>();
         _allCallbacksForSingle = new ConcurrentDictionary<TKey, HashSet<Action<ISerie<TKey, TEntry>>>>();
         _allCallbacksForAll = new ConcurrentDictionary<Action<ISerie<TKey, TEntry>>, byte>();
         _latest = new ConcurrentDictionary<TKey, DateTime>();

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

      public virtual Task WaitWhileDisconnectedAsync()
      {
         return _completed;
      }

      public async Task PublishAsync( IEnumerable<ISerie<TKey, TEntry>> entries, PublicationType publish )
      {
         if( publish == PublicationType.None )
         {
            return;
         }

         await OnPublished( entries, publish ).ConfigureAwait( false );
      }

      public async Task<Func<Task>> SubscribeAsync( IEnumerable<TKey> ids, SubscriptionType subscribe, Action<ISerie<TKey, TEntry>> callback )
      {
         IDictionary<TKey, HashSet<Action<ISerie<TKey, TEntry>>>> single;
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

      private async Task Unsubscribe( IEnumerable<TKey> ids, SubscriptionType subscribe, Action<ISerie<TKey, TEntry>> callback )
      {
         IDictionary<TKey, HashSet<Action<ISerie<TKey, TEntry>>>> single;
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

      public async Task<Func<Task>> SubscribeToAllAsync( SubscriptionType subscribe, Action<ISerie<TKey, TEntry>> callback )
      {
         IDictionary<Action<ISerie<TKey, TEntry>>, byte> all;
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

      public async Task UnsubscribeFromAll( Action<ISerie<TKey, TEntry>> callback, SubscriptionType subscribe )
      {
         IDictionary<Action<ISerie<TKey, TEntry>>, byte> all;
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

      private List<TKey> AddSubscriptionsToLatest(
         IEnumerable<TKey> ids,
         Action<ISerie<TKey, TEntry>> callback,
         IDictionary<TKey, HashSet<Action<ISerie<TKey, TEntry>>>> single )
      {
         List<TKey> newSubscriptions = new List<TKey>();
         lock( single )
         {
            foreach( var id in ids )
            {
               HashSet<Action<ISerie<TKey, TEntry>>> subscribers;
               if( !single.TryGetValue( id, out subscribers ) )
               {
                  subscribers = new HashSet<Action<ISerie<TKey, TEntry>>>();
                  single.Add( id, subscribers );
                  newSubscriptions.Add( id );
               }

               subscribers.Add( callback );
            }
         }
         return newSubscriptions;
      }

      private List<TKey> RemoveSubscriptionsFromLatest(
         IEnumerable<TKey> ids,
         Action<ISerie<TKey, TEntry>> callback,
         IDictionary<TKey, HashSet<Action<ISerie<TKey, TEntry>>>> single )
      {
         List<TKey> subscriptionsRemoved = new List<TKey>();
         lock( single )
         {
            foreach( var id in ids )
            {
               HashSet<Action<ISerie<TKey, TEntry>>> subscribers;
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

      protected virtual Task OnSubscribed( IEnumerable<TKey> ids, SubscriptionType subscribe )
      {
         return _completed;
      }

      protected virtual Task OnUnsubscribed( IEnumerable<TKey> ids, SubscriptionType subscribe )
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

      protected virtual Task OnPublished( IEnumerable<ISerie<TKey, TEntry>> entries, PublicationType publish )
      {
         IEnumerable<ISerie<TKey, TEntry>> latest = null;
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

      private void PublishForLatest( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         PublishFor( series, _latestCallbacksForSingle, _latestCallbacksForAll );
      }

      private void PublishForAll( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         PublishFor( series, _allCallbacksForSingle, _allCallbacksForAll );
      }

      private void PublishFor( IEnumerable<ISerie<TKey, TEntry>> series, IDictionary<TKey, HashSet<Action<ISerie<TKey, TEntry>>>> single, IDictionary<Action<ISerie<TKey, TEntry>>, byte> all )
      {
         foreach( var serie in series.GroupBy( x => x.Key ) )
         {
            var entries = serie.SelectMany( x => x.Entries );
            ISerie<TKey, TEntry> data = null;

            HashSet<Action<ISerie<TKey, TEntry>>> subscribers;
            if( single.TryGetValue( serie.Key, out subscribers ) )
            {
               if( data == null )
               {
                  data = new Serie<TKey, TEntry>( serie.Key, entries.ToList() );
               }

               foreach( var callback in subscribers )
               {
                  try
                  {
                     callback( data );
                  }
                  catch( Exception )
                  {

                  }
               }
            }

            // then handle all
            foreach( var callback in all )
            {
               if( data == null )
               {
                  data = new Serie<TKey, TEntry>( serie.Key, entries.ToList() );
               }

               try
               {
                  callback.Key( data );
               }
               catch( Exception )
               {

               }
            }
         }
      }

      protected void PublishToSingleForLatestEntriesWithSameId( ISerie<TKey, TEntry> serie )
      {
         if( _continueOnCapturedSynchronizationContext )
         {
            _taskFactory.StartNew( () => PublishToSingleForEntriesWithSameId( serie, _latestCallbacksForSingle, _latestCallbacksForAll ) );
         }
         else
         {
            PublishToSingleForEntriesWithSameId( serie, _latestCallbacksForSingle, _latestCallbacksForAll );
         }
      }

      protected void PublishToSingleForAllEntriesWithSameId( ISerie<TKey, TEntry> serie )
      {
         if( _continueOnCapturedSynchronizationContext )
         {
            _taskFactory.StartNew( () => PublishToSingleForEntriesWithSameId( serie, _allCallbacksForSingle, _allCallbacksForAll ) );
         }
         else
         {
            PublishToSingleForEntriesWithSameId( serie, _allCallbacksForSingle, _allCallbacksForAll );
         }
      }

      protected void PublishToAllForLatestEntriesWithSameId( ISerie<TKey, TEntry> serie )
      {
         if( _continueOnCapturedSynchronizationContext )
         {
            _taskFactory.StartNew( () => PublishToAllForEntriesWithSameId( serie, _latestCallbacksForSingle, _latestCallbacksForAll ) );
         }
         else
         {
            PublishToAllForEntriesWithSameId( serie, _latestCallbacksForSingle, _latestCallbacksForAll );
         }
      }

      protected void PublishToAllForAllEntriesWithSameId( ISerie<TKey, TEntry> serie )
      {
         if( _continueOnCapturedSynchronizationContext )
         {
            _taskFactory.StartNew( () => PublishToAllForEntriesWithSameId( serie, _allCallbacksForSingle, _allCallbacksForAll ) );
         }
         else
         {
            PublishToAllForEntriesWithSameId( serie, _allCallbacksForSingle, _allCallbacksForAll );
         }
      }

      private void PublishToSingleForEntriesWithSameId( ISerie<TKey, TEntry> serie, IDictionary<TKey, HashSet<Action<ISerie<TKey, TEntry>>>> single, IDictionary<Action<ISerie<TKey, TEntry>>, byte> all )
      {
         var id = serie.Key;

         HashSet<Action<ISerie<TKey, TEntry>>> subscribers;
         if( single.TryGetValue( id, out subscribers ) )
         {
            foreach( var callback in subscribers )
            {
               try
               {
                  callback( serie );
               }
               catch( Exception )
               {

               }
            }
         }
      }

      private void PublishToAllForEntriesWithSameId( ISerie<TKey, TEntry> serie, IDictionary<TKey, HashSet<Action<ISerie<TKey, TEntry>>>> single, IDictionary<Action<ISerie<TKey, TEntry>>, byte> all )
      {
         foreach( var callback in all )
         {
            try
            {
               callback.Key( serie );
            }
            catch( Exception )
            {

            }
         }
      }

      protected IEnumerable<ISerie<TKey, TEntry>> FindLatestForEachId( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         var foundSeries = new Dictionary<TKey, ISerie<TKey, TEntry>>();
         foreach( var serie in series )
         {
            var id = serie.Key;

            foreach( var entry in serie.Entries )
            {
               ISerie<TKey, TEntry> existingEntry;
               if( !foundSeries.TryGetValue( id, out existingEntry ) )
               {
                  DateTime currentLatest;
                  if( _latest.TryGetValue( id, out currentLatest ) )
                  {
                     var timestamp = entry.GetTimestamp();
                     if( timestamp > currentLatest )
                     {
                        foundSeries.Add( id, new Serie<TKey, TEntry>( id, entry ) );
                        _latest[ id ] = timestamp;
                     }
                  }
                  else
                  {
                     foundSeries.Add( id, new Serie<TKey, TEntry>( id, entry ) );
                     _latest[ id ] = entry.GetTimestamp();
                  }
               }
               else
               {
                  if( entry.GetTimestamp() > existingEntry.Entries[ 0 ].GetTimestamp() )
                  {
                     foundSeries[ id ].Entries[ 0 ] = entry;
                  }
               }
            }
         }

         return foundSeries.Values;
      }
   }
}
