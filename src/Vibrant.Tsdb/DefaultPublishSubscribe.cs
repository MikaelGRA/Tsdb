using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Vibrant.Tsdb.Extensions;

namespace Vibrant.Tsdb
{
   /// <summary>
   /// Simple publish subscribe implementation that only works locally.
   /// </summary>
   public class DefaultPublishSubscribe<TKey, TEntry> : IPublishSubscribe<TKey, TEntry>
      where TEntry : IEntry
   {
      private Task _completed = Task.FromResult( 0 );
      private IDictionary<TKey, List<Action<ISortedSerie<TKey, TEntry>>>> _latestCallbacksForSingle;
      private List<Action<ISortedSerie<TKey, TEntry>>> _latestCallbacksForAll;
      private IDictionary<TKey, List<Action<ISortedSerie<TKey, TEntry>>>> _allCallbacksForSingle;
      private List<Action<ISortedSerie<TKey, TEntry>>> _allCallbacksForAll;
      private readonly TaskFactory _taskFactory;
      private bool _publishOnBackgroundThread;

      public DefaultPublishSubscribe( bool publishOnBackgroundThread )
      {
         _latestCallbacksForSingle = new ConcurrentDictionary<TKey, List<Action<ISortedSerie<TKey, TEntry>>>>();
         _latestCallbacksForAll = new List<Action<ISortedSerie<TKey, TEntry>>>();
         _allCallbacksForSingle = new ConcurrentDictionary<TKey, List<Action<ISortedSerie<TKey, TEntry>>>>();
         _allCallbacksForAll = new List<Action<ISortedSerie<TKey, TEntry>>>();
         _taskFactory = new TaskFactory( TaskScheduler.Default );
         _publishOnBackgroundThread = publishOnBackgroundThread;
      }

      public virtual Task WaitWhileDisconnectedAsync()
      {
         return _completed;
      }

      public async Task PublishAsync( IEnumerable<ISortedSerie<TKey, TEntry>> entries, PublicationType publish )
      {
         if( publish == PublicationType.None )
         {
            return;
         }

         await OnPublished( entries, publish ).ConfigureAwait( false );
      }

      public async Task<Func<Task>> SubscribeAsync( IEnumerable<TKey> ids, SubscriptionType subscribe, Action<ISortedSerie<TKey, TEntry>> callback )
      {
         IDictionary<TKey, List<Action<ISortedSerie<TKey, TEntry>>>> single;
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

      private async Task Unsubscribe( IEnumerable<TKey> ids, SubscriptionType subscribe, Action<ISortedSerie<TKey, TEntry>> callback )
      {
         IDictionary<TKey, List<Action<ISortedSerie<TKey, TEntry>>>> single;
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

      public async Task<Func<Task>> SubscribeToAllAsync( SubscriptionType subscribe, Action<ISortedSerie<TKey, TEntry>> callback )
      {
         List<Action<ISortedSerie<TKey, TEntry>>> all;
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

            all.Add( callback );
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

      private async Task UnsubscribeFromAll( Action<ISortedSerie<TKey, TEntry>> callback, SubscriptionType subscribe )
      {
         List<Action<ISortedSerie<TKey, TEntry>>> all;
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
                  all.Add( callback );
               }

               throw;
            }
         }
      }

      private List<TKey> AddSubscriptionsToLatest(
         IEnumerable<TKey> ids,
         Action<ISortedSerie<TKey, TEntry>> callback,
         IDictionary<TKey, List<Action<ISortedSerie<TKey, TEntry>>>> single )
      {
         List<TKey> newSubscriptions = new List<TKey>();
         lock( single )
         {
            foreach( var id in ids )
            {
               List<Action<ISortedSerie<TKey, TEntry>>> subscribers;
               if( !single.TryGetValue( id, out subscribers ) )
               {
                  subscribers = new List<Action<ISortedSerie<TKey, TEntry>>>();
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
         Action<ISortedSerie<TKey, TEntry>> callback,
         IDictionary<TKey, List<Action<ISortedSerie<TKey, TEntry>>>> single )
      {
         List<TKey> subscriptionsRemoved = new List<TKey>();
         lock( single )
         {
            foreach( var id in ids )
            {
               List<Action<ISortedSerie<TKey, TEntry>>> subscribers;
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

      protected virtual Task OnPublished( IEnumerable<ISortedSerie<TKey, TEntry>> series, PublicationType publish )
      {
         IEnumerable<ISortedSerie<TKey, TEntry>> latest = null;
         if( publish.HasFlag( PublicationType.LatestPerCollection ) )
         {
            latest = FindLatestForEachId( series );
         }

         if( _publishOnBackgroundThread )
         {
            _taskFactory.StartNew( () =>
            {
               if( publish.HasFlag( PublicationType.LatestPerCollection ) )
               {
                  PublishForLatest( latest );
               }
               if( publish.HasFlag( PublicationType.AllFromCollections ) )
               {
                  PublishForAll( series );
               }
            } );
         }
         else
         {
            if( publish.HasFlag( PublicationType.LatestPerCollection ) )
            {
               PublishForLatest( latest );
            }
            if( publish.HasFlag( PublicationType.AllFromCollections ) )
            {
               PublishForAll( series );
            }
         }

         return _completed;
      }

      private void PublishForLatest( IEnumerable<ISortedSerie<TKey, TEntry>> series )
      {
         PublishFor( series, _latestCallbacksForSingle, _latestCallbacksForAll, false );
      }

      private void PublishForAll( IEnumerable<ISortedSerie<TKey, TEntry>> series )
      {
         PublishFor( series, _allCallbacksForSingle, _allCallbacksForAll, true );
      }

      private void PublishFor( IEnumerable<ISortedSerie<TKey, TEntry>> series, IDictionary<TKey, List<Action<ISortedSerie<TKey, TEntry>>>> single, List<Action<ISortedSerie<TKey, TEntry>>> all, bool createNew )
      {
         // TODO: Do not create NEW.... (less references!!!!!)

         
         foreach( var serie in series )
         {
            if( serie.GetEntries().Count > 0 )
            {
               List<Action<ISortedSerie<TKey, TEntry>>> subscribers;
               lock( single )
               {
                  if( single.TryGetValue( serie.GetKey(), out subscribers ) )
                  {
                     for( int i = subscribers.Count - 1 ; 0 <= i ; i-- )
                     {
                        try
                        {
                           subscribers[ i ]( serie );
                        }
                        catch( Exception )
                        {

                        }
                     }
                  }
               }

               // then handle all
               lock( all )
               {
                  for( int i = all.Count - 1 ; 0 <= i ; i-- )
                  {
                     try
                     {
                        all[ i ]( serie );
                     }
                     catch( Exception )
                     {

                     }
                  }
               }
            }
         }
      }

      protected void PublishToSingleForLatestEntriesWithSameId( ISortedSerie<TKey, TEntry> serie )
      {
         PublishToSingleForEntriesWithSameId( serie, _latestCallbacksForSingle );
      }

      protected void PublishToSingleForAllEntriesWithSameId( ISortedSerie<TKey, TEntry> serie )
      {
         PublishToSingleForEntriesWithSameId( serie, _allCallbacksForSingle );
      }

      protected void PublishToAllForLatestEntriesWithSameId( ISortedSerie<TKey, TEntry> serie )
      {
         PublishToAllForEntriesWithSameId( serie, _latestCallbacksForAll );
      }

      protected void PublishToAllForAllEntriesWithSameId( ISortedSerie<TKey, TEntry> serie )
      {
         PublishToAllForEntriesWithSameId( serie, _allCallbacksForAll );
      }

      private void PublishToSingleForEntriesWithSameId( ISortedSerie<TKey, TEntry> serie, IDictionary<TKey, List<Action<ISortedSerie<TKey, TEntry>>>> single )
      {
         if( serie.GetEntries().Count > 0 )
         {
            var id = serie.GetKey();
            List<Action<ISortedSerie<TKey, TEntry>>> subscribers;
            lock( single )
            {
               if( single.TryGetValue( id, out subscribers ) )
               {
                  for( int i = subscribers.Count - 1 ; 0 <= i ; i-- )
                  {
                     try
                     {
                        subscribers[ i ]( serie );
                     }
                     catch( Exception )
                     {

                     }
                  }
               }
            }
         }
      }

      private void PublishToAllForEntriesWithSameId( ISortedSerie<TKey, TEntry> serie, List<Action<ISortedSerie<TKey, TEntry>>> all )
      {
         if( serie.GetEntries().Count > 0 )
         {
            lock( all )
            {
               for( int i = all.Count - 1 ; 0 <= i ; i-- )
               {
                  try
                  {
                     all[ i ]( serie );
                  }
                  catch( Exception )
                  {

                  }
               }
            }
         }
      }

      protected IEnumerable<ISortedSerie<TKey, TEntry>> FindLatestForEachId( IEnumerable<ISortedSerie<TKey, TEntry>> series )
      {
         var foundSeries = new Dictionary<TKey, SortedSerie<TKey, TEntry>>();
         foreach( var serie in series )
         {
            var key = serie.GetKey();

            SortedSerie<TKey, TEntry> existingSerie;
            if( !foundSeries.TryGetValue( key, out existingSerie ) )
            {
               existingSerie = new SortedSerie<TKey, TEntry>( key, serie.GetOrdering() );
               foundSeries.Add( key, existingSerie );
            }

            var latestEntry = serie.GetLatestEntry();


            var timestamp = latestEntry.GetTimestamp();
            if( existingSerie.Entries.Count == 0 )
            {
               existingSerie.Entries.Add( latestEntry );
            }
            else
            {
               if( timestamp > existingSerie.Entries[ 0 ].GetTimestamp() )
               {
                  existingSerie.Entries[ 0 ] = latestEntry;
               }
            }
         }

         return foundSeries.Values;
      }
   }
}
