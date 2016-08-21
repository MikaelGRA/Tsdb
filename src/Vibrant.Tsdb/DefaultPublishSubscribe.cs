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
      private IDictionary<TKey, List<Action<Serie<TKey, TEntry>>>> _latestCallbacksForSingle;
      private List<Action<Serie<TKey, TEntry>>> _latestCallbacksForAll;
      private IDictionary<TKey, List<Action<Serie<TKey, TEntry>>>> _allCallbacksForSingle;
      private List<Action<Serie<TKey, TEntry>>> _allCallbacksForAll;
      private IDictionary<TKey, DateTimeRef> _latest;
      private readonly TaskFactory _taskFactory;
      private bool _continueOnCapturedSynchronizationContext;

      public DefaultPublishSubscribe( bool continueOnCapturedSynchronizationContext )
      {
         _latestCallbacksForSingle = new ConcurrentDictionary<TKey, List<Action<Serie<TKey, TEntry>>>>();
         _latestCallbacksForAll = new List<Action<Serie<TKey, TEntry>>>();
         _allCallbacksForSingle = new ConcurrentDictionary<TKey, List<Action<Serie<TKey, TEntry>>>>();
         _allCallbacksForAll = new List<Action<Serie<TKey, TEntry>>>();
         _latest = new ConcurrentDictionary<TKey, DateTimeRef>();

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

      public async Task<Func<Task>> SubscribeAsync( IEnumerable<TKey> ids, SubscriptionType subscribe, Action<Serie<TKey, TEntry>> callback )
      {
         IDictionary<TKey, List<Action<Serie<TKey, TEntry>>>> single;
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

      private async Task Unsubscribe( IEnumerable<TKey> ids, SubscriptionType subscribe, Action<Serie<TKey, TEntry>> callback )
      {
         IDictionary<TKey, List<Action<Serie<TKey, TEntry>>>> single;
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

      public async Task<Func<Task>> SubscribeToAllAsync( SubscriptionType subscribe, Action<Serie<TKey, TEntry>> callback )
      {
         List<Action<Serie<TKey, TEntry>>> all;
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

      public async Task UnsubscribeFromAll( Action<Serie<TKey, TEntry>> callback, SubscriptionType subscribe )
      {
         List<Action<Serie<TKey, TEntry>>> all;
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
         Action<Serie<TKey, TEntry>> callback,
         IDictionary<TKey, List<Action<Serie<TKey, TEntry>>>> single )
      {
         List<TKey> newSubscriptions = new List<TKey>();
         lock( single )
         {
            foreach( var id in ids )
            {
               List<Action<Serie<TKey, TEntry>>> subscribers;
               if( !single.TryGetValue( id, out subscribers ) )
               {
                  subscribers = new List<Action<Serie<TKey, TEntry>>>();
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
         Action<Serie<TKey, TEntry>> callback,
         IDictionary<TKey, List<Action<Serie<TKey, TEntry>>>> single )
      {
         List<TKey> subscriptionsRemoved = new List<TKey>();
         lock( single )
         {
            foreach( var id in ids )
            {
               List<Action<Serie<TKey, TEntry>>> subscribers;
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

      protected virtual Task OnPublished( IEnumerable<ISerie<TKey, TEntry>> series, PublicationType publish )
      {
         IEnumerable<Serie<TKey, TEntry>> latest = null;
         if( publish.HasFlag( PublicationType.LatestPerCollection ) )
         {
            latest = FindLatestForEachId( series );
         }

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

         return _completed;
      }

      private void PublishForLatest( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         PublishFor( series, _latestCallbacksForSingle, _latestCallbacksForAll, false );
      }

      private void PublishForAll( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         PublishFor( series, _allCallbacksForSingle, _allCallbacksForAll, true );
      }

      private void PublishFor( IEnumerable<ISerie<TKey, TEntry>> series, IDictionary<TKey, List<Action<Serie<TKey, TEntry>>>> single, List<Action<Serie<TKey, TEntry>>> all, bool createNew )
      {
         // FIXME: Is this approach alright?
         foreach( var serie in series )
         {
            if( serie.GetEntries().Count > 0 )
            {
               Serie<TKey, TEntry> data = null;

               List<Action<Serie<TKey, TEntry>>> subscribers;
               if( single.TryGetValue( serie.GetKey(), out subscribers ) )
               {
                  if( data == null )
                  {
                     data = createNew ? new Serie<TKey, TEntry>( serie.GetKey(), serie.GetEntries() ) : (Serie<TKey, TEntry>)serie;
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
               lock( all )
               {
                  foreach( var callback in all )
                  {
                     if( data == null )
                     {
                        data = createNew ? new Serie<TKey, TEntry>( serie.GetKey(), serie.GetEntries() ) : (Serie<TKey, TEntry>)serie;
                     }

                     try
                     {
                        callback( data );
                     }
                     catch( Exception )
                     {

                     }
                  }
               }
            }
         }
      }

      protected void PublishToSingleForLatestEntriesWithSameId( Serie<TKey, TEntry> serie )
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

      protected void PublishToSingleForAllEntriesWithSameId( Serie<TKey, TEntry> serie )
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

      protected void PublishToAllForLatestEntriesWithSameId( Serie<TKey, TEntry> serie )
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

      protected void PublishToAllForAllEntriesWithSameId( Serie<TKey, TEntry> serie )
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

      private void PublishToSingleForEntriesWithSameId( Serie<TKey, TEntry> serie, IDictionary<TKey, List<Action<Serie<TKey, TEntry>>>> single, List<Action<Serie<TKey, TEntry>>> all )
      {
         if( serie.GetEntries().Count > 0 )
         {
            var id = serie.GetKey();
            List<Action<Serie<TKey, TEntry>>> subscribers;
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
      }

      private void PublishToAllForEntriesWithSameId( Serie<TKey, TEntry> serie, IDictionary<TKey, List<Action<Serie<TKey, TEntry>>>> single, List<Action<Serie<TKey, TEntry>>> all )
      {
         if( serie.GetEntries().Count > 0 )
         {
            lock( all )
            {
               foreach( var callback in all )
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
      }

      protected IEnumerable<Serie<TKey, TEntry>> FindLatestForEachId( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         var foundSeries = new Dictionary<TKey, Serie<TKey, TEntry>>();
         foreach( var serie in series )
         {
            var key = serie.GetKey();

            Serie<TKey, TEntry> existingSerie;
            if( !foundSeries.TryGetValue( key, out existingSerie ) )
            {
               existingSerie = new Serie<TKey, TEntry>( key );
            }

            DateTimeRef latest;
            if( !_latest.TryGetValue( key, out latest ) )
            {
               latest = new DateTimeRef();
               _latest[ key ] = latest;
            }

            foreach( var entry in serie.GetEntries() )
            {
               var timestamp = entry.GetTimestamp();
               if( existingSerie.Entries.Count == 0 )
               {
                  if( timestamp > latest.Value )
                  {
                     latest.Value = timestamp;
                     existingSerie.Entries.Add( entry );
                  }
               }
               else
               {
                  if( timestamp > existingSerie.Entries[ 0 ].GetTimestamp() )
                  {
                     existingSerie.Entries[ 0 ] = entry;
                  }
               }
            }

            if( existingSerie.Entries.Count > 0 )
            {
               foundSeries[ key ] = existingSerie;
            }
         }

         return foundSeries.Values;
      }

      private class DateTimeRef
      {
         public DateTimeRef()
         {
            Value = DateTime.MinValue;
         }

         internal DateTime Value;
      }
   }
}
