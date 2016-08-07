using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Redis
{
   public class RedisPublishSubscribe<TEntry> : DefaultPublishSubscribe<TEntry>, IDisposable
      where TEntry : IRedisEntry, new()
   {
      private TaskCompletionSource<bool> _waitWhileDisconnected;
      private RedisConnection _connection;
      private string _connectionString;
      private int _state;

      public RedisPublishSubscribe( string connectionString, bool continueOnCapturedSynchronizationContext )
         : base( continueOnCapturedSynchronizationContext )
      {
         _connectionString = connectionString;
         _connection = new RedisConnection();
         _waitWhileDisconnected = new TaskCompletionSource<bool>();

         ReconnectDelay = TimeSpan.FromSeconds( 2 );

         ThreadPool.QueueUserWorkItem( _ =>
         {
            var ignore = ConnectWithRetry();
         } );
      }

      public TimeSpan ReconnectDelay { get; set; }

      private void Shutdown()
      {
         if( _connection != null )
         {
            _connection.Close( allowCommandsToComplete: false );
         }

         Interlocked.Exchange( ref _state, State.Disposed );
      }

      private void OnConnectionFailed( Exception ex )
      {
         Interlocked.Exchange( ref _state, State.Closed );
      }

      private void OnConnectionRestored( Exception ex )
      {
         Interlocked.Exchange( ref _state, State.Connected );
      }

      private void OnConnectionError( Exception ex )
      {
         // simply log
      }

      public override Task WaitWhileDisconnected()
      {
         return _waitWhileDisconnected.Task;
      }

      protected override async Task OnPublished( IEnumerable<TEntry> entries, PublicationType publish )
      {
         var tasks = new List<Task>();
         if( publish.HasFlag( PublicationType.LatestPerCollection ) )
         {
            var latest = FindLatestForEachId( entries );
            foreach( var entry in entries )
            {
               var id = entry.GetId();
               tasks.Add( _connection.PublishLatestAsync( id, entry ) );
            }
         }
         if( publish.HasFlag( PublicationType.AllFromCollections ) )
         {
            foreach( var entriesById in entries.GroupBy( x => x.GetId() ) )
            {
               var id = entriesById.Key;
               tasks.Add( _connection.PublishAllAsync( id, entriesById ) );
            }
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      protected override Task OnSubscribed( IEnumerable<string> ids, SubscriptionType subscribe )
      {
         List<Task> tasks = new List<Task>();
         foreach( var id in ids )
         {
            switch( subscribe )
            {
               case SubscriptionType.LatestPerCollection:
                  tasks.Add( _connection.SubscribeAsync<TEntry>( id, subscribe, PublishToSingleForLatestEntriesWithSameId ) );
                  break;
               case SubscriptionType.AllFromCollections:
                  tasks.Add( _connection.SubscribeAsync<TEntry>( id, subscribe, PublishToSingleForAllEntriesWithSameId ) );
                  break;
               default:
                  throw new ArgumentException( nameof( subscribe ) );
            }
         }
         return Task.WhenAll( tasks );
      }

      protected override Task OnUnsubscribed( IEnumerable<string> ids, SubscriptionType subscribe )
      {
         List<Task> tasks = new List<Task>();
         foreach( var id in ids )
         {
            tasks.Add( _connection.UnsubscribeAsync( id, subscribe ) );
         }
         return Task.WhenAll( tasks );
      }

      protected override Task OnSubscribedToAll( SubscriptionType subscribe )
      {
         switch( subscribe )
         {
            case SubscriptionType.LatestPerCollection:
               return _connection.SubscribeAsync<TEntry>( "*", subscribe, PublishToAllForLatestEntriesWithSameId );
            case SubscriptionType.AllFromCollections:
               return _connection.SubscribeAsync<TEntry>( "*", subscribe, PublishToAllForAllEntriesWithSameId );
            default:
               throw new ArgumentException( nameof( subscribe ) );
         }
      }

      protected override Task OnUnsubscribedFromAll( SubscriptionType subscribe )
      {
         return _connection.UnsubscribeAsync( "*", subscribe );
      }

      internal async Task ConnectWithRetry()
      {
         while( true )
         {
            try
            {
               await ConnectToRedisAsync().ConfigureAwait( false );

               var oldState = Interlocked.CompareExchange( ref _state, State.Connected, State.Closed );

               if( oldState == State.Closed )
               {
                  _waitWhileDisconnected.SetResult( true );
               }
               else
               {
                  Shutdown();
               }

               break;
            }
            catch( Exception )
            {
               // Error connecting to redis
            }

            if( _state == State.Disposing )
            {
               Shutdown();
               break;
            }

            await Task.Delay( ReconnectDelay ).ConfigureAwait( false );
         }
      }

      private async Task ConnectToRedisAsync()
      {
         if( _connection != null )
         {
            _connection.ErrorMessage -= OnConnectionError;
            _connection.ConnectionFailed -= OnConnectionFailed;
         }

         await _connection.ConnectAsync( _connectionString ).ConfigureAwait( false );
         
         _connection.ErrorMessage += OnConnectionError;
         _connection.ConnectionFailed += OnConnectionFailed;
      }

      internal static class State
      {
         public const int Closed = 0;
         public const int Connected = 1;
         public const int Disposing = 2;
         public const int Disposed = 3;
      }

      #region IDisposable Support

      private bool _disposed = false;

      protected virtual void Dispose( bool disposing )
      {
         if( !_disposed )
         {
            if( disposing )
            {
               var oldState = Interlocked.Exchange( ref _state, State.Disposing );

               switch( oldState )
               {
                  case State.Connected:
                     Shutdown();
                     break;
                  case State.Closed:
                  case State.Disposing:
                     // No-op
                     break;
                  case State.Disposed:
                     Interlocked.Exchange( ref _state, State.Disposed );
                     break;
                  default:
                     break;
               }
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
