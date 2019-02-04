using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Redis
{
   public class RedisPublishSubscribe<TKey, TEntry> : DefaultPublishSubscribe<TKey, TEntry>, IDisposable
      where TEntry : IRedisEntry, new()
   {
      private TaskCompletionSource<bool> _waitWhileDisconnected;
      private RedisConnection _connection;
      private int _state;
      private IKeyConverter<TKey> _keyConverter;
      private ITsdbLogger _logger;

      public RedisPublishSubscribe( string connectionString, string prefix, ITsdbLogger logger, IKeyConverter<TKey> keyConverter )
         : base( false )
      {
         _connection = new RedisConnection( connectionString, prefix, logger );
         _waitWhileDisconnected = new TaskCompletionSource<bool>();
         _keyConverter = keyConverter;
         _logger = logger;

         ThreadPool.QueueUserWorkItem( _ =>
         {
            var ignore = ConnectWithRetry();
         } );
      }

      public RedisPublishSubscribe( string connectionString, string prefix, ITsdbLogger logger )
         : this( connectionString, prefix, logger, DefaultKeyConverter<TKey>.Current )
      {
      }

      public RedisPublishSubscribe( string connectionString, string prefix )
         : this( connectionString, prefix, new NullTsdbLogger(), DefaultKeyConverter<TKey>.Current )
      {
      }

      public override Task WaitWhileDisconnectedAsync()
      {
         return _waitWhileDisconnected.Task;
      }

      protected override async Task OnPublished( IEnumerable<ISortedSerie<TKey, TEntry>> series, PublicationType publish )
      {
         await WaitWhileDisconnectedAsync().ConfigureAwait( false );

         var tasks = new List<Task>();
         if( publish.HasFlag( PublicationType.LatestPerCollection ) )
         {
            var latest = FindLatestForEachId( series );
            foreach( var serie in latest )
            {
               if( serie.GetEntries().Count > 0 )
               {
                  var key = serie.GetKey();
                  var entry = serie.GetEntries().First();
                  tasks.Add( _connection.PublishLatestAsync<TKey, TEntry>( _keyConverter.Convert( key ), entry ) );
               }
            }
         }
         if( publish.HasFlag( PublicationType.AllFromCollections ) )
         {
            foreach( var serie in series )
            {
               if( serie.GetEntries().Count > 0 )
               {
                  var key = serie.GetKey();
                  tasks.Add( _connection.PublishAllAsync<TKey, TEntry>( _keyConverter.Convert( key ), serie.GetEntries() ) );
               }
            }
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      protected override async Task OnSubscribed( IEnumerable<TKey> ids, SubscriptionType subscribe )
      {
         await WaitWhileDisconnectedAsync().ConfigureAwait( false );

         List<Task> tasks = new List<Task>();
         foreach( var id in ids )
         {
            switch( subscribe )
            {
               case SubscriptionType.LatestPerCollection:
                  tasks.Add( _connection.SubscribeAsync<TKey, TEntry>( _keyConverter.Convert( id ), _keyConverter, subscribe, PublishToSingleForLatestEntriesWithSameId ) );
                  break;
               case SubscriptionType.AllFromCollections:
                  tasks.Add( _connection.SubscribeAsync<TKey, TEntry>( _keyConverter.Convert( id ), _keyConverter, subscribe, PublishToSingleForAllEntriesWithSameId ) );
                  break;
               default:
                  throw new ArgumentException( nameof( subscribe ) );
            }
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      protected override Task OnUnsubscribed( IEnumerable<TKey> ids, SubscriptionType subscribe )
      {
         List<Task> tasks = new List<Task>();
         foreach( var id in ids )
         {
            tasks.Add( _connection.UnsubscribeAsync( _keyConverter.Convert( id ), subscribe ) );
         }
         return Task.WhenAll( tasks );
      }

      protected override async Task OnSubscribedToAll( SubscriptionType subscribe )
      {
         await WaitWhileDisconnectedAsync().ConfigureAwait( false );

         switch( subscribe )
         {
            case SubscriptionType.LatestPerCollection:
               await _connection.SubscribeAsync<TKey, TEntry>( "*", _keyConverter, subscribe, PublishToAllForLatestEntriesWithSameId ).ConfigureAwait( false );
               return;
            case SubscriptionType.AllFromCollections:
               await _connection.SubscribeAsync<TKey, TEntry>( "*", _keyConverter, subscribe, PublishToAllForAllEntriesWithSameId ).ConfigureAwait( false );
               return;
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


               //  at this point, I know we are connected
               var oldState = Interlocked.CompareExchange( ref _state, RedisConnectionState.Connected, RedisConnectionState.Closed );

               if( oldState == RedisConnectionState.Closed )
               {
                  _waitWhileDisconnected.SetResult( true );
               }
               else
               {
                  Shutdown();
               }

               break;
            }
            catch( Exception e )
            {
               _logger.Error( e, "An error ocurred while connecting to redis." );
            }

            if( _state == RedisConnectionState.Disposing )
            {
               Shutdown();
               break;
            }

            await Task.Delay( TimeSpan.FromSeconds( 2 ) ).ConfigureAwait( false );
         }
      }

      private async Task ConnectToRedisAsync()
      {
         _connection.ErrorMessage -= OnConnectionError;
         _connection.ConnectionFailed -= OnConnectionFailed;

         await _connection.ConnectAsync().ConfigureAwait( false );

         _connection.ErrorMessage += OnConnectionError;
         _connection.ConnectionFailed += OnConnectionFailed;
      }

      private void Shutdown()
      {
         _connection.Close( allowCommandsToComplete: false );

         if( !_waitWhileDisconnected.Task.IsCompleted )
         {
            _waitWhileDisconnected.TrySetCanceled();
         }

         Interlocked.Exchange( ref _state, RedisConnectionState.Disposed );
      }

      private void OnConnectionFailed( Exception ex )
      {
         _logger.Error( ex, "The connection failed in RedisPublishSubscribe." );

         Interlocked.Exchange( ref _state, RedisConnectionState.Closed );
      }

      private void OnConnectionError( Exception ex )
      {
         _logger.Error( ex, "An error ocurred in RedisPublishSubscribe." );
      }

      #region IDisposable Support

      private bool _disposed = false;

      protected virtual void Dispose( bool disposing )
      {
         if( !_disposed )
         {
            if( disposing )
            {
               var oldState = Interlocked.Exchange( ref _state, RedisConnectionState.Disposing );

               switch( oldState )
               {
                  case RedisConnectionState.Connected:
                     Shutdown();
                     break;
                  case RedisConnectionState.Disposed:
                     Interlocked.Exchange( ref _state, RedisConnectionState.Disposed );
                     break;
                  case RedisConnectionState.Closed:
                  case RedisConnectionState.Disposing:
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
