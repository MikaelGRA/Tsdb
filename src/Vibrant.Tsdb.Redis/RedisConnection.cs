using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using StackExchange.Redis;
using Vibrant.Tsdb.Ats.Serialization;

namespace Vibrant.Tsdb.Redis
{
   internal class RedisConnection : IDisposable
   {
      public event Action<Exception> ConnectionFailed;
      public event Action<Exception> ErrorMessage;

      private LoadedLuaScript _publishLatestScript;
      private ISubscriber _redisSubscriber;
      private ConnectionMultiplexer _connection;

      public async Task ConnectAsync( string connectionString )
      {
         _connection = await ConnectionMultiplexer.ConnectAsync( connectionString ).ConfigureAwait( false );

         _connection.ConnectionFailed += OnConnectionFailed;
         _connection.ConnectionRestored += OnConnectionRestored;
         _connection.ErrorMessage += OnError;
         _redisSubscriber = _connection.GetSubscriber();

         // create all scripts
         await CreateScriptsOnAllServers().ConfigureAwait( false );
      }

      private static string GetServerHostnameAndPort( EndPoint endpoint )
      {
         var dns = endpoint as DnsEndPoint;
         if( dns != null )
         {
            if( dns.Port == 0 ) return dns.Host;
            return dns.Host + ":" + dns.Port.ToString( CultureInfo.InvariantCulture );
         }
         var ip = endpoint as IPEndPoint;
         if( ip != null )
         {
            if( ip.Port == 0 ) return ip.Address.ToString();
            return ip.Address.ToString() + ":" + dns.Port.ToString( CultureInfo.InvariantCulture );
         }
         return endpoint?.ToString() ?? "";
      }

      public void Close( bool allowCommandsToComplete = true )
      {
         if( _redisSubscriber != null )
         {
            _redisSubscriber.UnsubscribeAll();
         }

         if( _connection != null )
         {
            _connection.Close( allowCommandsToComplete );
         }

         _connection.Dispose();
      }

      public Task SubscribeAsync<TKey, TEntry>( string id, IKeyConverter<TKey> keyConverter, SubscriptionType subscribe, Action<List<TEntry>> onMessage )
         where TEntry : IRedisEntry<TKey>, new()
      {
         var key = CreateSubscriptionKey( id, subscribe );
         return _redisSubscriber.SubscribeAsync( key, ( channel, data ) =>
         {
            var entries = RedisSerializer.Deserialize<TKey, TEntry>( data, keyConverter );
            onMessage( entries );
         } );
      }

      public Task UnsubscribeAsync( string id, SubscriptionType subscribe )
      {
         var key = CreateSubscriptionKey( id, subscribe );
         return _redisSubscriber.UnsubscribeAsync( key );
      }

      public Task PublishLatestAsync<TKey, TEntry>( string id, TEntry entry )
         where TEntry : IRedisEntry<TKey>
      {
         if( _connection == null )
         {
            throw new InvalidOperationException( "The redis connection has not been started." );
         }

         var data = RedisSerializer.Serialize<TKey, TEntry>( id, entry );


         var key = CreateSubscriptionKey( id, SubscriptionType.LatestPerCollection );
         var ticks = entry.GetTimestamp().Ticks;

         return _connection.GetDatabase( 0 )
            .ScriptEvaluateAsync( _publishLatestScript.Hash, new RedisKey[] { key }, new RedisValue[] { ticks, data } );
      }

      public Task PublishAllAsync<TKey, TEntry>( string id, IEnumerable<TEntry> entries )
         where TEntry : IRedisEntry<TKey>
      {
         if( _connection == null )
         {
            throw new InvalidOperationException( "The redis connection has not been started." );
         }

         var key = CreateSubscriptionKey( id, SubscriptionType.AllFromCollections );
         var arrays = RedisSerializer.Serialize<TKey, TEntry>( id, entries.ToList(), 64 * 1024 );
         var tasks = new List<Task>();
         foreach( var data in arrays )
         {
            tasks.Add( _redisSubscriber.PublishAsync( key, data ) );
         }
         return Task.WhenAll( tasks );
      }

      public async Task CreateScriptsOnAllServers()
      {
         var luaScript = LuaScript.Prepare( Lua.PublishLatest );
         foreach( var endpoint in _connection.GetEndPoints() )
         {
            var hostname = GetServerHostnameAndPort( endpoint );
            var server = _connection.GetServer( hostname );
            _publishLatestScript = await luaScript.LoadAsync( server ).ConfigureAwait( false );
         }
      }

      private string CreateSubscriptionKey( string id, SubscriptionType subscribe )
      {
         if( subscribe == SubscriptionType.AllFromCollections )
         {
            return id + "|A";
         }
         else if( subscribe == SubscriptionType.LatestPerCollection )
         {
            return id + "|L";
         }
         else
         {
            throw new ArgumentException( nameof( subscribe ) );
         }
      }

      private void OnConnectionFailed( object sender, ConnectionFailedEventArgs args )
      {
         var handler = ConnectionFailed;
         handler( args.Exception );
      }

      private async void OnConnectionRestored( object sender, ConnectionFailedEventArgs args )
      {
         // Workaround for StackExchange.Redis/issues/61 that sometimes Redis connection is not connected in ConnectionRestored event 
         while( !_connection.GetDatabase( 0 ).IsConnected( "someKey" ) )
         {
            await Task.Delay( 200 ).ConfigureAwait( false );
         }

         await CreateScriptsOnAllServers().ConfigureAwait( false );
      }

      private void OnError( object sender, RedisErrorEventArgs args )
      {
         var handler = ErrorMessage;
         handler( new InvalidOperationException( args.Message ) );
      }

      public void Dispose()
      {
         if( _connection != null )
         {
            _connection.Dispose();
         }
      }
   }
}
