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

      private ITsdbLogger _logger;
      private LoadedLuaScript _publishLatestScript;
      private ISubscriber _redisSubscriber;
      private ConnectionMultiplexer _connection;
      private string _prefix;
      private string _connectionString;

      public RedisConnection( string connectionString, string prefix, ITsdbLogger logger )
      {
         _connectionString = connectionString;
         _prefix = prefix;
         _logger = logger;
      }

      public async Task ConnectAsync()
      {
         _connection = await ConnectionMultiplexer.ConnectAsync( _connectionString ).ConfigureAwait( false );
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

      public Task SubscribeAsync<TKey, TEntry>( string id, IKeyConverter<TKey> keyConverter, SubscriptionType subscribe, Action<Serie<TKey, TEntry>> onMessage )
         where TEntry : IRedisEntry, new()
      {
         if( _redisSubscriber == null )
         {
            throw new InvalidOperationException( "The redis connection has not been started." );
         }

         var key = CreateSubscriptionKey( id, subscribe );
         return _redisSubscriber.SubscribeAsync( key, async ( channel, data ) =>
         {
            try
            {
               var entries = await RedisSerializer.Deserialize<TKey, TEntry>( data, keyConverter ).ConfigureAwait( false );
               onMessage( entries );
            }
            catch( Exception e )
            {
               _logger.Error( e, "An error ocurred while deserializing subscription message." );
            }
         } );
      }

      public Task UnsubscribeAsync( string id, SubscriptionType subscribe )
      {
         if( _redisSubscriber == null )
         {
            throw new InvalidOperationException( "The redis connection has not been started." );
         }

         var key = CreateSubscriptionKey( id, subscribe );
         return _redisSubscriber.UnsubscribeAsync( key );
      }

      public Task PublishLatestAsync<TKey, TEntry>( string id, TEntry entry )
         where TEntry : IRedisEntry
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
         where TEntry : IRedisEntry
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

      public void Close( bool allowCommandsToComplete = true )
      {
         if( _redisSubscriber != null )
         {
            _redisSubscriber.UnsubscribeAll();
            _redisSubscriber = null;
         }

         if( _connection != null )
         {
            _connection.Close( allowCommandsToComplete );
            _connection.Dispose();
            _connection = null;
         }
      }

      private async Task CreateScriptsOnAllServers()
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
            return _prefix + "." + id + "|A";
         }
         else if( subscribe == SubscriptionType.LatestPerCollection )
         {
            return _prefix + "." + id + "|L";
         }
         else
         {
            throw new ArgumentException( nameof( subscribe ) );
         }
      }

      private async void OnConnectionRestored( object sender, ConnectionFailedEventArgs args )
      {
         // Workaround for StackExchange.Redis/issues/61 that sometimes Redis connection is not connected in ConnectionRestored event 
         while( !_connection.GetDatabase( 0 ).IsConnected( _prefix + ".someKey" ) )
         {
            await Task.Delay( 200 ).ConfigureAwait( false );
         }

         await CreateScriptsOnAllServers().ConfigureAwait( false );
      }

      private void OnConnectionFailed( object sender, ConnectionFailedEventArgs args )
      {
         ConnectionFailed?.Invoke( args.Exception );
      }

      private void OnError( object sender, RedisErrorEventArgs args )
      {
         ErrorMessage?.Invoke( new InvalidOperationException( args.Message ) );
      }

      public void Dispose()
      {
         Close();
      }
   }
}
