using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;
using Vibrant.Tsdb.Ats.Serialization;

namespace Vibrant.Tsdb.Redis
{
   internal class RedisConnection : IDisposable
   {
      public event Action<Exception> ConnectionFailed;
      public event Action<Exception> ConnectionRestored;
      public event Action<Exception> ErrorMessage;

      private ISubscriber _redisSubscriber;
      private ConnectionMultiplexer _connection;

      public async Task ConnectAsync( string connectionString )
      {
         _connection = await ConnectionMultiplexer.ConnectAsync( connectionString ).ConfigureAwait( false );

         _connection.ConnectionFailed += OnConnectionFailed;
         _connection.ConnectionRestored += OnConnectionRestored;
         _connection.ErrorMessage += OnError;

         _redisSubscriber = _connection.GetSubscriber();
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

      public Task SubscribeAsync<TEntry>( string id, Action<List<TEntry>> onMessage )
         where TEntry : IRedisEntry
      {
         // TODO: Multiple at the same time...

         return _redisSubscriber.SubscribeAsync( id, ( channel, data ) =>
         {
            var entries = RedisSerializer.Deserialize<TEntry>( data );
            onMessage( entries );
         } );
      }

      public Task UnsubscribeAsync( string id )
      {
         return _redisSubscriber.UnsubscribeAsync( id );
      }

      public Task PublishAsync<TEntry>( string id, IEnumerable<TEntry> entries )
         where TEntry : IRedisEntry
      {
         if( _connection == null )
         {
            throw new InvalidOperationException( "The redis connection has not been started." );
         }

         var arrays = RedisSerializer.Serialize( entries.ToList(), 64 * 1024 );

         var tasks = new List<Task>();
         foreach( var data in arrays )
         {
            tasks.Add( _redisSubscriber.PublishAsync( id, data ) );
         }
         return Task.WhenAll( tasks );
      }

      private void OnConnectionFailed( object sender, ConnectionFailedEventArgs args )
      {
         var handler = ConnectionFailed;
         handler( args.Exception );
      }

      private void OnConnectionRestored( object sender, ConnectionFailedEventArgs args )
      {
         var handler = ConnectionRestored;
         handler( args.Exception );
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
