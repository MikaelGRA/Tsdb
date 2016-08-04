using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Redis
{
   public class RedisPublishSubscribe<TEntry> : DefaultPublishSubscribe<TEntry>, IDisposable
      where TEntry : IRedisEntry
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

      public override async Task Publish( IEnumerable<TEntry> entries )
      {
         var tasks = new List<Task>();
         foreach( var entriesById in entries.GroupBy( x => x.GetId() ) )
         {
            var id = entriesById.Key;
            tasks.Add( _connection.PublishAsync( id, entriesById ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      protected override Task OnSubscribed( IEnumerable<string> ids )
      {
         List<Task> tasks = new List<Task>();
         foreach( var id in ids )
         {
            tasks.Add( _connection.SubscribeAsync<TEntry>( id, OnEntriesReceivedForId ) );
         }
         return Task.WhenAll( tasks );
      }

      protected override Task OnUnsubscribed( IEnumerable<string> ids )
      {
         List<Task> tasks = new List<Task>();
         foreach( var id in ids )
         {
            tasks.Add( _connection.UnsubscribeAsync( id ) );
         }
         return Task.WhenAll( tasks );
      }

      protected override Task OnSubscribedToAll()
      {
         return _connection.SubscribeAsync<TEntry>( "*", OnEntriesReceivedForAll );
      }

      protected override Task OnUnsubscribedFromAll()
      {
         return _connection.UnsubscribeAsync( "*" );
      }

      private void OnEntriesReceivedForId( List<TEntry> entries )
      {
         if( _continueOnCapturedSynchronizationContext )
         {
            _taskFactory.StartNew( () => PublishToIndividual( entries ) );
         }
         else
         {
            PublishToIndividual( entries );
         }
      }

      private void OnEntriesReceivedForAll( List<TEntry> entries )
      {
         if( _continueOnCapturedSynchronizationContext )
         {
            _taskFactory.StartNew( () => PublishToAll( entries ) );
         }
         else
         {
            PublishToAll( entries );
         }
      }

      private void PublishToAll( List<TEntry> entries )
      {
         var id = entries[ 0 ].GetId();
         foreach( var callback in _allCallbacks )
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

      private void PublishToIndividual( List<TEntry> entries )
      {
         var id = entries[ 0 ].GetId();
         HashSet<Action<List<TEntry>>> subscribers;
         if( _callbacks.TryGetValue( id, out subscribers ) )
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
            _connection.ConnectionRestored -= OnConnectionRestored;
         }

         await _connection.ConnectAsync( _connectionString ).ConfigureAwait( false );

         _connection.ErrorMessage += OnConnectionError;
         _connection.ConnectionFailed += OnConnectionFailed;
         _connection.ConnectionRestored += OnConnectionRestored;
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
