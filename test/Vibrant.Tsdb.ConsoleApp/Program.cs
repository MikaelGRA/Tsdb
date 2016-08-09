using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Vibrant.Tsdb.Ats;
using Vibrant.Tsdb.Client;
using Vibrant.Tsdb.ConsoleApp.Entries;
using Vibrant.Tsdb.Sql;

namespace Vibrant.Tsdb.ConsoleApp
{
   public class Program : IWorkProvider<string>
   {
      public event Action<TsdbVolumeMoval<string>> MovalChangedOrAdded;
      public event Action<string> MovalRemoved;

      public static void Main( string[] args )
      {
         var builder = new ConfigurationBuilder()
            .AddJsonFile( "appsettings.json" )
            .AddJsonFile( "appsettings.Hidden.json", true );
         var config = builder.Build();

         var program = new Program( config );

         Console.ReadKey();
      }

      private DataSource[] _dataSources;

      public Program( IConfiguration config )
      {
         var ats = config.GetSection( "AtsStorage" );
         var sql = config.GetSection( "SqlStorage" );
         var redis = config.GetSection( "RedisCache" );

         var startTime = DateTime.UtcNow;

         _dataSources = new DataSource[]
         {
            new DataSource("m2", startTime, TimeSpan.FromMilliseconds( 5 ) ),
            new DataSource("m3", startTime, TimeSpan.FromMilliseconds( 5 ) ),
            new DataSource("m4", startTime, TimeSpan.FromMilliseconds( 5 ) ),
            new DataSource("m5", startTime, TimeSpan.FromMilliseconds( 5 ) ),
            new DataSource("m6", startTime, TimeSpan.FromMilliseconds( 5 ) ),
            new DataSource("m7", startTime, TimeSpan.FromMilliseconds( 5 ) ),
            new DataSource("m8", startTime, TimeSpan.FromMilliseconds( 5 ) ),
            new DataSource("m9", startTime, TimeSpan.FromMilliseconds( 5 ) ),
            new DataSource("m1", startTime, TimeSpan.FromMilliseconds( 5 ) ),
            new DataSource("m10", startTime, TimeSpan.FromMilliseconds( 5 ) ),
         };

         var client = TsdbFactory.CreateAtsClient<string, BasicEntry>(
            "VolumeTable",
            "DynamicTable",
            ats.GetSection( "ConnectionString" ).Value,
            @"C:\tsdb\cache" );

         // redis.GetSection( "ConnectionString" ).Value

         client.TemporaryWriteFailure += Client_TemporaryWriteFailure;
         client.WriteFailure += Client_WriteFailure;

         var batcher = new TsdbWriteBatcher<string, BasicEntry>( client, PublicationType.None, TimeSpan.FromSeconds( 5 ), 10000 );

         var engine = new TsdbEngine<string, BasicEntry>( this, client );
         engine.MoveTemporaryDataFailed += Engine_MoveTemporaryDataFailed;
         engine.MoveToVolumeStorageFailed += Engine_MoveToVolumeStorageFailed;
         engine.StartAsync().Wait();

         // TODO: Test if this works as expected
         //  -> Moval to temp storage and moval away from it again...
         //  -> Dont keep this moving infitely to test

         while( true )
         {
            var now = DateTime.UtcNow;
            foreach( var ds in _dataSources )
            {
               var entries = ds.GetEntries( now ).ToList();

               Console.WriteLine( $"Writing {entries.Count} entries..." );

               batcher.Write( entries );
            }

            Thread.Sleep( 1000 );
         }
      }

      private int _c1, _c2, _c3, _c4;

      private void Client_WriteFailure( object sender, TsdbWriteFailureEventArgs<string, BasicEntry> e )
      {
         if(!( e.Exception is SqlException ) )
         {

         }

         Console.WriteLine( "Client_WriteFailure: " + e.Exception );
      }

      private void Client_TemporaryWriteFailure( object sender, TsdbWriteFailureEventArgs<string, BasicEntry> e )
      {
         if( !( e.Exception is SqlException ) )
         {

         }

         Console.WriteLine( "Client_TemporaryWriteFailure: " + e.Exception );
      }

      private void Engine_MoveToVolumeStorageFailed( object sender, ExceptionEventArgs e )
      {
         if( !( e.Exception is SqlException ) )
         {
         }

         Console.WriteLine( "Engine_MoveToVolumeStorageFailed: " + e.Exception );
      }

      private void Engine_MoveTemporaryDataFailed( object sender, ExceptionEventArgs e )
      {
         if( !( e.Exception is SqlException ) )
         {

         }

         Console.WriteLine( "Engine_MoveTemporaryDataFailed: " + e.Exception );
      }

      public Task<IEnumerable<TsdbVolumeMoval<string>>> GetAllMovalsAsync( DateTime now )
      {
         // move data that is two minutes old, every minute
         var movalTime = now + TimeSpan.FromMinutes( 1 );
         var moveUntil = movalTime - TimeSpan.FromMinutes( 2 );
         return Task.FromResult( _dataSources.Select( x => new TsdbVolumeMoval<string>( x.Id, movalTime, moveUntil ) ) );

      }

      public Task<TsdbVolumeMoval<string>> GetMovalAsync( TsdbVolumeMoval<string> completedMoval )
      {
         // move data that is two minutes old, every minute
         var movalTime = completedMoval.Timestamp + TimeSpan.FromMinutes( 1 );
         var moveUntil = movalTime - TimeSpan.FromMinutes( 2 );
         return Task.FromResult( new TsdbVolumeMoval<string>( completedMoval.Id, movalTime, moveUntil ) );
      }

      public TimeSpan GetTemporaryMovalInterval()
      {
         return TimeSpan.FromMinutes( 1 );
      }

      public int GetTemporaryMovalBatchSize()
      {
         return 5000;
      }

      public int GetDynamicMovalBatchSize()
      {
         return 5000;
      }
   }
}
