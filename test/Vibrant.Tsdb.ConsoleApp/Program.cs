using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Vibrant.Tsdb.Ats;
using Vibrant.Tsdb.Client;
using Vibrant.Tsdb.ConsoleApp.Entries;
using Vibrant.Tsdb.Sql;

namespace Vibrant.Tsdb.ConsoleApp
{
   public class Program : IWorkProvider<string>, ITsdbLogger
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
#if !COREFX
         ServicePointManager.DefaultConnectionLimit = 500;
         ServicePointManager.Expect100Continue = false;
         ServicePointManager.UseNagleAlgorithm = false;
#endif

         var ats = config.GetSection( "AtsStorage" );
         var sql = config.GetSection( "SqlStorage" );
         var redis = config.GetSection( "RedisCache" );

         var startTime = DateTime.UtcNow;

         _dataSources = new DataSource[]
         {
            new DataSource("m2", startTime, TimeSpan.FromMilliseconds( 1 ) ),
            new DataSource("m3", startTime, TimeSpan.FromMilliseconds( 1 ) ),
            new DataSource("m4", startTime, TimeSpan.FromMilliseconds( 1 ) ),
            new DataSource("m5", startTime, TimeSpan.FromMilliseconds( 1 ) ),
            new DataSource("m6", startTime, TimeSpan.FromMilliseconds( 1 ) ),
            new DataSource("m7", startTime, TimeSpan.FromMilliseconds( 1 ) ),
            new DataSource("m8", startTime, TimeSpan.FromMilliseconds( 1 ) ),
            new DataSource("m9", startTime, TimeSpan.FromMilliseconds( 1 ) ),
            new DataSource("m1", startTime, TimeSpan.FromMilliseconds( 1 ) ),
            new DataSource("m10", startTime, TimeSpan.FromMilliseconds( 1 ) ),
         };

         var client = TsdbFactory.CreateAtsClient<string, BasicEntry>(
            "VolumeTable",
            //sql.GetSection( "ConnectionString" ).Value,
            "DynamicTable",
            ats.GetSection( "ConnectionString" ).Value,
            @"C:\tsdb\cache",
            this );

         // redis.GetSection( "ConnectionString" ).Value

         var batcher = new TsdbWriteBatcher<string, BasicEntry>( client, PublicationType.None, TimeSpan.FromSeconds( 5 ), 20000, this );

         var engine = new TsdbEngine<string, BasicEntry>( this, client );
         engine.StartAsync().Wait();

         // TODO: Test if this works as expected
         //  -> Moval to temp storage and moval away from it again...
         //  -> Dont keep this moving infitely to test

         Console.WriteLine( $"Info: Writing entries..." );
         while( true )
         {
            var now = DateTime.UtcNow;
            foreach( var ds in _dataSources )
            {
               var entries = ds.GetEntries( now ).ToList();

               batcher.Write( entries );
            }

            Thread.Sleep( 1000 );
         }
      }

      private int _c1, _c2, _c3, _c4;

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

      public void Debug( string message )
      {
         Console.WriteLine( "Debug: " + message );
      }

      public void Info( string message )
      {
         Console.WriteLine( "Info: " + message );
      }

      public void Warn( string message )
      {
         Console.WriteLine( "Warn: " + message );
      }

      public void Error( string message )
      {
         Console.WriteLine( "Error: " + message );
      }

      public void Fatal( string message )
      {
         Console.WriteLine( "Fatal: " + message );
      }

      public void Debug( Exception e, string message )
      {
         Console.WriteLine( "Debug: " + message + "(" + e.GetType().Name + ")" );
      }

      public void Info( Exception e, string message )
      {
         Console.WriteLine( "Info: " + message + "(" + e.GetType().Name + ")" );
      }

      public void Warn( Exception e, string message )
      {
         Console.WriteLine( "Warn: " + message + "(" + e.GetType().Name + ")" );
      }

      public void Error( Exception e, string message )
      {
         Console.WriteLine( "Error: " + message + "(" + e.GetType().Name + ")" );
      }

      public void Fatal( Exception e, string message )
      {
         Console.WriteLine( "Fatal: " + message + "(" + e.GetType().Name + ")" );
      }
   }
}
