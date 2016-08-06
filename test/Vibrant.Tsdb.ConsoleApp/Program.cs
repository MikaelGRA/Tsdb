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
   public class Program : IWorkProvider
   {
      public event Action<TsdbVolumeMoval> MovalChangedOrAdded;
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
            new DataSource("m2", startTime, TimeSpan.FromMilliseconds( 10 ) ),
            new DataSource("m3", startTime, TimeSpan.FromMilliseconds( 10 ) ),
            new DataSource("m4", startTime, TimeSpan.FromMilliseconds( 10 ) ),
            new DataSource("m5", startTime, TimeSpan.FromMilliseconds( 10 ) ),
            new DataSource("m6", startTime, TimeSpan.FromMilliseconds( 10 ) ),
            new DataSource("m7", startTime, TimeSpan.FromMilliseconds( 10 ) ),
            new DataSource("m8", startTime, TimeSpan.FromMilliseconds( 10 ) ),
            new DataSource("m9", startTime, TimeSpan.FromMilliseconds( 10 ) ),
            new DataSource("m1", startTime, TimeSpan.FromMilliseconds( 10 ) ),
            new DataSource("m10", startTime, TimeSpan.FromMilliseconds( 10 ) ),
         };

         var client = TsdbFactory.CreateClient<BasicEntry>(
            sql.GetSection( "Table" ).Value,
            sql.GetSection( "ConnectionString" ).Value,
            ats.GetSection( "Table" ).Value,
            ats.GetSection( "ConnectionString" ).Value );

         // redis.GetSection( "ConnectionString" ).Value

         var batcher = new TsdbWriteBatcher<BasicEntry>( client, Publish.None, TimeSpan.FromSeconds( 5 ), 10000 );

         var engine = new TsdbEngine<BasicEntry>( this, client );

         engine.StartAsync().Wait();

         while( true )
         {
            var now = DateTime.UtcNow;
            foreach( var ds in _dataSources )
            {
               var entries = ds.GetEntries( now ).ToList();

               Console.WriteLine( $"Writing {entries.Count} entries..." );

               batcher.Write( entries ).ContinueWith( t =>
               {
                  if( t.IsFaulted || t.IsCanceled )
                  {
                     Console.WriteLine( t.Exception.Message );
                  }
                  else
                  {
                     Console.WriteLine( "Batch write completed!" );
                  }
               } );
            }

            Thread.Sleep( 1000 );
         }
      }

      public Task<IEnumerable<TsdbVolumeMoval>> GetAllMovalsAsync( DateTime now )
      {
         // move data that is two minutes old, every minute
         var movalTime = now + TimeSpan.FromMinutes( 1 );
         var moveUntil = movalTime - TimeSpan.FromMinutes( 2 );
         return Task.FromResult( _dataSources.Select( x => new TsdbVolumeMoval( x.Id, movalTime, moveUntil ) ) );

      }

      public Task<TsdbVolumeMoval> GetMovalAsync( TsdbVolumeMoval completedMoval )
      {
         // move data that is two minutes old, every minute
         var movalTime = completedMoval.Timestamp + TimeSpan.FromMinutes( 1 );
         var moveUntil = movalTime - TimeSpan.FromMinutes( 2 );
         return Task.FromResult( new TsdbVolumeMoval( completedMoval.Id, movalTime, moveUntil ) );
      }
   }
}
