using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Vibrant.Tsdb.Ats;
using Vibrant.Tsdb.Client;
using Vibrant.Tsdb.ConsoleApp.Entries;
using Vibrant.Tsdb.ConsoleApp.Model;
using Vibrant.Tsdb.Files;
using Vibrant.Tsdb.Helpers;
using Vibrant.Tsdb.Sql;

namespace Vibrant.Tsdb.ConsoleApp
{
   public class Program : IWorkProvider<BasicKey>, ITsdbLogger, IKeyConverter<BasicKey>
   {
      public event Action<TsdbVolumeMoval<BasicKey>> MovalChangedOrAdded;
      public event Action<BasicKey> MovalRemoved;

      public static void Main( string[] args )
      {
         var builder = new ConfigurationBuilder()
            .AddJsonFile( "appsettings.json" )
            .AddJsonFile( "appsettings.Hidden.json", true );
         var config = builder.Build();

         var program = new Program( config );

         Console.ReadKey();
      }

      private List<DataSource> _dataSources;

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

         _dataSources = new List<DataSource>();
         for( int i = 0 ; i < 80 ; i++ )
         {
            _dataSources.Add( new DataSource( new BasicKey { Id = Guid.NewGuid(), Sampling = Sampling.Daily }, startTime, TimeSpan.FromMilliseconds( 10 ) ) );
         }

         var dats = new AtsDynamicStorage<BasicKey, BasicEntry>(
            "DatsTables13",
            ats.GetSection( "ConnectionString" ).Value,
            new ConcurrencyControl( AtsDynamicStorage<BasicKey, BasicEntry>.DefaultReadParallelism, AtsDynamicStorage<BasicKey, BasicEntry>.DefaultWriteParallelism ),
            new YearlyPartitioningProvider<BasicKey>(),
            new YearlyTableProvider(),
            this );

         var dsql = new SqlDynamicStorage<BasicKey, BasicEntry>(
            "SqlTable3",
            sql.GetSection( "ConnectionString" ).Value,
            new ConcurrencyControl( 5, 5 ),
            this );

         var switchDate = new DateTime( 2016, 10, 20, 18, 25, 0, DateTimeKind.Utc );

         var selector = new TestDynamicStorageSelector( new StorageSelection<BasicKey, BasicEntry, IDynamicStorage<BasicKey, BasicEntry>>[]
         {
            new StorageSelection<BasicKey, BasicEntry, IDynamicStorage<BasicKey, BasicEntry>>( dsql, switchDate, null ),
            new StorageSelection<BasicKey, BasicEntry, IDynamicStorage<BasicKey, BasicEntry>>( dats, null, switchDate ),
         } );

         var vats = new AtsVolumeStorage<BasicKey, BasicEntry>(
            "VatsTable1",
            ats.GetSection( "ConnectionString" ).Value,
            new ConcurrencyControl( AtsVolumeStorage<BasicKey, BasicEntry>.DefaultReadParallelism, AtsVolumeStorage<BasicKey, BasicEntry>.DefaultWriteParallelism ),
            new YearlyPartitioningProvider<BasicKey>(),
            this );

         var tfs = new TemporaryFileStorage<BasicKey, BasicEntry>(
            @"C:\tsdb\cache",
            TemporaryFileStorage<BasicKey, BasicEntry>.DefaultMaxFileSize,
            TemporaryFileStorage<BasicKey, BasicEntry>.DefaultMaxStorageSize,
            this );

         //var client = new TsdbClient<BasicKey, BasicEntry>( selector, vats, tfs, this );

         //var batcher = new TsdbWriteBatcher<BasicKey, BasicEntry>( client, PublicationType.None, TimeSpan.FromSeconds( 5 ), 20000, this );

         //ThreadPool.QueueUserWorkItem( obj => batcher.Handle() );

         ////var engine = new TsdbEngine<BasicKey, BasicEntry>( this, client );
         ////engine.StartAsync().Wait();

         //Console.WriteLine( $"Info: Writing entries..." );
         //while( true )
         //{
         //   var now = DateTime.UtcNow;
         //   foreach( var ds in _dataSources )
         //   {
         //      var serie = ds.GetEntries( now );

         //      batcher.Write( serie );
         //   }

         //   Thread.Sleep( 1000 );
         //}






         var typeStorage = new TestTypedKeyStorage( _dataSources.Select( x => x.Id ) );
         var client = new TsdbClient<BasicKey, BasicEntry>( dats, tfs, this );
         var aggregationFunctions = new AggregationTsdbClient<BasicKey, BasicEntry, MeasureType>( dats, typeStorage, this );

         var batcher = new TsdbWriteBatcher<BasicKey, BasicEntry>( client, PublicationType.None, TimeSpan.FromSeconds( 5 ), 20000, this );

         ThreadPool.QueueUserWorkItem( obj => batcher.Handle() );

         var from = DateTime.UtcNow;

         Console.WriteLine( $"Info: Writing entries..." );
         for( int i = 0 ; i < 5 ; i++ )
         {
            var now = DateTime.UtcNow;
            foreach( var ds in _dataSources )
            {
               var serie = ds.GetEntries( now );

               batcher.Write( serie );
            }

            Thread.Sleep( 1000 );
         }

         var to = DateTime.UtcNow;


         Thread.Sleep( 30000 );
         Console.WriteLine( $"Info: Reading groupings..." );

         // make optional or nullable...
         //var result = client.ReadLatestAsync( _dataSources.Select( x => x.Id ), 10 ).Result;
         var result = aggregationFunctions.ReadGroupsAsync( "Temperature", /*from, to,*/ new Dictionary<string, string>(), new[] { "Placement" }, GroupMethod.Average ).Result;

         Console.WriteLine( result.Sum( x => x.Entries.Count ) );
      }

      public Task<IEnumerable<TsdbVolumeMoval<BasicKey>>> GetAllMovalsAsync( DateTime now )
      {
         // move data that is two minutes old, every minute
         var movalTime = now + TimeSpan.FromMinutes( 1 );
         var moveUntil = movalTime - TimeSpan.FromMinutes( 2 );
         return Task.FromResult( _dataSources.Select( x => new TsdbVolumeMoval<BasicKey>( x.Id, movalTime, moveUntil, TimeSpan.MaxValue ) ) );

      }

      public Task<TsdbVolumeMoval<BasicKey>> GetMovalAsync( TsdbVolumeMoval<BasicKey> completedMoval )
      {
         // move data that is two minutes old, every minute
         var movalTime = completedMoval.Timestamp + TimeSpan.FromMinutes( 1 );
         var moveUntil = movalTime - TimeSpan.FromMinutes( 2 );
         return Task.FromResult( new TsdbVolumeMoval<BasicKey>( completedMoval.Id, movalTime, moveUntil, TimeSpan.MaxValue ) );
      }

      public TimeSpan GetTemporaryMovalInterval()
      {
         return TimeSpan.FromMinutes( 1 );
      }

      public int GetTemporaryMovalBatchSize()
      {
         // This number should probably be very large and the items should be more or less
         // continuously processed, BUT NOT AT THE SAME TIME!!!!! How can this be achieved?
         return 5000;
      }

      public int GetDynamicMovalBatchSize()
      {
         return 20000;
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

      // requires optimal implementation
      public async Task<BasicKey> ConvertAsync( string key )
      {
         var parts = key.Split( '|' ); // substring or string split???
         return new BasicKey
         {
            Id = GuidHelper.ParseShortGuid( parts[ 0 ] ),
            Sampling = (Sampling)Enum.Parse( typeof( Sampling ), parts[ 1 ] ), // dictionary lookup or parse?
         };
      }

      public string Convert( BasicKey key )
      {
         var sb = new StringBuilder();
         sb.Append( key.Id.ToShortString() ).Append( "|" ).Append( key.Sampling ); // string builder of string concat?
         return sb.ToString();
      }

      public void Trace( string message )
      {
         Console.WriteLine( "Trace: " + message );
      }

      public void Trace( Exception e, string message )
      {
         Console.WriteLine( "Trace: " + message + "(" + e.GetType().Name + ")" );
      }
   }
}
