using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Vibrant.Tsdb.Ats;
using Vibrant.Tsdb.ConsoleApp.Entries;

namespace Vibrant.Tsdb.ConsoleApp
{
   public class Program
   {
      private static readonly string[] Ids = new[]
      {
         "row1",
         "row2",
         "row3",
         "row4",
         "row5",
         "row6",
         "row7",
      };

      private static readonly Random _rng = new Random();

      private static List<IEntry> CreateRows( DateTime startTime, int count )
      {
         List<IEntry> entries = new List<IEntry>();

         DateTime current = startTime;
         for( int i = 0 ; i < count ; i++ )
         {
            var entry = new BasicEntry();
            entry.Id = Ids[ _rng.Next( Ids.Length ) ];
            entry.Timestamp = current;
            entry.Value = _rng.NextDouble();

            current = current.AddSeconds( 1 );

            entries.Add( entry );
         }

         return entries;
      }

      public static void Main( string[] args )
      {
         var builder = new ConfigurationBuilder()
            .AddJsonFile( "appsettings.json" )
            .AddJsonFile( "appsettings.Hidden.json", true );
         var config = builder.Build();

         var ats = config.GetSection( "AtsStorage" );
         var connectionString = ats.GetSection( "ConnectionString" ).Value;
         var tableName = ats.GetSection( "Table" ).Value;

         TsdbTypeRegistry.Register<BasicEntry>();
         var store = new AtsVolumeStorage( tableName, connectionString );

         int count = 1000000;

         var from = new DateTime( 2016, 12, 26, 0, 0, 0, DateTimeKind.Utc );
         var to = from.AddSeconds( count - 1 );

         var rows = CreateRows( from, count );

         store.Write( rows ).Wait();

         var read = store.ReadMultiAs<BasicEntry>( Ids, from, to ).Result;

         Console.WriteLine( to );
         foreach( var r in read )
         {
            Console.WriteLine( r.Entries[ 0 ].Timestamp );
         }

         var sum = read.Sum( x => x.Entries.Count );

         var deletedCount = store.DeleteMulti( Ids, from, to ).Result;

         Console.WriteLine( sum );

      }
   }
}
