using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Vibrant.Tsdb.Tests.Entries;
using Xunit;

namespace Vibrant.Tsdb.Tests
{
   public abstract class AbstractTemporaryStorageTests<TStorage>
      where TStorage : ITemporaryStorage<string, BasicEntry>
   {
      protected static readonly string[] Ids = new[]
      {
         "row1",
         "row2",
         "row3",
         "row4",
         "row5",
         "row6",
         "row7",
      };

      protected static readonly Random _rng = new Random();

      protected static Dictionary<string, Serie<string, BasicEntry>> CreateRows( DateTime startTime, int count )
      {
         var series = new Dictionary<string, Serie<string, BasicEntry>>();

         DateTime current = startTime;
         for( int i = 0 ; i < count ; i++ )
         {
            var id = Ids[ _rng.Next( Ids.Length ) ];

            Serie<string, BasicEntry> serie;
            if( !series.TryGetValue( id, out serie ) )
            {
               serie = new Serie<string, BasicEntry>( id );
               series.Add( id, serie );
            }

            var entry = new BasicEntry();
            entry.Timestamp = current;
            entry.Value = _rng.NextDouble();

            current = current.AddSeconds( 1 );

            serie.Entries.Add( entry );
         }

         return series;
      }

      public abstract TStorage GetStorage();

      [Fact]
      public void Should_Write_And_Read_Basic_Rows()
      {
         var store = GetStorage();
         int count = 50000;
         var from = new DateTime( 2016, 12, 31, 0, 0, 0, DateTimeKind.Utc );
         var to = from.AddSeconds( count );

         var written = CreateRows( from, count );
         store.Write( written.Values );

         int i = 0;
         int seriesCount = 0;
         do
         {
            var segment = store.Read( 533 );
            foreach( var serie in segment.Series )
            {
               foreach( var entry in serie.Entries )
               {
                  i++;
               }
            }

            segment.Delete();

            seriesCount = segment.Series.Count;
         }
         while( seriesCount != 0 );

         Assert.Equal( count, i );
      }
   }
}
