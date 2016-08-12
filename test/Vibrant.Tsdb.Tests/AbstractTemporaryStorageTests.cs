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

      protected static List<BasicEntry> CreateRows( DateTime startTime, int count )
      {
         var entries = new List<BasicEntry>();

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

      protected static List<BasicEntry> CreateRows( string id, DateTime startTime, int count )
      {
         var entries = new List<BasicEntry>();

         DateTime current = startTime;
         for( int i = 0 ; i < count ; i++ )
         {
            var entry = new BasicEntry();
            entry.Id = id;
            entry.Timestamp = current;
            entry.Value = _rng.NextDouble();

            current = current.AddSeconds( 1 );

            entries.Add( entry );
         }

         return entries;
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
         store.Write( written );

         int i = 0;
         int entryCount = 0;
         do
         {
            var segment = store.Read( 533 );
            for( int j = 0 ; j < segment.Entries.Count ; j++ )
            {
               var read = segment.Entries[ j ];
               var orignal = written[ i ];

               Assert.Equal( orignal.Id, read.Id );
               Assert.Equal( orignal.Timestamp, read.Timestamp );
               Assert.Equal( orignal.Value, read.Value );

               i++;
            }

            segment.Delete();

            entryCount = segment.Entries.Count;
         }
         while( entryCount != 0 );

         Assert.Equal( count, i );
      }
   }
}
