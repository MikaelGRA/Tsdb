using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Vibrant.Tsdb.Tests.Entries;
using Xunit;

namespace Vibrant.Tsdb.Tests
{
   public abstract class AbstractStorageTests<TStorage>
      where TStorage : IStorage<string, BasicEntry>
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

      public abstract TStorage GetStorage( string tableName );

      [Theory]
      [InlineData( "Table1", Sort.Descending )]
      [InlineData( "Table2", Sort.Ascending )]
      public async Task Should_Write_And_Read_Basic_Rows( string tableName, Sort sort )
      {
         var store = GetStorage( tableName );

         int count = 50000;

         var from = new DateTime( 2016, 12, 31, 0, 0, 0, DateTimeKind.Utc );
         var to = from.AddSeconds( count );

         var written = CreateRows( from, count );
         await store.WriteAsync( written );
         var read = await store.ReadAsync( Ids, from, to, sort );
         var latest = await store.ReadLatestAsync( Ids );

         Dictionary<string, List<BasicEntry>> entries = new Dictionary<string, List<BasicEntry>>();
         foreach( var item in written )
         {
            List<BasicEntry> items;
            if( !entries.TryGetValue( item.GetKey(), out items ) )
            {
               items = new List<BasicEntry>();
               entries[ item.GetKey() ] = items;
            }
            items.Add( item );
         }

         await store.DeleteAsync( Ids, from, to );

         foreach( var readResult in read )
         {
            var sourceList = entries[ readResult.Key ].ToList();
            if( sort == Sort.Descending )
            {
               sourceList.Reverse();
            }

            Assert.Equal( sourceList.Count, readResult.Entries.Count );
            for( int i = 0 ; i < sourceList.Count ; i++ )
            {
               var original = sourceList[ i ];
               var readen = readResult.Entries[ i ];

               Assert.Equal( original.Value, readen.Value, 5 );
               Assert.Equal( original.Timestamp, readen.Timestamp );

               if( ( i == 0 && sort == Sort.Descending ) || ( i == sourceList.Count - 1 && sort == Sort.Ascending ) )
               {
                  var latestEntry = latest.First( x => x.Id == readResult.Key );
                  var entry = latestEntry.Entries[ 0 ];

                  Assert.Equal( original.Value, entry.Value, 5 );
                  Assert.Equal( original.Timestamp, entry.Timestamp );
               }
            }
         }
      }

      [Theory]
      [InlineData( "Table3", Sort.Descending )]
      [InlineData( "Table4", Sort.Ascending )]
      public async Task Should_Write_And_Delete_Basic_Rows( string tableName, Sort sort )
      {
         var store = GetStorage( tableName );

         int count = 50000;

         var from = new DateTime( 2016, 12, 31, 0, 0, 0, DateTimeKind.Utc );
         var to = from.AddSeconds( count );

         var written = CreateRows( from, count );

         await store.WriteAsync( written );

         await store.DeleteAsync( Ids, from, to );

         var read = await store.ReadAsync( Ids, from, to, sort );

         Assert.Equal( 0, read.Sum( x => x.Entries.Count ) );
      }

      [Theory]
      [InlineData( "Table5", Sort.Descending )]
      [InlineData( "Table6", Sort.Ascending )]
      public async Task Should_Write_Twice_Then_Delete_All_Basic_Rows( string tableName, Sort sort )
      {
         var store = GetStorage( tableName );

         int count = 1000;

         var from1 = new DateTime( 2012, 12, 26, 0, 0, 0, DateTimeKind.Utc );
         var written1 = CreateRows( from1, count );
         await store.WriteAsync( written1 );

         var from2 = new DateTime( 2013, 12, 26, 0, 0, 0, DateTimeKind.Utc );
         var written2 = CreateRows( from2, count );
         await store.WriteAsync( written2 );


         var rows = await store.ReadAsync( Ids );

         await store.DeleteAsync( Ids );

         var read = await store.ReadAsync( Ids, sort );

         Assert.Equal( count * 2, rows.Sum( x => x.Entries.Count ) );
         Assert.Equal( 0, read.Sum( x => x.Entries.Count ) );
      }

      [Fact]
      public async Task Should_Write_Duplicate_Rows()
      {
         var store = GetStorage( "Table9" );

         var from = new DateTime( 2016, 12, 31, 0, 0, 0, DateTimeKind.Utc );

         var written = new BasicEntry[]
         {
            new BasicEntry { Id = "row1", Timestamp = from, Value = 13.37 },
            new BasicEntry { Id = "row1", Timestamp = from, Value = 37.13 },
         };

         await store.WriteAsync( written );

         var read = await store.ReadAsync( Ids );

         await store.DeleteAsync( Ids );

         Assert.Equal( 1, read.Sum( x => x.Entries.Count ) );
      }
   }
}
