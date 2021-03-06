﻿using System;
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

      protected static Serie<string, BasicEntry> CreateRows( string id, DateTime startTime, int count )
      {
         var serie = new Serie<string, BasicEntry>( id );

         DateTime current = startTime;
         for( int i = 0 ; i < count ; i++ )
         {
            var entry = new BasicEntry();
            entry.Timestamp = current;
            entry.Value = _rng.NextDouble();

            current = current.AddSeconds( 1 );

            serie.Entries.Add( entry );
         }

         return serie;
      }

      protected static Serie<string, BasicEntry> CreateByteArrayRows( string id, DateTime startTime, byte[] arr, int count )
      {
         var serie = new Serie<string, BasicEntry>( id );

         DateTime current = startTime;
         for( int i = 0 ; i < count ; i++ )
         {
            var entry = new BasicEntry();
            entry.Timestamp = current;
            entry.Fields[ "Data" ] = arr;

            current = current.AddSeconds( 1 );

            serie.Entries.Add( entry );
         }

         return serie;
      }

      public abstract TStorage GetStorage( string tableName );

      [Theory]
      [InlineData( "Table1", Sort.Descending )]
      [InlineData( "Table2", Sort.Ascending )]
      public async Task Should_Write_And_Read_Basic_Rows( string tableName, Sort sort )
      {
         var store = GetStorage( tableName );

         int count = 50000;

         var from = new DateTime( 2015, 12, 31, 0, 0, 0, DateTimeKind.Utc );
         var to = from.AddSeconds( count );

         var series = CreateRows( from, count );
         await store.WriteAsync( series.Values );
         var read = await store.ReadAsync( Ids, from, to, sort );
         var latest = await store.ReadLatestAsync( Ids, 1 );
         var latest2 = await store.ReadLatestAsync( Ids, 1500 );

         await store.DeleteAsync( Ids, from, to );

         foreach( var item in latest )
         {
            Assert.Equal( 1, item.Entries.Count );
         }

         foreach( var item in latest2 )
         {
            Assert.Equal( 1500, item.Entries.Count );
         }

         foreach( var readResult in read )
         {
            var sourceList = series[ readResult.Key ].Entries.ToList();
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
                  var latestEntry = latest.First( x => x.GetKey() == readResult.Key );
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

         var from = new DateTime( 2015, 12, 31, 0, 0, 0, DateTimeKind.Utc );
         var to = from.AddSeconds( count );

         var written = CreateRows( from, count );

         await store.WriteAsync( written.Values );

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

         var from1 = new DateTime( 2014, 12, 26, 0, 0, 0, DateTimeKind.Utc );
         var written1 = CreateRows( from1, count );
         await store.WriteAsync( written1.Values );

         var from2 = new DateTime( 2015, 12, 26, 0, 0, 0, DateTimeKind.Utc );
         var written2 = CreateRows( from2, count );
         await store.WriteAsync( written2.Values );


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

         var from = new DateTime( 2015, 12, 31, 0, 0, 0, DateTimeKind.Utc );

         var serie = new Serie<string, BasicEntry>( "row1", new[]
         {
            new BasicEntry { Timestamp = from, Value = 13.37 },
            new BasicEntry { Timestamp = from, Value = 37.13 },
         } );

         await store.WriteAsync( serie );

         var read = await store.ReadAsync( Ids );

         await store.DeleteAsync( Ids );

         Assert.Equal( 1, read.Sum( x => x.Entries.Count ) );
      }

      [Fact]
      public async Task Should_Read_Two_Latest()
      {
         var store = GetStorage( "Table10" );

         var from = new DateTime( 2015, 12, 31, 0, 0, 0, DateTimeKind.Utc );

         var serie = new Serie<string, BasicEntry>( "row5", new[]
         {
            new BasicEntry { Timestamp = from.AddMilliseconds( 20 ), Value = 13.37 },
            new BasicEntry { Timestamp = from.AddMilliseconds( 10 ), Value = 37.13 },
            new BasicEntry { Timestamp = from, Value = 153.10 },
         } );

         await store.WriteAsync( serie );

         var read = await store.ReadLatestAsync( "row5", 2 );

         await store.DeleteAsync( "row5" );

         Assert.Equal( 2, read.Entries.Count );
         Assert.Equal( serie.Entries[ 0 ].Value, read.Entries[ 0 ].Value );
         Assert.Equal( serie.Entries[ 1 ].Value, read.Entries[ 1 ].Value );
      }

      [Fact]
      public async Task Should_Read_Two_Latest_Since1()
      {
         var store = GetStorage( "Table11" );

         var from = new DateTime( 2015, 12, 31, 0, 0, 0, DateTimeKind.Utc );
         var to = from.AddMilliseconds( 35 );

         var serie = new Serie<string, BasicEntry>( "row5", new[]
         {
            new BasicEntry { Timestamp = from.AddMilliseconds( 40 ), Value = 60 },
            new BasicEntry { Timestamp = from.AddMilliseconds( 30 ), Value = 53.13 },
            new BasicEntry { Timestamp = from.AddMilliseconds( 20 ), Value = 13.37 },
            new BasicEntry { Timestamp = from.AddMilliseconds( 10 ), Value = 37.13 },
            new BasicEntry { Timestamp = from, Value = 153.10 },
         } );

         await store.WriteAsync( serie );

         var read = await store.ReadLatestSinceAsync( "row5", to, 2, Sort.Descending );

         await store.DeleteAsync( "row5" );

         Assert.Equal( 2, read.Entries.Count );
         Assert.Equal( serie.Entries[ 1 ].Value, read.Entries[ 0 ].Value );
         Assert.Equal( serie.Entries[ 2 ].Value, read.Entries[ 1 ].Value );
      }

      [Fact]
      public async Task Should_Read_Two_Latest_Since2()
      {
         var store = GetStorage( "Table12" );

         var from = new DateTime( 2015, 12, 31, 0, 0, 0, DateTimeKind.Utc );
         var to = from.AddMilliseconds( 35 );

         var serie = new Serie<string, BasicEntry>( "row5", new[]
         {
            new BasicEntry { Timestamp = from.AddMilliseconds( 40 ), Value = 60 },
            new BasicEntry { Timestamp = from.AddMilliseconds( 30 ), Value = 53.13 },
            new BasicEntry { Timestamp = from.AddMilliseconds( 20 ), Value = 13.37 },
            new BasicEntry { Timestamp = from.AddMilliseconds( 10 ), Value = 37.13 },
            new BasicEntry { Timestamp = from, Value = 153.10 },
         } );

         await store.WriteAsync( serie );

         var read = await store.ReadLatestSinceAsync( "row5", to, 2, Sort.Ascending );

         await store.DeleteAsync( "row5" );

         Assert.Equal( 2, read.Entries.Count );
         Assert.Equal( serie.Entries[ 2 ].Value, read.Entries[ 0 ].Value );
         Assert.Equal( serie.Entries[ 1 ].Value, read.Entries[ 1 ].Value );
      }

      [Fact]
      public async Task Should_Write_And_Delete_ByteArray_Rows()
      {
         var store = GetStorage( "Table13" );

         int count = 250;

         var from = new DateTime( 2015, 12, 31, 0, 0, 0, DateTimeKind.Utc );
         var to = from.AddSeconds( count );

         var written = CreateByteArrayRows( "lul", from, new byte[] { 1, 2, 3 }, count );

         await store.WriteAsync( written );

         var read1 = await store.ReadAsync( "lul", from, to );

         await store.DeleteAsync( "lul", from, to );

         var read = await store.ReadAsync( "lul", from, to );

         Assert.Equal( 0, read.Entries.Count );
      }
   }
}
