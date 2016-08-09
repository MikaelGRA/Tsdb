using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Ats.Tests.Entries;
using Xunit;

namespace Vibrant.Tsdb.Ats.Tests
{
   public abstract class AbstractDynamicStorageTests<TStorage> : AbstractStorageTests<TStorage>
     where TStorage : IDynamicStorage<string, BasicEntry>
   {
      [Fact]
      public async Task Should_Read_Segmented()
      {
         var store = GetStorage( "SegmentTable2" );

         int count = 45000;

         var from = new DateTime( 2015, 12, 31, 0, 0, 0, DateTimeKind.Utc );
         var to = from.AddSeconds( count );

         var written = CreateRows( "rowlol2", from, count );

         await store.Write( written );

         IContinuationToken token = null;
         int round = 0;
         do
         {
            round++;
            var segment = await store.Read( "rowlol2", new DateTime( 2018, 12, 31, 0, 0, 0 ), 10000, token );

            if( round == 5 )
            {
               Assert.Equal( 5000, segment.Entries.Count );
            }
            else if( round == 1 || round == 2 || round == 3 || round == 4 )
            {
               Assert.Equal( 10000, segment.Entries.Count );
            }


            token = segment.ContinuationToken;
         }
         while( token.HasMore );

         await store.Delete( "rowlol2" );
      }

      [Fact]
      public async Task Should_Read_Segmented_Unbounded()
      {
         var store = GetStorage( "SegmentTable3" );

         int count = 45000;

         var from = new DateTime( 2015, 12, 31, 0, 0, 0, DateTimeKind.Utc );
         var to = from.AddSeconds( count );

         var written = CreateRows( "rowlol3", from, count );

         await store.Write( written );

         IContinuationToken token = null;
         int round = 0;
         do
         {
            round++;
            var segment = await store.Read( "rowlol3", 10000, token );

            if( round == 5 )
            {
               Assert.Equal( 5000, segment.Entries.Count );
            }
            else if( round == 1 || round == 2 || round == 3 || round == 4 )
            {
               Assert.Equal( 10000, segment.Entries.Count );
            }


            token = segment.ContinuationToken;
         }
         while( token.HasMore );

         await store.Delete( "rowlol3" );
      }

      [Fact]
      public async Task Should_Read_And_Delete_Segmented()
      {
         var store = GetStorage( "SegmentTable4" );

         int count = 45000;

         var from = new DateTime( 2015, 12, 31, 0, 0, 0, DateTimeKind.Utc );
         var to = from.AddSeconds( count );

         var written = CreateRows( "rowlol2", from, count );

         await store.Write( written );

         IContinuationToken token = null;
         int round = 0;
         do
         {
            round++;
            var segment = await store.Read( "rowlol2", new DateTime( 2018, 12, 31, 0, 0, 0 ), 10000, token );

            if( round == 5 )
            {
               Assert.Equal( 5000, segment.Entries.Count );
            }
            else if( round == 1 || round == 2 || round == 3 || round == 4 )
            {
               Assert.Equal( 10000, segment.Entries.Count );
            }

            await segment.DeleteAsync().ConfigureAwait( false );

            token = segment.ContinuationToken;
         }
         while( token.HasMore );

         var read = await store.Read( "rowlol2" );

         Assert.Equal( 0, read.Entries.Count );
      }
   }
}
