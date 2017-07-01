using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Tests.Entries;
using Vibrant.Tsdb.Tests.Model;
using Xunit;

namespace Vibrant.Tsdb.Tests
{
   public abstract class AbstractTypedStorageTests<TStorage> : AbstractStorageTests<TStorage>
     where TStorage : IStorage<string, BasicEntry>, ITypedStorage<BasicEntry, MeasureType>
   {
      [Fact]
      public async Task Should_Read_Grouped()
      {
         var store = GetStorage( "GroupedTable1" );

         foreach( var id in Ids )
         {
            int count = 1000;
            var from = new DateTime( 2015, 12, 31, 0, 0, 0, DateTimeKind.Utc );
            var to = from.AddSeconds( count );
            var written = CreateRows( id, from, count );
            await store.WriteAsync( written );
         }

         var result = await store.ReadGroupsAsync( "Temperature", new Dictionary<string, string>(), new[] { "Placement" }, AggregationFunction.Average );

         await store.DeleteAsync( Ids );

         Assert.Equal( 2, result.Count() );

         var insideResult = result.FindResult( new TagCollection( new[] { new KeyValuePair<string, string>( "Placement", "Inside" ) } ) );
         var outsideResult = result.FindResult( new TagCollection( new[] { new KeyValuePair<string, string>( "Placement", "Outside" ) } ) );

         var insideEntry = insideResult.Entries.FirstOrDefault() as IAggregatableEntry;
         var outsideEntry = outsideResult.Entries.FirstOrDefault() as IAggregatableEntry;

         int seriesCount = 0;
         if( insideEntry != null )
         {
            seriesCount += insideEntry.GetCount();
         }
         if( outsideEntry != null )
         {
            seriesCount += outsideEntry.GetCount();
         }

         Assert.Equal( 7, seriesCount );
      }

      [Fact]
      public async Task Should_Read_Grouped_With_Requirement()
      {
         var store = GetStorage( "GroupedTable2" );

         foreach( var id in Ids )
         {
            int count = 1000;
            var from = new DateTime( 2015, 12, 31, 0, 0, 0, DateTimeKind.Utc );
            var to = from.AddSeconds( count );
            var written = CreateRows( id, from, count );
            await store.WriteAsync( written );
         }

         var result = await store.ReadGroupsAsync( "Temperature", new [] { new KeyValuePair<string, string>( "Owner", "ABC") }, new[] { "Placement" }, AggregationFunction.Average );

         await store.DeleteAsync( Ids );
      }

      [Fact]
      public async Task Should_Read_By_Multiple_Groups()
      {
         var store = GetStorage( "GroupedTable3" );

         foreach( var id in Ids )
         {
            int count = 1000;
            var from = new DateTime( 2015, 12, 31, 0, 0, 0, DateTimeKind.Utc );
            var to = from.AddSeconds( count );
            var written = CreateRows( id, from, count );
            await store.WriteAsync( written );
         }

         var result = await store.ReadGroupsAsync( "Temperature", new Dictionary<string, string>(), new[] { "Placement", "Owner" }, AggregationFunction.Sum );

         await store.DeleteAsync( Ids );
      }
   }
}
