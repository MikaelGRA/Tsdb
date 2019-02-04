using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Tests.Entries;
using Xunit;

namespace Vibrant.Tsdb.Tests
{
   public abstract class AbstractPublishSubscribeTests<TPublishSubscribe>
     where TPublishSubscribe : IPublishSubscribe<string, BasicEntry>
   {
      public abstract TPublishSubscribe CreatePublishSubscribe();

      [Fact]
      public async Task Should_Subscribe_And_Publish()
      {
         var ps = CreatePublishSubscribe();

         await ps.WaitWhileDisconnectedAsync();

         int received1 = 0;
         var unsubscribe1 = await ps.SubscribeAsync( new[] { "row1" }, SubscriptionType.AllFromCollections, serie =>
         {
            Assert.Equal( "row1", serie.GetKey() );
            foreach( var entry in serie.GetEntries() )
            {
               received1++;
            }
         } );

         int received2 = 0;
         var unsubscribe2 = await ps.SubscribeAsync( new[] { "row2" }, SubscriptionType.AllFromCollections, serie =>
         {
            Assert.Equal( "row2", serie.GetKey() );
            foreach( var entry in serie.GetEntries() )
            {
               received2++;
            }
         } );

         int received3 = 0;
         var unsubscribe3 = await ps.SubscribeToAllAsync( SubscriptionType.AllFromCollections, serie =>
         {
            foreach( var entry in serie.GetEntries() )
            {
               received3++;
            }
         } );

         int received4 = 0;
         var unsubscribe4 = await ps.SubscribeAsync( new[] { "row6" }, SubscriptionType.LatestPerCollection, serie =>
         {
            foreach( var entry in serie.GetEntries() )
            {
               received4++;
            }
         } );

         var now = DateTime.UtcNow;

         var series = new[]
         {
            new SortedSerie<string, BasicEntry>( "row1", Sort.Ascending, new[]
            {
               new BasicEntry { Timestamp = now, Value = 23.53 },
               new BasicEntry { Timestamp = now + TimeSpan.FromSeconds( 1 ), Value = 50.23 },
            } ),
            new SortedSerie<string, BasicEntry>( "row2", Sort.Ascending, new[]
            {
               new BasicEntry { Timestamp = now + TimeSpan.FromSeconds( 2 ), Value = 23 },
            } ),
            new SortedSerie<string, BasicEntry>( "row6", Sort.Ascending, new[]
            {
               new BasicEntry { Timestamp = now + TimeSpan.FromSeconds( 3 ), Value = 2364 },
            } ),
         };

         await ps.PublishAsync( series, PublicationType.Both );
         await Task.Delay( 3000 );

         Assert.Equal( 2, received1 );
         Assert.Equal( 1, received2 );
         Assert.Equal( 4, received3 );
         Assert.Equal( 1, received4 );

         await unsubscribe3();
         await ps.PublishAsync( series, PublicationType.Both );
         await Task.Delay( 3000 );

         Assert.Equal( 4, received1 );
         Assert.Equal( 2, received2 );
         Assert.Equal( 4, received3 );
         Assert.Equal( 2, received4 );

         await unsubscribe2();
         await ps.PublishAsync( series, PublicationType.Both );
         await Task.Delay( 3000 );

         Assert.Equal( 6, received1 );
         Assert.Equal( 2, received2 );
         Assert.Equal( 4, received3 );
         Assert.Equal( 3, received4 );

         await unsubscribe1();
         await ps.PublishAsync( series, PublicationType.Both );
         await Task.Delay( 3000 );

         Assert.Equal( 6, received1 );
         Assert.Equal( 2, received2 );
         Assert.Equal( 4, received3 );
         Assert.Equal( 4, received4 );

         await ps.PublishAsync( new SortedSerie<string, BasicEntry>( "row6", Sort.Ascending, new BasicEntry { Timestamp = now + TimeSpan.FromSeconds( 5 ), Value = 1337 } ), PublicationType.LatestPerCollection );
         await Task.Delay( 3000 );

         Assert.Equal( 6, received1 );
         Assert.Equal( 2, received2 );
         Assert.Equal( 4, received3 );
         Assert.Equal( 5, received4 );

         await unsubscribe4();
      }
   }
}
