using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Ats.Tests.Entries;
using Xunit;

namespace Vibrant.Tsdb.Ats.Tests
{
   public abstract class AbstractPublishSubscribeTests<TPublishSubscribe>
     where TPublishSubscribe : IPublishSubscribe<string, BasicEntry>
   {
      public abstract TPublishSubscribe CreatePublishSubscribe();

      [Fact]
      public async Task Should_Subscribe_And_Publish()
      {
         var ps = CreatePublishSubscribe();

         await ps.WaitWhileDisconnected();

         int received1 = 0;
         var unsubscribe1 = await ps.Subscribe( new[] { "row1" }, SubscriptionType.AllFromCollections, entries =>
         {
            foreach( var entry in entries )
            {
               Assert.Equal( "row1", entry.GetKey() );
               received1++;
            }
         } );

         int received2 = 0;
         var unsubscribe2 = await ps.Subscribe( new[] { "row2" }, SubscriptionType.AllFromCollections, entries =>
         {
            foreach( var entry in entries )
            {
               Assert.Equal( "row2", entry.GetKey() );
               received2++;
            }
         } );

         int received3 = 0;
         var unsubscribe3 = await ps.SubscribeToAll( SubscriptionType.AllFromCollections, entries =>
         {
            foreach( var entry in entries )
            {
               received3++;
            }
         } );

         int received4 = 0;
         var unsubscribe4 = await ps.Subscribe( new[] { "row6" }, SubscriptionType.LatestPerCollection, entries =>
         {
            foreach( var entry in entries )
            {
               received4++;
            }
         } );

         var now = DateTime.UtcNow;

         var toPublish = new[] {
               new BasicEntry { Id = "row1", Timestamp = now, Value = 23.53 },
               new BasicEntry { Id = "row1", Timestamp = now + TimeSpan.FromSeconds( 1 ), Value = 50.23 },
               new BasicEntry { Id = "row2", Timestamp = now + TimeSpan.FromSeconds( 2 ), Value = 23 },
               new BasicEntry { Id = "row6", Timestamp = now + TimeSpan.FromSeconds( 3 ), Value = 2364 },
            };

         await ps.Publish( toPublish, PublicationType.Both );
         await Task.Delay( 3000 );

         Assert.Equal( 2, received1 );
         Assert.Equal( 1, received2 );
         Assert.Equal( 4, received3 );
         Assert.Equal( 1, received4 );

         await unsubscribe3();
         await ps.Publish( toPublish, PublicationType.Both );
         await Task.Delay( 3000 );

         Assert.Equal( 4, received1 );
         Assert.Equal( 2, received2 );
         Assert.Equal( 4, received3 );
         Assert.Equal( 1, received4 );

         await unsubscribe2();
         await ps.Publish( toPublish, PublicationType.Both );
         await Task.Delay( 3000 );

         Assert.Equal( 6, received1 );
         Assert.Equal( 2, received2 );
         Assert.Equal( 4, received3 );
         Assert.Equal( 1, received4 );

         await unsubscribe1();
         await ps.Publish( toPublish, PublicationType.Both );
         await Task.Delay( 3000 );

         Assert.Equal( 6, received1 );
         Assert.Equal( 2, received2 );
         Assert.Equal( 4, received3 );
         Assert.Equal( 1, received4 );

         await ps.Publish( new[] { new BasicEntry { Id = "row6", Timestamp = now + TimeSpan.FromSeconds( 5 ), Value = 1337 } }, PublicationType.LatestPerCollection );
         await Task.Delay( 3000 );

         Assert.Equal( 6, received1 );
         Assert.Equal( 2, received2 );
         Assert.Equal( 4, received3 );
         Assert.Equal( 2, received4 );

         await unsubscribe4();
      }
   }
}
