using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Ats.Tests.Entries;
using Xunit;

namespace Vibrant.Tsdb.Ats.Tests
{
   public abstract class AbstractPublishSubscribeTests<TPublishSubscribe>
     where TPublishSubscribe : IPublishSubscribe
   {
      public abstract TPublishSubscribe CreatePublishSubscribe();

      [Fact]
      public async Task Should_Subscribe_And_Publish()
      {
         var ps = CreatePublishSubscribe();

         await ps.WaitWhileDisconnected();

         int received1 = 0;
         var unsubscribe1 = await ps.Subscribe( new[] { "row1" }, entries =>
         {
            foreach( var entry in entries )
            {
               Assert.Equal( "row1", entry.GetId() );
               received1++;
            }
         } );

         int received2 = 0;
         var unsubscribe2 = await ps.Subscribe( new[] { "row2" }, entries =>
         {
            foreach( var entry in entries )
            {
               Assert.Equal( "row2", entry.GetId() );
               received2++;
            }
         } );

         int received3 = 0;
         var unsubscribe3 = await ps.SubscribeToAll( entries =>
         {
            foreach( var entry in entries )
            {
               received3++;
            }
         } );

         var toPublish = new[] {
               new BasicEntry { Id = "row1", Timestamp = DateTime.UtcNow, Value = 23.53 },
               new BasicEntry { Id = "row1", Timestamp = DateTime.UtcNow, Value = 50.23 },
               new BasicEntry { Id = "row2", Timestamp = DateTime.UtcNow, Value = 23 },
               new BasicEntry { Id = "row6", Timestamp = DateTime.UtcNow, Value = 2364 },
            };

         await ps.Publish( toPublish );

         await Task.Delay( 3000 );

         Assert.Equal( 2, received1 );
         Assert.Equal( 1, received2 );
         Assert.Equal( 4, received3 );
      }
   }
}
