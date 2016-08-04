using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Ats.Tests.Entries;

namespace Vibrant.Tsdb.Ats.Tests
{
   public class DefaultPublishSubscribeTests : AbstractPublishSubscribeTests<DefaultPublishSubscribe<BasicEntry>>
   {
      public override DefaultPublishSubscribe<BasicEntry> CreatePublishSubscribe()
      {
         return new DefaultPublishSubscribe<BasicEntry>( false );
      }
   }
}
