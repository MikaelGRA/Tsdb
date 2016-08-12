using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Tests.Entries;

namespace Vibrant.Tsdb.Tests
{
   public class DefaultPublishSubscribeTests : AbstractPublishSubscribeTests<DefaultPublishSubscribe<string, BasicEntry>>
   {
      public override DefaultPublishSubscribe<string, BasicEntry> CreatePublishSubscribe()
      {
         return new DefaultPublishSubscribe<string, BasicEntry>( false );
      }
   }
}
