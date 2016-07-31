using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats.Tests
{
   public class DefaultPublishSubscribeTests : AbstractPublishSubscribeTests<DefaultPublishSubscribe>
   {
      public override DefaultPublishSubscribe CreatePublishSubscribe()
      {
         return new DefaultPublishSubscribe( false );
      }
   }
}
