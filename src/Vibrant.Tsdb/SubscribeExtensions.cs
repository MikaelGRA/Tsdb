using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public static class SubscribeExtensions
   {
      public static Task<Func<Task>> Subscribe( this ISubscribe subscribe, string id, Action<List<IEntry>> callback )
      {
         return subscribe.Subscribe( new[] { id }, callback );
      }
   }
}
