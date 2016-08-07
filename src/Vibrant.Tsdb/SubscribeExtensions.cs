using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public static class SubscribeExtensions
   {
      public static Task<Func<Task>> Subscribe<TSubscribeEntry>( this ISubscribe<TSubscribeEntry> subscribe, string id, SubscriptionType subscribeType, Action<List<TSubscribeEntry>> callback )
         where TSubscribeEntry : IEntry
      {
         return subscribe.Subscribe( new[] { id }, subscribeType, callback );
      }
   }
}
