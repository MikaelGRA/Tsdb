using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public static class SubscribeExtensions
   {
      public static Task<Func<Task>> SubscribeAsync<TKey, TEntry>( this ISubscribe<TKey, TEntry> subscribe, TKey id, SubscriptionType subscribeType, Action<List<TEntry>> callback )
         where TEntry : IEntry
      {
         return subscribe.SubscribeAsync( new[] { id }, subscribeType, callback );
      }
   }
}
