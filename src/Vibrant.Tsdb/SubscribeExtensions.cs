using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public static class SubscribeExtensions
   {
      public static Task<Func<Task>> Subscribe<TKey, TEntry>( this ISubscribe<TKey, TEntry> subscribe, TKey id, SubscriptionType subscribeType, Action<List<TEntry>> callback )
         where TEntry : IEntry<TKey>
      {
         return subscribe.Subscribe( new[] { id }, subscribeType, callback );
      }
   }
}
