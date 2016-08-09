using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface ISubscribe<TKey, TEntry>
      where TEntry : IEntry<TKey>
   {
      Task<Func<Task>> Subscribe( IEnumerable<TKey> ids, SubscriptionType subscribe, Action<List<TEntry>> callback );

      Task<Func<Task>> SubscribeToAll( SubscriptionType subscribe, Action<List<TEntry>> callback );
   }
}
