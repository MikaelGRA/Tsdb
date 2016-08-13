using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface ISubscribe<TKey, TEntry>
      where TEntry : IEntry<TKey>
   {
      Task<Func<Task>> SubscribeAsync( IEnumerable<TKey> ids, SubscriptionType subscribe, Action<List<TEntry>> callback );

      Task<Func<Task>> SubscribeToAllAsync( SubscriptionType subscribe, Action<List<TEntry>> callback );
   }
}
