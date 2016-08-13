using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IPublishSubscribe<TKey, TEntry> : IPublish<TKey, TEntry>, ISubscribe<TKey, TEntry>
      where TEntry : IEntry<TKey>
   {
      Task WaitWhileDisconnectedAsync();
   }
}
