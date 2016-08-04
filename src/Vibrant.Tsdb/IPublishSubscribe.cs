using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IPublishSubscribe<TEntry> : IPublish<TEntry>, ISubscribe<TEntry>
      where TEntry : IEntry
   {
      Task WaitWhileDisconnected();
   }
}
