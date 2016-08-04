using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface ISubscribe<TEntry>
      where TEntry : IEntry
   {
      Task<Func<Task>> Subscribe( IEnumerable<string> ids, Action<List<TEntry>> callback );

      Task<Func<Task>> SubscribeToAll( Action<List<TEntry>> callback );
   }
}
