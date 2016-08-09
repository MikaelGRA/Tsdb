using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IPublish<TKey,TEntry>
      where TEntry : IEntry<TKey>
   {
      Task Publish( IEnumerable<TEntry> entries, PublicationType publish );
   }
}
