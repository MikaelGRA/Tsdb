using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IPublish<TEntry>
      where TEntry : IEntry
   {
      Task Publish( IEnumerable<TEntry> entries, PublicationType publish );
   }
}
