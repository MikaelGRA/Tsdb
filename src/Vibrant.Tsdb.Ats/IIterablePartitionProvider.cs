using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   public interface IIterablePartitionProvider<TKey> : IPartitionProvider<TKey>
   {
      IEnumerable<string> IteratePartitions( TKey key, DateTime from, DateTime to );
   }
}
