using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.CosmosTables
{
   public interface IPartitionProvider<TKey>
   {
      string GetPartitioning( TKey id, DateTime timestamp );

      string GetMaxPartitioning( TKey id );

      string GetMinPartitioning( TKey id );
   }
}
