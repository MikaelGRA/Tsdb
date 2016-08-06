using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   public interface IPartitionProvider
   {
      string GetPartitioning( string id, DateTime timestamp );

      string GetMaxPartitioning( string id );

      string GetMinPartitioning( string id );
   }
}
