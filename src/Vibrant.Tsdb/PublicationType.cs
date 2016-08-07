using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   [Flags]
   public enum PublicationType
   {
      None = 0x00,
      LatestPerCollection = 0x01,
      AllFromCollections = 0x02,
      Both = LatestPerCollection | AllFromCollections,
   }
}
