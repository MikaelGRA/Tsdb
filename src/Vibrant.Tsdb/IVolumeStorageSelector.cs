using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IVolumeStorageSelector
   {
      IVolumeStorage GetStorage( string id );
   }
}
