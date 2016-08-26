using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Redis
{
   internal class RedisConnectionState
   {
      public const int Closed = 0;
      public const int Connected = 1;
      public const int Disposing = 2;
      public const int Disposed = 3;
   }
}
