using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IWorkProvider
   {
      DateTime GetNextActionTime( string id );
   }
}
