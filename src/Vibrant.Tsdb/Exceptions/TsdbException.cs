using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Exceptions
{

   public class TsdbException : Exception
   {
      public TsdbException() { }
      public TsdbException( string message ) : base( message ) { }
      public TsdbException( string message, Exception inner ) : base( message, inner ) { }
   }
}
