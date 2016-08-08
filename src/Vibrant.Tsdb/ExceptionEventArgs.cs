using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class ExceptionEventArgs : EventArgs
   {
      private Exception _exception;

      public ExceptionEventArgs( Exception exception )
      {
         _exception = exception;
      }

      public Exception Exception
      {
         get
         {
            return _exception;
         }
      }
   }
}
