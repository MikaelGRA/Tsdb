using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Exceptions
{
   public class MissingTsdbServiceException : TsdbException
   {
      public MissingTsdbServiceException() { }
      public MissingTsdbServiceException( string message ) : base( message ) { }
      public MissingTsdbServiceException( string message, Exception inner ) : base( message, inner ) { }
   }
}
