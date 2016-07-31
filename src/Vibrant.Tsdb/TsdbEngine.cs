using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TsdbEngine
   {
      private TsdbClient _client;
      private IWorkProvider _workProvider;

      public TsdbEngine( IWorkProvider workProvider, TsdbClient client )
      {
         _client = client;
         _workProvider = workProvider;

         // TODO: Should probably depend on subscription API
      }
   }
}
