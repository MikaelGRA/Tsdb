using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client
{
   public class AggregationParameters
   {
      public static readonly IReadOnlyDictionary<string, string> NoTagRequirements;
      public static readonly string[] NoGroupings;

      static AggregationParameters()
      {
         NoTagRequirements = new Dictionary<string, string>();
         NoGroupings = new string[ 0 ];
      }

      public static AggregatedField[] Fields( params AggregatedField[] aggregatedFields )
      {
         return aggregatedFields;
      }
   }
}
