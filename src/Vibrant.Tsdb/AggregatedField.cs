using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class AggregatedField
   {
      public AggregatedField()
      {

      }

      public AggregatedField( string field, AggregationFunction function )
      {
         Field = field;
         Function = function;
      }

      public string Field { get; set; }

      public AggregationFunction Function { get; set; }
   }
}
