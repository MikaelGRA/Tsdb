using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vibrant.Tsdb.Helpers;

namespace Vibrant.Tsdb.Client.Calculators
{
   class AverageFieldCalculator : IFieldCalculator
   {
      public AverageFieldCalculator( IFieldInfo field )
      {
         Field = field;
      }

      public IFieldInfo Field { get; private set; }

      public void Aggregate( ref dynamic current, dynamic value, int count )
      {
         current += ( value * count );
      }

      public void Complete( ref dynamic current, int finalCount )
      {
         current /= finalCount;
      }

      public dynamic CreateInitialValue()
      {
         return TypeHelper.GetDefaultValue( Field.ValueType );
      }
   }
}
