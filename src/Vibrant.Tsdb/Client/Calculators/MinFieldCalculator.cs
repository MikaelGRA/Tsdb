using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client.Calculators
{
   class MinFieldCalculator : IFieldCalculator
   {
      public MinFieldCalculator( IFieldInfo field )
      {
         Field = field;
      }

      public IFieldInfo Field { get; private set; }

      public void Aggregate( ref dynamic current, dynamic value, int count )
      {
         if( current == null || value < current )
         {
            current = value;
         }
      }

      public void Complete( ref dynamic current, int finalCount )
      {
         // nothing
      }

      public dynamic CreateInitialValue()
      {
         return null;
      }
   }
}
