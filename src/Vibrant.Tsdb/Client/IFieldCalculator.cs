using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client
{
   internal interface IFieldCalculator
   {
      IFieldInfo Field { get; }

      void Aggregate( ref dynamic current, dynamic value, int count );

      void Complete( ref dynamic current, int finalCount );

      dynamic CreateInitialValue();
   }
}
